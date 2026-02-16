import logging
import re
import os
import json
import time
import threading
import requests
import hashlib
from datetime import timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from django.utils import timezone
from django.conf import settings
from django.db.models.signals import post_save
from django.dispatch import receiver
from apps.epg.models import ProgramData, EPGSource
from apps.channels.models import Channel

try:
    from .helpers import render_template, render_title_template, title_similarity
except Exception:
    from helpers import render_template, render_title_template, title_similarity

LOGGER = logging.getLogger("plugins.epg_enhancer")
_BACKGROUND_THREAD_LOCK = threading.Lock()
_BACKGROUND_THREAD = None
RUN_LOCK_STALE_SECONDS = 6 * 3600
DEFAULT_AUTO_ENHANCE_DEBOUNCE_SECONDS = 180

DEFAULT_TITLE_TEMPLATE = "{title} ({year})"
DEFAULT_DESCRIPTION_TEMPLATE = "{title} ({year}) - {genres}\\nCast: {cast}\\nScores: {scores}\\n{overview}"


class ApiCallLimitReached(Exception):
    pass


class Plugin:
    name = "EPG Enhancer"
    version = "0.1.0"
    description = "Enhance EPG entries with metadata from TMDB or OMDb/IMDB."

    fields = [
        {
            "id": "provider",
            "label": "Metadata Provider",
            "type": "select",
            "default": "tmdb",
            "options": [
                {"value": "tmdb", "label": "TMDB"},
                {"value": "omdb", "label": "OMDb / IMDB"},
                {"value": "both", "label": "TMDB + OMDb (fallback)"},
            ],
            "help_text": "TMDB requires an API key. OMDb also requires an API key for reliable results.",
        },
        {
            "id": "provider_priority",
            "label": "Provider Priority (when using both)",
            "type": "select",
            "default": "tmdb_first",
            "options": [
                {"value": "tmdb_first", "label": "TMDB first"},
                {"value": "omdb_first", "label": "OMDb first"},
            ],
            "help_text": "Choose which provider to try first when using TMDB + OMDb.",
        },
        {
            "id": "content_type_filter",
            "label": "Program Type Filter",
            "type": "select",
            "default": "movies",
            "options": [
                {"value": "movies", "label": "Movies only"},
                {"value": "series", "label": "Series only"},
                {"value": "both", "label": "Movies + Series"},
            ],
            "help_text": "Choose whether to enhance movies, series, or both.",
        },
        {
            "id": "retry_count",
            "label": "API Retry Count",
            "type": "number",
            "default": 2,
            "help_text": "Retry failed API calls this many times before giving up.",
        },
        {
            "id": "retry_backoff_seconds",
            "label": "Retry Backoff (seconds)",
            "type": "number",
            "default": 1,
            "help_text": "Seconds to wait between retry attempts.",
        },
        {
            "id": "min_title_similarity",
            "label": "Min Title Similarity (TMDB)",
            "type": "number",
            "default": 0.72,
            "help_text": "Minimum title similarity to accept a TMDB match (0 = disabled).",
        },
        {
            "id": "tmdb_api_key",
            "label": "TMDB API Key",
            "type": "string",
            "default": "",
            "help_text": "Required when provider is TMDB. Create one at themoviedb.org.",
        },
        {
            "id": "omdb_api_key",
            "label": "OMDb API Key",
            "type": "string",
            "default": "",
            "help_text": "Optional fallback for IMDB/RottenTomatoes/Metacritic scores. Create at omdbapi.com.",
        },
        {
            "id": "channel_group_name",
            "label": "Channel Group Name Filter",
            "type": "string",
            "default": "movies",
            "help_text": "Only process channels in this group name (case-insensitive). Leave blank to include all.",
        },
        {
            "id": "channel_name_regex",
            "label": "Channel Name Regex",
            "type": "string",
            "default": "",
            "help_text": "Optional regex applied to channel name (e.g. (?i)movie). Leave blank to include all.",
        },
        {
            "id": "lookahead_hours",
            "label": "Lookahead Hours",
            "type": "number",
            "default": 12,
            "help_text": "How far into the future to enhance programs.",
        },
        {
            "id": "lookback_hours",
            "label": "Lookback Hours",
            "type": "number",
            "default": 12,
            "help_text": "How far into the past to include programs (helps catch recent updates).",
        },
        {
            "id": "max_programs",
            "label": "Max Programs per Run",
            "type": "number",
            "default": 2000,
            "help_text": "Safety limit for each run to avoid heavy loads.",
        },
        {
            "id": "cache_enabled",
            "label": "Enable Metadata Cache",
            "type": "boolean",
            "default": True,
            "help_text": "Reuse metadata across refreshes to reduce API calls.",
        },
        {
            "id": "cache_ttl_hours",
            "label": "Cache TTL (hours)",
            "type": "number",
            "default": 48,
            "help_text": "Expire cached metadata after this many hours (0 = never).",
        },
        {
            "id": "cache_max_entries",
            "label": "Cache Max Entries",
            "type": "number",
            "default": 5000,
            "help_text": "Maximum cached items to keep (0 = unlimited).",
        },
        {
            "id": "exports_dir",
            "label": "Exports Directory",
            "type": "string",
            "default": "",
            "help_text": "Optional absolute path for report exports. Leave blank to use the app data exports folder.",
        },
        {
            "id": "tmdb_api_call_limit",
            "label": "TMDB API Call Limit",
            "type": "number",
            "default": 0,
            "help_text": "Maximum TMDB API calls per run (0 = unlimited).",
        },
        {
            "id": "omdb_api_call_limit",
            "label": "OMDb API Call Limit",
            "type": "number",
            "default": 1000,
            "help_text": "Maximum OMDb API calls per run (0 = unlimited).",
        },
        {
            "id": "replace_title",
            "label": "Replace Program Title",
            "type": "boolean",
            "default": False,
            "help_text": "When enabled, replace the program title with the metadata title.",
        },
        {
            "id": "title_template",
            "label": "Title Template",
            "type": "string",
            "default": DEFAULT_TITLE_TEMPLATE,
            "help_text": "Template used when replacing titles. Tokens: {title}, {year}, {genre}",
        },
        {
            "id": "description_mode",
            "label": "Description Update Mode",
            "type": "select",
            "default": "append",
            "options": [
                {"value": "append", "label": "Append metadata block"},
                {"value": "replace", "label": "Replace description"},
            ],
            "help_text": "Choose whether to append metadata or replace the description entirely.",
        },
        {
            "id": "description_template",
            "label": "Description Template",
            "type": "string",
            "default": DEFAULT_DESCRIPTION_TEMPLATE,
            "help_text": (
                "Template for the metadata block. Tokens: {title} (movie title), "
                "{year} (release year), {genre} (first genre), {genres} (all genres), "
                "{runtime} (runtime), {director} (director), {writers} (writers), {cast} (top cast list), {scores} (ratings summary), "
                "{overview} (plot summary), {series_title} (series name), {episode_title} (episode title), {season_episode} (SxxEyy)."
            ),
        },
        {
            "id": "auto_enhance",
            "label": "Auto-Enhance on EPG Updates",
            "type": "boolean",
            "default": True,
            "help_text": "Automatically enhance programs when EPG data is updated.",
        },
        {
            "id": "auto_enhance_debounce_seconds",
            "label": "Auto-Enhance Debounce (seconds)",
            "type": "number",
            "default": DEFAULT_AUTO_ENHANCE_DEBOUNCE_SECONDS,
            "help_text": "Minimum seconds between auto-enhance triggers per source.",
        },
        {
            "id": "dry_run",
            "label": "Dry Run",
            "type": "boolean",
            "default": False,
            "help_text": "When enabled, enhancement runs as preview (no database writes).",
        },
        {
            "id": "force_reapply",
            "label": "Force Reapply",
            "type": "boolean",
            "default": False,
            "help_text": "Reapply templates even when a program was already enhanced by this plugin.",
        },
    ]

    actions = [
        {
            "id": "enhance",
            "label": "Enhance Programs",
            "description": "Fetch metadata and update program descriptions.",
            "confirm": {
                "required": True,
                "title": "Enhance movie EPG entries?",
                "message": "This will fetch metadata and update program descriptions.",
            },
        },
        {
            "id": "check_progress",
            "label": "Check Progress",
            "description": "Show current run progress and remaining programs.",
        },
        {
            "id": "last_result",
            "label": "View Last Run Result",
            "description": "Show summary and samples from the last preview/enhance run.",
        },
        {
            "id": "clear_exports",
            "label": "Clear Exports",
            "description": "Delete EPG Enhancer export reports and reset progress/last-result state.",
        },
        {
            "id": "clear_cache",
            "label": "Clear Cache",
            "description": "Clear only metadata cache files (keep progress/last-result state).",
        },
    ]

    def run(self, action: str, params: dict, context: dict):
        logger = context.get("logger") or LOGGER
        settings = context.get("settings", {})
        params = params or {}

        if action not in {"enhance", "check_progress", "last_result", "clear_exports", "clear_cache"}:
            return {"status": "error", "message": f"Unknown action {action}"}

        if action == "check_progress":
            return self._load_progress(logger=logger)

        if action == "last_result":
            return self._load_last_run_result(logger=logger)

        if action == "clear_exports":
            return self._clear_exports(logger=logger)

        if action == "clear_cache":
            return self._clear_cache(logger=logger)

        # Enhance path is asynchronous from UI; workers pass _background=True.
        if action == "enhance" and not params.get("_background"):
            return self._start_background_action(action_id=action, settings=settings, logger=logger)

        provider = settings.get("provider", "tmdb")
        provider_priority = settings.get("provider_priority", "tmdb_first")
        content_type_filter = (settings.get("content_type_filter", "movies") or "movies").lower()
        tmdb_api_key = settings.get("tmdb_api_key", "").strip()
        omdb_api_key = settings.get("omdb_api_key", "").strip()
        channel_group_name = settings.get("channel_group_name", "").strip()
        channel_name_regex = settings.get("channel_name_regex", "").strip()
        lookahead_hours = int(settings.get("lookahead_hours", 12) or 12)
        lookback_hours = int(settings.get("lookback_hours", 12) or 0)
        max_programs = int(settings.get("max_programs", 2000) or 2000)
        dry_run = bool(settings.get("dry_run", False))
        force_reapply = bool(settings.get("force_reapply", False))
        retry_count = int(settings.get("retry_count", 2) or 0)
        retry_backoff_seconds = float(settings.get("retry_backoff_seconds", 1) or 0)
        tmdb_api_call_limit = int(settings.get("tmdb_api_call_limit", 0) or 0)
        omdb_api_call_limit = int(settings.get("omdb_api_call_limit", 1000) or 0)
        min_title_similarity = float(settings.get("min_title_similarity", 0.72) or 0)
        cache_enabled = bool(settings.get("cache_enabled", True))
        cache_ttl_hours = float(settings.get("cache_ttl_hours", 48) or 0)
        cache_max_entries = int(settings.get("cache_max_entries", 5000) or 0)
        exports_dir_override = (settings.get("exports_dir", "") or "").strip()
        replace_title = bool(settings.get("replace_title", False))
        description_mode = (settings.get("description_mode", "append") or "append").lower()
        title_template = settings.get("title_template", DEFAULT_TITLE_TEMPLATE) or DEFAULT_TITLE_TEMPLATE
        description_template = settings.get(
            "description_template",
            DEFAULT_DESCRIPTION_TEMPLATE,
        ) or DEFAULT_DESCRIPTION_TEMPLATE
        # Normalize literal escapes entered in UI to real line breaks.
        description_template = description_template.replace("\\r\\n", "\n").replace("\\n", "\n")

        logger.info(
            "EPG Enhancer run started: action=%s provider=%s priority=%s dry_run=%s lookback_h=%s lookahead_h=%s max_programs=%s cache=%s",
            action,
            provider,
            provider_priority,
            dry_run,
            lookback_hours,
            lookahead_hours,
            max_programs,
            cache_enabled,
        )

        if provider == "tmdb" and not tmdb_api_key:
            return {"status": "error", "message": "TMDB API key is required when provider is TMDB."}
        if provider == "omdb" and not omdb_api_key:
            return {"status": "error", "message": "OMDb API key is required when provider is OMDb."}
        if provider == "both" and not (tmdb_api_key or omdb_api_key):
            return {"status": "error", "message": "At least one API key is required when provider is TMDB + OMDb."}

        if channel_name_regex:
            try:
                channel_name_pattern = re.compile(channel_name_regex)
            except re.error as exc:
                return {"status": "error", "message": f"Invalid channel_name_regex: {exc}"}
        else:
            channel_name_pattern = None

        window_start = timezone.now() - timedelta(hours=lookback_hours)
        window_end = timezone.now() + timedelta(hours=lookahead_hours)

        programs = self._find_programs(
            window_start,
            window_end,
            channel_group_name,
            channel_name_pattern,
            max_programs,
        )

        if not programs:
            logger.info("EPG Enhancer run found no programs matching filters.")
            empty_progress = {
                "status": "completed",
                "running": False,
                "action": action,
                "dry_run": dry_run,
                "total_programs": 0,
                "attempted": 0,
                "updated": 0,
                "preview_matches": 0,
                "matched": 0,
                "skipped": 0,
                "remaining": 0,
                "api_calls": {"tmdb": 0, "omdb": 0},
                "started_at": timezone.now().isoformat(),
                "finished_at": timezone.now().isoformat(),
            }
            self._save_progress(empty_progress, logger=logger)
            empty_summary = {
                "status": "ok",
                "attempted": 0,
                "updated": 0,
                "preview_matches": 0,
                "matched": 0,
                "skipped": 0,
                "dry_run": dry_run,
                "api_calls": {"tmdb": 0, "omdb": 0},
                "details": {"updated": [], "preview": [], "skipped": []},
            }
            report_file = self._save_full_report(
                action=action,
                summary=empty_summary,
                updated=[],
                preview=[],
                skipped=[],
                exports_dir=exports_dir_override,
                logger=logger,
            )
            self._save_last_run_result(
                empty_summary,
                logger=logger,
                report_file=report_file,
                exports_dir=exports_dir_override,
            )
            return {"status": "ok", "message": "No programs matched filters."}

        logger.info("EPG Enhancer run matched %s program(s) for processing.", len(programs))

        updated = []
        preview = []
        skipped = []
        attempted = 0
        total_programs = len(programs)
        dry_mode = dry_run

        call_counter = {
            "tmdb": {"count": 0, "limit": tmdb_api_call_limit},
            "omdb": {"count": 0, "limit": omdb_api_call_limit},
        }
        cache_context = self._load_cache_context(
            enabled=cache_enabled,
            ttl_hours=cache_ttl_hours,
            max_entries=cache_max_entries,
            logger=logger,
        )

        progress = {
            "status": "running",
            "running": True,
            "action": action,
            "dry_run": dry_mode,
            "total_programs": total_programs,
            "attempted": 0,
            "updated": 0,
            "preview_matches": 0,
            "matched": 0,
            "skipped": 0,
            "remaining": total_programs,
            "api_calls": {"tmdb": 0, "omdb": 0},
            "started_at": timezone.now().isoformat(),
            "finished_at": None,
        }
        self._save_progress(progress, logger=logger)

        try:
            for program in programs:
                attempted += 1
                result = self._process_program(
                    program=program,
                    provider=provider,
                    tmdb_api_key=tmdb_api_key,
                    omdb_api_key=omdb_api_key,
                    dry_run=dry_mode,
                    force_reapply=force_reapply,
                    replace_title=replace_title,
                    description_mode=description_mode,
                    title_template=title_template,
                    description_template=description_template,
                    provider_priority=provider_priority,
                    content_type_filter=content_type_filter,
                    retry_count=retry_count,
                    retry_backoff_seconds=retry_backoff_seconds,
                    call_counter=call_counter,
                    cache_context=cache_context,
                    min_title_similarity=min_title_similarity,
                    logger=logger,
                )
                if result.get("stop"):
                    skipped.append(result)
                else:
                    status_value = result.get("status")
                    if status_value == "updated":
                        updated.append(result)
                    elif status_value == "preview":
                        preview.append(result)
                    else:
                        skipped.append(result)

                progress.update(
                    {
                        "attempted": attempted,
                        "updated": len(updated),
                        "preview_matches": len(preview),
                        "matched": len(updated) + len(preview),
                        "skipped": len(skipped),
                        "remaining": max(total_programs - attempted, 0),
                        "api_calls": {
                            "tmdb": call_counter["tmdb"].get("count", 0),
                            "omdb": call_counter["omdb"].get("count", 0),
                        },
                    }
                )
                self._save_progress(progress, logger=logger)

                if result.get("stop"):
                    break

            if cache_context.get("dirty"):
                self._save_cache_context(cache_context, logger=logger)

            summary = {
                "status": "ok",
                "attempted": attempted,
                "updated": len(updated),
                "preview_matches": len(preview),
                "matched": len(updated) + len(preview),
                "skipped": len(skipped),
                "dry_run": dry_mode,
                "api_calls": {
                    "tmdb": call_counter["tmdb"].get("count", 0),
                    "omdb": call_counter["omdb"].get("count", 0),
                },
                "details": {
                    "updated": updated[:10],
                    "preview": preview[:10],
                    "skipped": skipped[:10],
                },
            }

            progress.update(
                {
                    "status": "completed",
                    "running": False,
                    "finished_at": timezone.now().isoformat(),
                    "remaining": max(total_programs - attempted, 0),
                }
            )
            self._save_progress(progress, logger=logger)
            report_file = self._save_full_report(
                action=action,
                summary=summary,
                updated=updated,
                preview=preview,
                skipped=skipped,
                exports_dir=exports_dir_override,
                logger=logger,
            )
            self._save_last_run_result(
                summary,
                logger=logger,
                report_file=report_file,
                exports_dir=exports_dir_override,
            )

            if logger:
                logger.info(
                    "EPG Enhancer finished: attempted=%s matched=%s updated=%s preview=%s skipped=%s dry_run=%s",
                    attempted,
                    summary["matched"],
                    len(updated),
                    len(preview),
                    len(skipped),
                    dry_mode,
                )

            return summary
        except Exception as exc:
            progress.update(
                {
                    "status": "failed",
                    "running": False,
                    "error": str(exc),
                    "finished_at": timezone.now().isoformat(),
                    "remaining": max(total_programs - attempted, 0),
                    "api_calls": {
                        "tmdb": call_counter["tmdb"].get("count", 0),
                        "omdb": call_counter["omdb"].get("count", 0),
                    },
                }
            )
            self._save_progress(progress, logger=logger)
            raise

    def _find_programs(
        self,
        window_start,
        window_end,
        channel_group_name,
        channel_name_pattern,
        max_programs,
    ):
        qs = (
            ProgramData.objects.filter(
                start_time__lte=window_end,
                end_time__gte=window_start,
            )
            .select_related("epg")
            .prefetch_related("epg__channels", "epg__channels__channel_group")
        )

        if channel_group_name:
            qs = qs.filter(
                epg__channels__channel_group__name__icontains=channel_group_name
            )

        programs = []
        for program in qs.order_by("start_time").distinct()[: max_programs * 2]:
            channels = list(program.epg.channels.all())
            if channel_name_pattern:
                channels = [c for c in channels if channel_name_pattern.search(c.name or "")]
            if not channels:
                continue
            if len(programs) >= max_programs:
                break
            programs.append({"program": program, "channels": channels})

        return programs

    def _get_program_content_hash(self, program_obj):
        """Generate a hash of the program's content to detect changes.

        Note: We exclude start/end times from the hash because the same content
        airing at different times should reuse the same enhancement to avoid
        redundant API calls.
        """
        return self._get_content_hash(
            program_obj.title,
            program_obj.sub_title,
            program_obj.description,
        )

    def _get_content_hash(self, title, sub_title, description):
        content = f"{title}|{sub_title or ''}|{description or ''}"
        return hashlib.md5(content.encode('utf-8')).hexdigest()

    def _process_program(
        self,
        program,
        provider,
        tmdb_api_key,
        omdb_api_key,
        dry_run,
        force_reapply,
        replace_title,
        description_mode,
        title_template,
        description_template,
        provider_priority,
        content_type_filter,
        retry_count,
        retry_backoff_seconds,
        call_counter,
        cache_context,
        min_title_similarity,
        logger,
    ):
        program_obj = program["program"]
        channels = program["channels"]
        title, year, series_hints = self._extract_title_and_year(program_obj.title, program_obj.sub_title, program_obj.description)

        metadata = None
        error = None
        used_provider = None
        cache_key = self._get_program_content_hash(program_obj)
        if cache_context.get("enabled"):
            cached = self._cache_get(cache_context, cache_key)
            if cached:
                metadata = cached.get("metadata")
                used_provider = cached.get("provider")
                logger.info(
                    "EPG Enhancer cache hit: program=%s provider=%s",
                    program_obj.title,
                    used_provider or "unknown",
                )
            else:
                logger.info("EPG Enhancer cache miss: program=%s", program_obj.title)

        provider_order = []
        if provider == "both":
            if provider_priority == "omdb_first":
                provider_order = ["omdb", "tmdb"]
            else:
                provider_order = ["tmdb", "omdb"]
        else:
            provider_order = [provider]

        for chosen_provider in provider_order:
            if metadata:
                break
            if chosen_provider == "tmdb" and not tmdb_api_key:
                continue
            if chosen_provider == "omdb" and not omdb_api_key:
                continue

            logger.info("EPG Enhancer lookup attempt: program=%s provider=%s year=%s", program_obj.title, chosen_provider, year)

            try:
                if chosen_provider == "tmdb":
                    metadata = self._call_with_retry(
                        lambda: self._lookup_tmdb(
                            title,
                            year,
                            tmdb_api_key,
                            call_counter["tmdb"],
                            min_title_similarity,
                            content_type_filter,
                            series_hints,
                        ),
                        retries=retry_count,
                        backoff_seconds=retry_backoff_seconds,
                    )
                else:
                    metadata = self._call_with_retry(
                        lambda: self._lookup_omdb(title, year, omdb_api_key, call_counter["omdb"], content_type_filter, series_hints),
                        retries=retry_count,
                        backoff_seconds=retry_backoff_seconds,
                    )
            except ApiCallLimitReached:
                logger.warning("EPG Enhancer API call limit reached: provider=%s program=%s", chosen_provider, program_obj.title)
                return {
                    "status": "skipped",
                    "program": program_obj.title,
                    "channel": channels[0].name if channels else "",
                    "reason": "API call limit reached",
                    "stop": True,
                }
            except Exception as exc:
                error = str(exc)
                metadata = None
                logger.warning("EPG Enhancer lookup failed: provider=%s program=%s error=%s", chosen_provider, program_obj.title, exc)

            if metadata:
                used_provider = chosen_provider
                logger.info("EPG Enhancer metadata match: program=%s provider=%s", program_obj.title, used_provider)
                if provider == "both" and len(provider_order) > 1 and used_provider == provider_order[1]:
                    logger.info("EPG Enhancer fallback provider used: program=%s primary=%s fallback=%s", program_obj.title, provider_order[0], provider_order[1])
                if cache_context.get("enabled"):
                    self._cache_set(cache_context, cache_key, metadata, used_provider)
                break

        if not metadata:
            logger.info("EPG Enhancer no metadata match: program=%s reason=%s", program_obj.title, error or "No metadata found")
            return {
                "status": "skipped",
                "program": program_obj.title,
                "channel": channels[0].name if channels else "",
                "reason": error or "No metadata found",
            }

        enhanced_block = render_template(description_template, metadata)
        if not enhanced_block:
            return {
                "status": "skipped",
                "program": program_obj.title,
                "channel": channels[0].name if channels else "",
                "metadata": metadata,
                "reason": "Description template rendered empty",
            }

        if description_mode not in {"append", "replace"}:
            description_mode = "append"

        if description_mode == "replace":
            proposed_description = enhanced_block
        else:
            proposed_description = enhanced_block
            if program_obj.description:
                proposed_description = f"{program_obj.description.strip()}\n\n{enhanced_block}"

        proposed_title = None
        if replace_title:
            proposed_title = render_title_template(title_template, metadata) or None

        # Content-based caching: only skip if we've processed this exact program content before
        already_applied = False
        current_content_hash = self._get_program_content_hash(program_obj)

        custom_props = program_obj.custom_properties or {}
        plugin_state = custom_props.get("epg_enhancer", {})

        # Check if we've processed this exact program content before
        stored_content_hash = plugin_state.get("content_hash")
        if stored_content_hash == current_content_hash:
            already_applied = True

        if dry_run or (already_applied and not force_reapply):
            if already_applied and not force_reapply:
                logger.info("EPG Enhancer skipped already-processed content: program=%s", program_obj.title)
            result = {
                "status": "skipped" if already_applied else "preview",
                "program": program_obj.title,
                "channel": channels[0].name if channels else "",
                "metadata": metadata,
                "reason": "Already processed" if already_applied else "Preview only",
            }
            if dry_run and not already_applied:
                result.update(
                    {
                        "current_title": program_obj.title,
                        "current_description": program_obj.description or "",
                        "proposed_title": proposed_title,
                        "proposed_description": proposed_description,
                    }
                )
            return result

        new_description = proposed_description

        update_fields = ["description", "custom_properties"]

        if proposed_title:
            program_obj.title = proposed_title
            update_fields.append("title")

        stored_content_hash = self._get_content_hash(
            program_obj.title,
            program_obj.sub_title,
            new_description,
        )

        plugin_state = {
            "content_hash": stored_content_hash,
            "provider": used_provider or provider,
            "title": metadata.get("title"),
            "year": metadata.get("year"),
            "tmdb_id": metadata.get("tmdb_id"),
            "imdb_id": metadata.get("imdb_id"),
            "last_updated": timezone.now().isoformat(),
        }
        custom_props["epg_enhancer"] = plugin_state

        program_obj.description = new_description
        program_obj.custom_properties = custom_props
        program_obj.save(update_fields=update_fields)
        logger.info("EPG Enhancer updated program: program=%s provider=%s mode=%s title_replaced=%s", program_obj.title, used_provider or provider, description_mode, bool(proposed_title))

        return {
            "status": "updated",
            "program": program_obj.title,
            "channel": channels[0].name if channels else "",
            "metadata": metadata,
        }

    def _extract_title_and_year(self, title, sub_title, description=None):
        """Extract clean title, year, and optional episode hints from EPG fields."""
        raw_title = title or ""
        raw_sub_title = sub_title or ""
        raw_description = description or ""

        # Check for year in title first, then description.
        match = re.search(r"\((\d{4})\)", raw_title)
        year = int(match.group(1)) if match else None
        if not year and raw_description:
            match = re.search(r"\((\d{4})\)", raw_description)
            year = int(match.group(1)) if match else None

        # Detect common episode markers (SxxEyy or 'Season x Episode y').
        season = None
        episode = None
        for source in (raw_title, raw_sub_title, raw_description):
            if not source:
                continue
            ep_match = re.search(r"(?i)\bS(\d{1,2})\s*E(\d{1,2})\b", source)
            if not ep_match:
                ep_match = re.search(r"(?i)\bSeason\s*(\d{1,2})\s*Episode\s*(\d{1,2})\b", source)
            if ep_match:
                season = int(ep_match.group(1))
                episode = int(ep_match.group(2))
                break

        # Clean the title by removing year and episode markers.
        clean_title = re.sub(r"\s*\(\d{4}\)\s*", "", raw_title)
        clean_title = re.sub(r"(?i)\bS\d{1,2}\s*E\d{1,2}\b", "", clean_title)
        clean_title = re.sub(r"(?i)\bSeason\s*\d{1,2}\s*Episode\s*\d{1,2}\b", "", clean_title)
        clean_title = re.sub(r"\s{2,}", " ", clean_title).strip(" -:	")

        if not clean_title and raw_sub_title:
            clean_title = raw_sub_title.strip()

        episode_title = ""
        if raw_sub_title and raw_sub_title.strip().lower() != (clean_title or "").lower():
            episode_title = raw_sub_title.strip()

        series_hints = {
            "is_episode": bool(season and episode),
            "season": season,
            "episode": episode,
            "episode_title": episode_title,
        }

        return clean_title, year, series_hints

    def _lookup_tmdb(self, title, year, api_key, call_counter, min_title_similarity, content_type_filter, series_hints):
        filter_value = (content_type_filter or "movies").lower()
        is_episode = bool((series_hints or {}).get("is_episode"))

        if filter_value == "series":
            search_modes = ["tv"]
        elif filter_value == "both":
            search_modes = ["tv", "movie"] if is_episode else ["movie", "tv"]
        else:
            search_modes = ["movie"]

        for mode in search_modes:
            params = {
                "api_key": api_key,
                "query": title,
                "include_adult": False,
            }
            if year:
                if mode == "movie":
                    params["year"] = year
                else:
                    params["first_air_date_year"] = year

            self._consume_api_call(call_counter)
            search_resp = self._get_requests_session().get(
                f"https://api.themoviedb.org/3/search/{mode}", params=params, timeout=10
            )
            search_resp.raise_for_status()
            results = search_resp.json().get("results", [])
            if not results:
                continue

            results.sort(key=lambda result: result.get("popularity") or 0, reverse=True)
            selected = None
            if min_title_similarity > 0:
                for candidate in results:
                    candidate_title = candidate.get("title") or candidate.get("name") or ""
                    similarity = title_similarity(title, candidate_title)
                    if similarity >= min_title_similarity:
                        selected = candidate
                        break
                if not selected:
                    continue
            else:
                selected = results[0]

            tmdb_id = selected.get("id")
            self._consume_api_call(call_counter)
            detail_resp = self._get_requests_session().get(
                f"https://api.themoviedb.org/3/{mode}/{tmdb_id}",
                params={"api_key": api_key, "append_to_response": "credits,external_ids"},
                timeout=10,
            )
            detail_resp.raise_for_status()
            detail = detail_resp.json()

            credits = detail.get("credits", {})
            cast = [c.get("name") for c in credits.get("cast", [])[:6] if c.get("name")]

            crew = credits.get("crew", [])
            writer_jobs = {"Writer", "Screenplay", "Story", "Characters"}
            directors = []
            writers = []
            for member in crew:
                name = member.get("name")
                job = member.get("job")
                if not name or not job:
                    continue
                if job == "Director" and name not in directors:
                    directors.append(name)
                if job in writer_jobs and name not in writers:
                    writers.append(name)

            ratings = {
                "tmdb": detail.get("vote_average"),
                "tmdb_count": detail.get("vote_count"),
            }
            imdb_id = detail.get("external_ids", {}).get("imdb_id")
            if imdb_id:
                ratings["imdb_id"] = imdb_id

            metadata = {
                "provider": "tmdb",
                "content_type": "series" if mode == "tv" else "movie",
                "tmdb_id": tmdb_id,
                "imdb_id": imdb_id,
                "title": (detail.get("name") if mode == "tv" else detail.get("title")) or title,
                "series_title": (detail.get("name") if mode == "tv" else "") or "",
                "episode_title": (series_hints or {}).get("episode_title") or "",
                "season": (series_hints or {}).get("season"),
                "episode": (series_hints or {}).get("episode"),
                "year": ((detail.get("first_air_date") if mode == "tv" else detail.get("release_date")) or "")[:4],
                "genres": [g.get("name") for g in detail.get("genres", []) if g.get("name")],
                "overview": detail.get("overview"),
                "cast": cast,
                "runtime": detail.get("runtime"),
                "director": ", ".join(directors[:3]),
                "writers": ", ".join(writers[:6]),
                "ratings": ratings,
            }

            if mode == "tv" and (series_hints or {}).get("season") and (series_hints or {}).get("episode"):
                season = (series_hints or {}).get("season")
                episode = (series_hints or {}).get("episode")
                try:
                    self._consume_api_call(call_counter)
                    ep_resp = self._get_requests_session().get(
                        f"https://api.themoviedb.org/3/tv/{tmdb_id}/season/{season}/episode/{episode}",
                        params={"api_key": api_key},
                        timeout=10,
                    )
                    ep_resp.raise_for_status()
                    ep_detail = ep_resp.json()
                    metadata["episode_title"] = ep_detail.get("name") or metadata.get("episode_title") or ""
                    metadata["overview"] = ep_detail.get("overview") or metadata.get("overview")
                    if ep_detail.get("runtime"):
                        metadata["runtime"] = ep_detail.get("runtime")
                except Exception:
                    # Episode lookup is optional; keep series-level metadata on failure.
                    pass

            return metadata

        return None

    def _lookup_omdb(self, title, year, api_key, call_counter, content_type_filter, series_hints):
        filter_value = (content_type_filter or "movies").lower()
        is_episode = bool((series_hints or {}).get("is_episode"))

        if filter_value == "series":
            search_types = ["series"]
        elif filter_value == "both":
            search_types = ["series", "movie"] if is_episode else ["movie", "series"]
        else:
            search_types = ["movie"]

        for search_type in search_types:
            params = {"apikey": api_key, "t": title, "type": search_type}
            if year:
                params["y"] = year

            self._consume_api_call(call_counter)
            resp = self._get_requests_session().get("https://www.omdbapi.com/", params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if data.get("Response") != "True":
                continue

            ratings = {
                "imdb": data.get("imdbRating"),
                "rt": next((r.get("Value") for r in data.get("Ratings", []) if r.get("Source") == "Rotten Tomatoes"), None),
                "metacritic": data.get("Metascore"),
            }

            cast = [actor.strip() for actor in (data.get("Actors") or "").split(",") if actor.strip()][:6]
            genres = [g.strip() for g in (data.get("Genre") or "").split(",") if g.strip()]
            director_value = ", ".join([d.strip() for d in (data.get("Director") or "").split(",") if d.strip()][:3])
            writers_value = ", ".join([w.strip() for w in (data.get("Writer") or "").split(",") if w.strip()][:6])

            omdb_type = (data.get("Type") or search_type).lower()
            metadata = {
                "provider": "omdb",
                "content_type": "series" if omdb_type == "series" else "movie",
                "imdb_id": data.get("imdbID"),
                "title": data.get("Title") or title,
                "series_title": data.get("Title") if omdb_type == "series" else "",
                "episode_title": (series_hints or {}).get("episode_title") or "",
                "season": (series_hints or {}).get("season"),
                "episode": (series_hints or {}).get("episode"),
                "year": data.get("Year"),
                "genres": genres,
                "overview": data.get("Plot"),
                "cast": cast,
                "runtime": data.get("Runtime"),
                "director": director_value if director_value != "N/A" else "",
                "writers": writers_value if writers_value != "N/A" else "",
                "ratings": ratings,
            }

            if omdb_type == "series" and data.get("imdbID") and (series_hints or {}).get("season") and (series_hints or {}).get("episode"):
                season = (series_hints or {}).get("season")
                episode = (series_hints or {}).get("episode")
                try:
                    self._consume_api_call(call_counter)
                    ep_resp = self._get_requests_session().get(
                        "https://www.omdbapi.com/",
                        params={
                            "apikey": api_key,
                            "i": data.get("imdbID"),
                            "Season": season,
                            "Episode": episode,
                        },
                        timeout=10,
                    )
                    ep_resp.raise_for_status()
                    ep_data = ep_resp.json()
                    if ep_data.get("Response") == "True":
                        metadata["episode_title"] = ep_data.get("Title") or metadata.get("episode_title") or ""
                        if ep_data.get("Plot") and ep_data.get("Plot") != "N/A":
                            metadata["overview"] = ep_data.get("Plot")
                        if ep_data.get("Runtime") and ep_data.get("Runtime") != "N/A":
                            metadata["runtime"] = ep_data.get("Runtime")
                except Exception:
                    pass

            return metadata

        return None

    def _call_with_retry(self, func, retries, backoff_seconds):
        attempt = 0
        while True:
            try:
                return func()
            except ApiCallLimitReached:
                raise
            except Exception:
                if attempt >= retries:
                    raise
                attempt += 1
                if backoff_seconds:
                    time.sleep(backoff_seconds)

    def _get_requests_session(self):
        session = getattr(self, "_requests_session", None)
        if session:
            return session
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods={"GET", "POST"},
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        self._requests_session = session
        return session

    def _consume_api_call(self, call_counter):
        limit = call_counter.get("limit") or 0
        if limit > 0 and call_counter.get("count", 0) >= limit:
            raise ApiCallLimitReached()
        call_counter["count"] = call_counter.get("count", 0) + 1

    def _load_cache_context(self, enabled, ttl_hours, max_entries, logger=None):
        context = {
            "enabled": enabled,
            "ttl_seconds": max(ttl_hours, 0) * 3600,
            "max_entries": max_entries,
            "path": self._get_cache_path(),
            "entries": {},
            "dirty": False,
        }
        if not enabled:
            if logger:
                logger.info("EPG Enhancer cache disabled for this run.")
            return context

        try:
            with open(context["path"], "r", encoding="utf-8") as handle:
                payload = json.load(handle)
                context["entries"] = payload.get("entries", {})
        except FileNotFoundError:
            return context
        except Exception as exc:
            if logger:
                logger.warning("EPG Enhancer cache load failed: %s", exc)
            return context

        self._prune_cache(context)
        if logger:
            logger.info("EPG Enhancer cache loaded: entries=%s path=%s", len(context.get("entries", {})), context["path"])
        return context

    def _save_cache_context(self, context, logger=None):
        if not context.get("enabled"):
            return

        cache_dir = os.path.dirname(context["path"])
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)
        lock_path = context["path"] + ".lock"
        lock_fd = self._acquire_cache_lock(lock_path)
        if lock_fd is None:
            if logger:
                logger.warning("EPG Enhancer cache lock timeout. Skipping save.")
            return

        try:
            self._prune_cache(context)
            payload = {
                "version": 1,
                "updated_at": time.time(),
                "entries": context["entries"],
            }
            tmp_path = context["path"] + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, separators=(",", ":"))
            try:
                os.replace(tmp_path, context["path"])
            except Exception:
                # Fallback for platforms that may not support os.replace semantics
                if os.path.exists(context["path"]):
                    os.remove(context["path"])
                os.rename(tmp_path, context["path"])
            if logger:
                logger.info("EPG Enhancer cache saved: entries=%s path=%s", len(context.get("entries", {})), context["path"])
        finally:
            self._release_cache_lock(lock_path, lock_fd)

    def _prune_cache(self, context):
        entries = context.get("entries", {})
        ttl_seconds = context.get("ttl_seconds", 0)
        max_entries = context.get("max_entries", 0)
        now = time.time()

        if ttl_seconds > 0:
            expired = [
                key for key, entry in entries.items()
                if now - entry.get("ts", now) > ttl_seconds
            ]
            for key in expired:
                entries.pop(key, None)

        if max_entries > 0 and len(entries) > max_entries:
            ordered = sorted(
                entries.items(),
                key=lambda item: item[1].get("last_used", item[1].get("ts", 0)),
                reverse=True,
            )
            entries = dict(ordered[:max_entries])

        context["entries"] = entries

    def _cache_get(self, context, cache_key):
        entry = context.get("entries", {}).get(cache_key)
        if not entry:
            return None
        return entry

    def _cache_set(self, context, cache_key, metadata, provider):
        context.setdefault("entries", {})[cache_key] = {
            "metadata": metadata,
            "provider": provider,
            "ts": time.time(),
            "last_used": time.time(),
        }
        context["dirty"] = True

    def _get_cache_path(self):
        base_dir = getattr(settings, "MEDIA_ROOT", None) or getattr(settings, "BASE_DIR", "")
        return os.path.join(str(base_dir), "epg_enhancer_cache.json")

    def _get_progress_path(self):
        base_dir = getattr(settings, "MEDIA_ROOT", None) or getattr(settings, "BASE_DIR", "")
        return os.path.join(str(base_dir), "epg_enhancer_progress.json")

    def _get_last_result_path(self):
        base_dir = getattr(settings, "MEDIA_ROOT", None) or getattr(settings, "BASE_DIR", "")
        return os.path.join(str(base_dir), "epg_enhancer_last_result.json")

    def _get_exports_dir(self, override_path=None):
        if override_path:
            return override_path
        base_dir = getattr(settings, "MEDIA_ROOT", None) or getattr(settings, "BASE_DIR", "")
        return os.path.join(str(base_dir), "exports")

    def _write_json_file(self, path, payload, logger=None):
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, separators=(",", ":"))

    def _save_full_report(self, action, summary, updated, preview, skipped, exports_dir=None, logger=None):
        timestamp = timezone.now().strftime("%Y%m%d_%H%M%S")
        output_dir = self._get_exports_dir(override_path=exports_dir)
        report_file = os.path.join(output_dir, f"epg_enhancer_report_{timestamp}.json")
        latest_file = os.path.join(output_dir, "epg_enhancer_report_latest.json")

        payload = {
            "generated_at": timezone.now().isoformat(),
            "action": action,
            "summary": summary,
            "details": {
                "updated": updated,
                "preview": preview,
                "skipped": skipped,
            },
        }

        try:
            self._write_json_file(report_file, payload, logger=logger)
            self._write_json_file(latest_file, payload, logger=logger)
            if logger:
                logger.info("EPG Enhancer report saved: %s", report_file)
            return report_file
        except Exception as exc:
            if logger:
                logger.warning("EPG Enhancer failed to save full report: %s", exc)
            return None

    def _save_progress(self, progress, logger=None):
        path = self._get_progress_path()
        try:
            parent = os.path.dirname(path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(progress, handle, separators=(",", ":"))
        except Exception as exc:
            if logger:
                logger.warning("EPG Enhancer failed to save progress: %s", exc)

    def _load_progress(self, logger=None):
        path = self._get_progress_path()
        try:
            with open(path, "r", encoding="utf-8") as handle:
                progress = json.load(handle)

            state = progress.get("status", "unknown")
            action_name = progress.get("action", "unknown")
            attempted = progress.get("attempted", 0)
            total = progress.get("total_programs")
            remaining = progress.get("remaining")
            matched = progress.get("matched", 0)
            updated = progress.get("updated", 0)
            skipped = progress.get("skipped", 0)
            api_calls = progress.get("api_calls", {})
            tmdb_calls = api_calls.get("tmdb", 0)
            omdb_calls = api_calls.get("omdb", 0)

            total_str = "?" if total is None else str(total)
            remaining_str = "?" if remaining is None else str(remaining)
            message = (
                f"Progress [{action_name}:{state}] {attempted}/{total_str} attempted, "
                f"remaining {remaining_str}, matched {matched}, updated {updated}, "
                f"skipped {skipped}, API TMDB/OMDb {tmdb_calls}/{omdb_calls}."
            )

            return {
                "status": "ok",
                "message": message,
                "progress": progress,
            }
        except FileNotFoundError:
            return {
                "status": "ok",
                "message": "No active or previous progress state found.",
                "progress": None,
            }
        except Exception as exc:
            if logger:
                logger.warning("EPG Enhancer failed to load progress: %s", exc)
            return {
                "status": "error",
                "message": f"Failed to load progress: {exc}",
            }

    def _format_timestamp(self, value):
        if not value:
            return "unknown"
        try:
            dt = timezone.datetime.fromisoformat(value)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return str(value)

    def _save_last_run_result(self, summary, logger=None, report_file=None, exports_dir=None):
        path = self._get_last_result_path()
        try:
            output_dir = self._get_exports_dir(override_path=exports_dir)
            payload = {
                "saved_at": timezone.now().isoformat(),
                "summary": summary,
                "report_file": report_file,
                "report_latest_file": os.path.join(output_dir, "epg_enhancer_report_latest.json"),
            }
            self._write_json_file(path, payload, logger=logger)
        except Exception as exc:
            if logger:
                logger.warning("EPG Enhancer failed to save last-run result: %s", exc)

    def _load_last_run_result(self, logger=None):
        path = self._get_last_result_path()
        try:
            with open(path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)

            summary = payload.get("summary", {})
            attempted = summary.get("attempted", 0)
            matched = summary.get("matched", 0)
            updated = summary.get("updated", 0)
            skipped = summary.get("skipped", 0)
            dry_run = summary.get("dry_run", False)
            api_calls = summary.get("api_calls", {})
            tmdb_calls = api_calls.get("tmdb", 0)
            omdb_calls = api_calls.get("omdb", 0)
            saved_at = self._format_timestamp(payload.get("saved_at", ""))
            report_file = (
                payload.get("report_file")
                or payload.get("report_latest_file")
                or os.path.join(self._get_exports_dir(), "epg_enhancer_report_latest.json")
            )

            message = (
                f"Last run ({'dry-run' if dry_run else 'enhance'}) attempted {attempted}, "
                f"Report: {report_file}"
                f"matched {matched}, updated {updated}, skipped {skipped}, "
                f"API TMDB/OMDb {tmdb_calls}/{omdb_calls}. Saved: {saved_at}. "
            )

            return {
                "status": "ok",
                "message": message,
                "last_run": payload,
            }
        except FileNotFoundError:
            return {
                "status": "ok",
                "message": "No previous run result saved yet.",
                "last_run": None,
            }
        except Exception as exc:
            if logger:
                logger.warning("EPG Enhancer failed to load last-run result: %s", exc)
            return {
                "status": "error",
                "message": f"Failed to load last run result: {exc}",
            }

    def _clear_exports(self, logger=None):
        exports_dir = self._get_exports_dir()
        deleted = 0
        missing = 0
        errors = []

        export_targets = []
        try:
            if os.path.isdir(exports_dir):
                for name in os.listdir(exports_dir):
                    if name.startswith("epg_enhancer_") and name.endswith(".json"):
                        export_targets.append(os.path.join(exports_dir, name))
        except Exception as exc:
            return {"status": "error", "message": f"Failed to list exports: {exc}"}

        state_targets = [
            self._get_progress_path(),
            self._get_last_result_path(),
        ]

        for path in export_targets + state_targets:
            try:
                if os.path.exists(path):
                    os.remove(path)
                    deleted += 1
                else:
                    missing += 1
            except Exception as exc:
                errors.append(f"{path}: {exc}")

        message = (
            "Cleared export reports and run state files: "
            f"deleted={deleted}, missing={missing}."
        )
        if errors:
            message += f" {len(errors)} error(s): {'; '.join(errors[:3])}"
        if logger:
            logger.info(message)
        return {
            "status": "ok" if not errors else "error",
            "message": message,
            "deleted": deleted,
            "missing": missing,
            "errors": errors,
        }

    def _clear_cache(self, logger=None):
        targets = [
            self._get_cache_path(),
            self._get_cache_path() + ".lock",
        ]
        deleted = 0
        missing = 0
        errors = []

        for path in targets:
            try:
                if os.path.exists(path):
                    os.remove(path)
                    deleted += 1
                else:
                    missing += 1
            except Exception as exc:
                errors.append(f"{path}: {exc}")

        message = f"Cleared metadata cache files: deleted={deleted}, missing={missing}."
        if errors:
            message += f" {len(errors)} error(s): {'; '.join(errors[:2])}"
        if logger:
            logger.info(message)
        return {"status": "ok" if not errors else "error", "message": message, "deleted": deleted, "missing": missing, "errors": errors}

    def _start_background_action(self, action_id, settings=None, logger=None):
        """Queue enhance action in a background worker thread."""
        from apps.plugins.models import PluginConfig

        queued_at = timezone.now().isoformat()
        self._save_progress(
            {
                "status": "queued",
                "running": False,
                "action": action_id,
                "dry_run": False,
                "total_programs": None,
                "attempted": 0,
                "updated": 0,
                "preview_matches": 0,
                "matched": 0,
                "skipped": 0,
                "remaining": None,
                "api_calls": {"tmdb": 0, "omdb": 0},
                "started_at": queued_at,
                "finished_at": None,
            },
            logger=logger,
        )

        plugin_settings = settings or {}
        if not plugin_settings:
            plugin_instance = (
                PluginConfig.objects.filter(key="epg_enhancer").first()
                or PluginConfig.objects.filter(name=self.name).first()
            )
            plugin_settings = (plugin_instance.settings or {}) if plugin_instance else {}

        with _BACKGROUND_THREAD_LOCK:
            global _BACKGROUND_THREAD
            if _BACKGROUND_THREAD and _BACKGROUND_THREAD.is_alive():
                return {
                    "status": "ok",
                    "queued": False,
                    "already_running": True,
                    "message": "Enhancement is already running in background.",
                }

            run_lock_path = self._get_run_lock_path()
            run_lock_fd = self._acquire_run_lock(
                lock_path=run_lock_path,
                stale_seconds=RUN_LOCK_STALE_SECONDS,
            )
            if run_lock_fd is None:
                return {
                    "status": "ok",
                    "queued": False,
                    "already_running": True,
                    "message": "Enhancement is already running in another worker.",
                }

            def _thread_worker():
                try:
                    try:
                        from django.db import close_old_connections
                        close_old_connections()
                    except Exception:
                        pass
                    self.run(
                        action_id,
                        {"_background": True},
                        {"settings": plugin_settings, "logger": logger or LOGGER, "actions": {}},
                    )
                except Exception as exc:
                    (logger or LOGGER).exception("Background enhancement failed: %s", exc)
                    self._save_progress(
                        {
                            "status": "error",
                            "running": False,
                            "action": action_id,
                            "dry_run": bool(plugin_settings.get("dry_run", False)),
                            "message": str(exc),
                            "finished_at": timezone.now().isoformat(),
                        },
                        logger=logger,
                    )
                finally:
                    self._release_run_lock(run_lock_path, run_lock_fd, logger=logger)
                    try:
                        from django.db import close_old_connections
                        close_old_connections()
                    except Exception:
                        pass
                    with _BACKGROUND_THREAD_LOCK:
                        global _BACKGROUND_THREAD
                        _BACKGROUND_THREAD = None

            _BACKGROUND_THREAD = threading.Thread(
                target=_thread_worker,
                name=f"epg_enhancer_{action_id}",
                daemon=True,
            )
            _BACKGROUND_THREAD.start()

        return {
            "status": "ok",
            "queued": True,
            "queued_at": queued_at,
            "message": f"{action_id.capitalize()} queued in background. Use 'Check Progress' to monitor run state.",
        }

    def _get_run_lock_path(self):
        base_dir = getattr(settings, "MEDIA_ROOT", None) or getattr(settings, "BASE_DIR", "")
        return os.path.join(str(base_dir), "epg_enhancer_run.lock")

    def _acquire_run_lock(self, lock_path, stale_seconds=RUN_LOCK_STALE_SECONDS):
        while True:
            try:
                fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                payload = {
                    "pid": os.getpid(),
                    "acquired_at": timezone.now().isoformat(),
                }
                os.write(fd, json.dumps(payload, separators=(",", ":")).encode("utf-8"))
                return fd
            except FileExistsError:
                if not self._run_lock_is_stale(lock_path, stale_seconds=stale_seconds):
                    return None
                try:
                    os.remove(lock_path)
                except Exception:
                    return None

    def _run_lock_is_stale(self, lock_path, stale_seconds):
        if stale_seconds <= 0:
            return False
        try:
            with open(lock_path, "r", encoding="utf-8") as handle:
                raw = handle.read().strip()
            if not raw:
                return True
            payload = json.loads(raw)
            acquired_at = payload.get("acquired_at")
            if not acquired_at:
                return True
            acquired_dt = timezone.datetime.fromisoformat(acquired_at)
            age_seconds = (timezone.now() - acquired_dt).total_seconds()
            return age_seconds > stale_seconds
        except Exception:
            return True

    def _release_run_lock(self, lock_path, fd, logger=None):
        try:
            os.close(fd)
        except Exception:
            pass
        try:
            os.remove(lock_path)
        except FileNotFoundError:
            pass
        except Exception as exc:
            if logger:
                logger.warning("EPG Enhancer failed to release run lock: %s", exc)

    def _get_auto_trigger_state_path(self):
        base_dir = getattr(settings, "MEDIA_ROOT", None) or getattr(
            settings,
            "BASE_DIR",
            "",
        )
        return os.path.join(str(base_dir), "epg_enhancer_auto_trigger_state.json")

    def _allow_auto_trigger(self, source_id, debounce_seconds, logger=None):
        if debounce_seconds <= 0:
            return True

        state_path = self._get_auto_trigger_state_path()
        lock_path = state_path + ".lock"
        lock_fd = self._acquire_cache_lock(lock_path)
        if lock_fd is None:
            if logger:
                logger.warning("Auto-trigger lock timeout; skipping auto-enhance.")
            return False

        now_ts = time.time()
        try:
            payload = {"sources": {}}
            try:
                with open(state_path, "r", encoding="utf-8") as handle:
                    payload = json.load(handle)
            except FileNotFoundError:
                payload = {"sources": {}}
            except Exception:
                payload = {"sources": {}}

            sources = payload.get("sources") or {}
            source_key = str(source_id)
            last_ts = float(sources.get(source_key, 0) or 0)

            if last_ts and (now_ts - last_ts) < debounce_seconds:
                if logger:
                    logger.info(
                        "Auto-enhance debounced: source_id=%s elapsed=%.1fs window=%ss",
                        source_id,
                        now_ts - last_ts,
                        debounce_seconds,
                    )
                return False

            sources[source_key] = now_ts
            payload["sources"] = sources

            parent = os.path.dirname(state_path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            tmp_path = state_path + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, separators=(",", ":"))
            os.replace(tmp_path, state_path)
            return True
        finally:
            self._release_cache_lock(lock_path, lock_fd)


    def _acquire_cache_lock(self, lock_path, timeout_seconds=5):
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            try:
                fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                os.write(fd, str(os.getpid()).encode("ascii", "ignore"))
                return fd
            except FileExistsError:
                time.sleep(0.1)
        return None

    def _release_cache_lock(self, lock_path, fd):
        try:
            os.close(fd)
        finally:
            try:
                os.remove(lock_path)
            except FileNotFoundError:
                pass


# Add signal receiver for automatic triggering
@receiver(post_save, sender=EPGSource)
def on_epg_source_updated(sender, instance, **kwargs):
    """
    Automatically trigger EPG enhancement when EPG source is updated successfully.
    This hooks into the built-in EPG refresh completion.
    """
    if kwargs.get("raw"):
        return

    # Only trigger if the EPG source was successfully updated and is active
    if instance.status == 'success' and instance.is_active and instance.source_type != 'dummy':
        # Import here to avoid circular imports
        from apps.plugins.models import PluginConfig

        try:
            # Get the plugin instance by key first, fallback by name.
            plugin_instance = (
                PluginConfig.objects.filter(key="epg_enhancer").first()
                or PluginConfig.objects.filter(name="EPG Enhancer").first()
            )
            if not plugin_instance:
                return

            plugin_settings = plugin_instance.settings or {}

            # Check if auto-enhance is enabled
            if plugin_settings.get("auto_enhance", True):
                from apps.plugins.loader import PluginManager

                loaded = PluginManager.get().get_plugin("epg_enhancer")
                if loaded and loaded.instance and hasattr(loaded.instance, "_start_background_action"):
                    debounce_seconds = int(
                        plugin_settings.get(
                            "auto_enhance_debounce_seconds",
                            DEFAULT_AUTO_ENHANCE_DEBOUNCE_SECONDS,
                        )
                        or 0
                    )
                    if not loaded.instance._allow_auto_trigger(
                        source_id=instance.id,
                        debounce_seconds=debounce_seconds,
                        logger=LOGGER,
                    ):
                        return
                    loaded.instance._start_background_action(
                        action_id="enhance",
                        settings=plugin_settings,
                        logger=LOGGER,
                    )
                else:
                    LOGGER.warning("Auto-enhance skipped: epg_enhancer plugin not loaded.")
        except Exception as e:
            # Log error but do not break EPG processing.
            LOGGER.exception("Failed to trigger auto-enhancement for EPG %s: %s", instance.name, e)

