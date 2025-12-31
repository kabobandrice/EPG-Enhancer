import re
import requests
import hashlib
from datetime import timedelta
from django.utils import timezone
from django.db.models.signals import post_save
from django.dispatch import receiver

from apps.epg.models import ProgramData, EPGSource
from apps.channels.models import Channel


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
            ],
            "help_text": "TMDB requires an API key. OMDb also requires an API key for reliable results.",
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
            "default": 2,
            "help_text": "How far into the past to include programs (helps catch recent updates).",
        },
        {
            "id": "max_programs",
            "label": "Max Programs per Run",
            "type": "number",
            "default": 50,
            "help_text": "Safety limit for each run to avoid heavy loads.",
        },
        {
            "id": "dry_run",
            "label": "Dry Run",
            "type": "boolean",
            "default": False,
            "help_text": "When enabled, only preview changes without saving.",
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
            "default": "{title} ({year})",
            "help_text": "Template used when replacing titles. Tokens: {title} (movie title), {year} (release year), {genre} (first genre).",
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
            "default": "{title} ({year}) - {genres}\nCast: {cast}\nScores: {scores}\n{overview}",
            "help_text": (
                "Template for the metadata block. Tokens: {title} (movie title), "
                "{year} (release year), {genre} (first genre), {genres} (all genres), "
                "{cast} (top cast list), {scores} (ratings summary), {overview} (plot)."
            ),
        },
        {
            "id": "auto_enhance",
            "label": "Auto-Enhance on EPG Updates",
            "type": "boolean",
            "default": True,
            "help_text": "Automatically enhance programs when EPG data is updated.",
        },
    ]

    actions = [
        {
            "id": "preview",
            "label": "Preview Enrichment",
            "description": "Show which programs would be updated without saving.",
        },
        {
            "id": "enrich",
            "label": "Enhance Programs",
            "description": "Fetch metadata and update program descriptions.",
            "confirm": {
                "required": True,
                "title": "Enhance movie EPG entries?",
                "message": "This will fetch metadata and update program descriptions.",
            },
        },
    ]

    def run(self, action: str, params: dict, context: dict):
        logger = context.get("logger")
        settings = context.get("settings", {})

        if action not in {"preview", "enrich"}:
            return {"status": "error", "message": f"Unknown action {action}"}

        provider = settings.get("provider", "tmdb")
        tmdb_api_key = settings.get("tmdb_api_key", "").strip()
        omdb_api_key = settings.get("omdb_api_key", "").strip()
        channel_group_name = settings.get("channel_group_name", "").strip()
        channel_name_regex = settings.get("channel_name_regex", "").strip()
        lookahead_hours = int(settings.get("lookahead_hours", 12) or 12)
        lookback_hours = int(settings.get("lookback_hours", 2) or 0)
        max_programs = int(settings.get("max_programs", 50) or 50)
        dry_run = bool(settings.get("dry_run", False))
        replace_title = bool(settings.get("replace_title", False))
        description_mode = (settings.get("description_mode", "append") or "append").lower()
        title_template = settings.get("title_template", "{title} ({year})") or "{title} ({year})"
        description_template = settings.get(
            "description_template",
            "{title} ({year}) - {genres}\nCast: {cast}\nScores: {scores}\n{overview}",
        ) or "{title} ({year}) - {genres}\nCast: {cast}\nScores: {scores}\n{overview}"

        if provider == "tmdb" and not tmdb_api_key:
            return {"status": "error", "message": "TMDB API key is required when provider is TMDB."}

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
            return {"status": "ok", "message": "No programs matched filters."}

        updated = []
        skipped = []

        for program in programs:
            result = self._process_program(
                program=program,
                provider=provider,
                tmdb_api_key=tmdb_api_key,
                omdb_api_key=omdb_api_key,
                dry_run=dry_run or action == "preview",
                replace_title=replace_title,
                description_mode=description_mode,
                title_template=title_template,
                description_template=description_template,
                logger=logger,
            )
            if result["status"] == "updated":
                updated.append(result)
            else:
                skipped.append(result)

        summary = {
            "status": "ok",
            "updated": len(updated),
            "skipped": len(skipped),
            "dry_run": dry_run or action == "preview",
            "details": {
                "updated": updated[:10],  # cap detail noise
                "skipped": skipped[:10],
            },
        }

        if logger:
            logger.info(
                "EPG Enhancer finished: updated=%s skipped=%s dry_run=%s",
                len(updated),
                len(skipped),
                summary["dry_run"],
            )

        return summary

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
        replace_title,
        description_mode,
        title_template,
        description_template,
        logger,
    ):
        program_obj = program["program"]
        channels = program["channels"]
        title, year = self._extract_title_and_year(program_obj.title, program_obj.sub_title, program_obj.description)

        metadata = None
        error = None

        try:
            if provider == "tmdb":
                metadata = self._lookup_tmdb(title, year, tmdb_api_key)
            else:
                metadata = self._lookup_omdb(title, year, omdb_api_key)
        except Exception as exc:
            error = str(exc)

        if not metadata:
            return {
                "status": "skipped",
                "program": program_obj.title,
                "channel": channels[0].name if channels else "",
                "reason": error or "No metadata found",
            }

        enriched_block = self._render_template(description_template, metadata)
        if not enriched_block:
            return {
                "status": "skipped",
                "program": program_obj.title,
                "channel": channels[0].name if channels else "",
                "metadata": metadata,
                "reason": "Description template rendered empty",
            }

        # Content-based caching: only skip if we've processed this exact program content before
        already_applied = False
        current_content_hash = self._get_program_content_hash(program_obj)
        
        custom_props = program_obj.custom_properties or {}
        plugin_state = custom_props.get("epg_enhancer", {})
        
        # Check if we've processed this exact program content before
        stored_content_hash = plugin_state.get("content_hash")
        if stored_content_hash == current_content_hash:
            already_applied = True

        if dry_run or already_applied:
            return {
                "status": "skipped" if already_applied else "preview",
                "program": program_obj.title,
                "channel": channels[0].name if channels else "",
                "metadata": metadata,
                "reason": "Already processed" if already_applied else "Preview only",
            }

        if description_mode not in {"append", "replace"}:
            description_mode = "append"

        if description_mode == "replace":
            new_description = enriched_block
        else:
            new_description = enriched_block
            if program_obj.description:
                new_description = f"{program_obj.description.strip()}\n\n{enriched_block}"

        update_fields = ["description", "custom_properties"]

        if replace_title:
            new_title = self._render_title_template(title_template, metadata)
            if new_title:
                program_obj.title = new_title
                update_fields.append("title")

        stored_content_hash = self._get_content_hash(
            program_obj.title,
            program_obj.sub_title,
            new_description,
        )

        plugin_state = {
            "content_hash": stored_content_hash,
            "provider": provider,
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

        return {
            "status": "updated",
            "program": program_obj.title,
            "channel": channels[0].name if channels else "",
            "metadata": metadata,
        }

    def _extract_title_and_year(self, title, sub_title, description=None):
        """Extract clean title and year from title, subtitle, and description."""
        raw_title = title or ""

        # Check for year in title first
        match = re.search(r"\((\d{4})\)", raw_title)
        year = int(match.group(1)) if match else None

        # If no year in title, check description
        if not year and description:
            match = re.search(r"\((\d{4})\)", description)
            year = int(match.group(1)) if match else None

        # Clean the title by removing year
        clean_title = re.sub(r"\s*\(\d{4})\)\s*", "", raw_title).strip()
        if not clean_title and sub_title:
            clean_title = sub_title.strip()
            
        return clean_title, year

    def _lookup_tmdb(self, title, year, api_key):
        params = {
            "api_key": api_key,
            "query": title,
            "include_adult": False,
        }
        if year:
            params["year"] = year

        search_resp = requests.get(
            "https://api.themoviedb.org/3/search/movie", params=params, timeout=10
        )
        search_resp.raise_for_status()
        results = search_resp.json().get("results", [])
        if not results:
            return None

        # Prefer the most popular result to reduce mismatches on ambiguous titles.
        movie = max(results, key=lambda result: result.get("popularity") or 0)
        tmdb_id = movie.get("id")
        detail_resp = requests.get(
            f"https://api.themoviedb.org/3/movie/{tmdb_id}",
            params={"api_key": api_key, "append_to_response": "credits,release_dates,external_ids"},
            timeout=10,
        )
        detail_resp.raise_for_status()
        detail = detail_resp.json()

        credits = detail.get("credits", {})
        cast = [c["name"] for c in credits.get("cast", [])[:6]]

        ratings = {
            "tmdb": detail.get("vote_average"),
            "tmdb_count": detail.get("vote_count"),
        }

        imdb_id = detail.get("external_ids", {}).get("imdb_id")
        if imdb_id:
            ratings["imdb_id"] = imdb_id

        return {
            "provider": "tmdb",
            "tmdb_id": tmdb_id,
            "imdb_id": imdb_id,
            "title": detail.get("title") or title,
            "year": (detail.get("release_date") or "")[:4],
            "genres": [g["name"] for g in detail.get("genres", [])],
            "overview": detail.get("overview"),
            "cast": cast,
            "ratings": ratings,
        }

    def _lookup_omdb(self, title, year, api_key):
        params = {"apikey": api_key, "t": title, "type": "movie"}
        if year:
            params["y"] = year
        resp = requests.get("https://www.omdbapi.com/", params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("Response") != "True":
            return None

        ratings = {
            "imdb": data.get("imdbRating"),
            "rt": next((r["Value"] for r in data.get("Ratings", []) if r.get("Source") == "Rotten Tomatoes"), None),
            "metacritic": data.get("Metascore"),
        }

        cast = []
        if data.get("Actors"):
            cast = [actor.strip() for actor in data["Actors"].split(",")[:6]]

        genres = []
        if data.get("Genre"):
            genres = [g.strip() for g in data["Genre"].split(",")]

        return {
            "provider": "omdb",
            "imdb_id": data.get("imdbID"),
            "title": data.get("Title") or title,
            "year": data.get("Year"),
            "genres": genres,
            "overview": data.get("Plot"),
            "cast": cast,
            "ratings": ratings,
        }

    def _render_title_template(self, template, metadata):
        context = self._build_template_context(metadata)
        title_context = {
            "title": context["title"],
            "year": context["year"],
            "genre": context["genre"],
        }
        return self._render_template_from_context(template, title_context)

    def _render_template(self, template, metadata):
        context = self._build_template_context(metadata)
        return self._render_template_from_context(template, context)

    def _build_template_context(self, metadata):
        genres_list = metadata.get("genres") or []
        cast_list = metadata.get("cast") or []
        ratings = metadata.get("ratings") or {}

        rating_bits = []
        if ratings.get("tmdb"):
            rating_bits.append(f"TMDB {ratings['tmdb']}")
        if ratings.get("imdb"):
            rating_bits.append(f"IMDB {ratings['imdb']}")
        if ratings.get("rt"):
            rating_bits.append(f"RT {ratings['rt']}")
        if ratings.get("metacritic"):
            rating_bits.append(f"Metacritic {ratings['metacritic']}")

        return {
            "title": metadata.get("title") or "",
            "year": str(metadata.get("year") or ""),
            "genre": genres_list[0] if genres_list else "",
            "genres": ", ".join(genres_list) if genres_list else "",
            "cast": ", ".join(cast_list) if cast_list else "",
            "scores": " | ".join(rating_bits) if rating_bits else "",
            "overview": metadata.get("overview") or "",
        }

    def _render_template_from_context(self, template, context):
        rendered = template or ""
        for key, value in context.items():
            rendered = rendered.replace(f"{{{key}}}", value)
        rendered = re.sub(r"\{[a-z_]+\}", "", rendered)

        lines = []
        for line in rendered.splitlines():
            cleaned = re.sub(r"\(\s*\)", "", line)
            cleaned = re.sub(r"\s*-\s*$", "", cleaned)
            cleaned = re.sub(r"\s{2,}", " ", cleaned).rstrip()
            stripped = cleaned.strip()
            if not stripped:
                continue
            if re.match(r"^[A-Za-z ]+:\s*$", stripped):
                continue
            lines.append(cleaned)

        return "\n".join(lines).strip()


# Add signal receiver for automatic triggering
@receiver(post_save, sender=EPGSource)
def on_epg_source_updated(sender, instance, **kwargs):
    """
    Automatically trigger EPG enhancement when EPG source is updated successfully.
    This hooks into the built-in EPG refresh completion.
    """
    # Only trigger if the EPG source was successfully updated and is active
    if instance.status == 'success' and instance.is_active and instance.source_type != 'dummy':
        # Import here to avoid circular imports
        from apps.plugins.models import Plugin as PluginModel
        from apps.plugins.tasks import run_plugin_action
        
        try:
            # Get the plugin instance
            plugin_instance = PluginModel.objects.get(name="EPG Enhancer")
            plugin_settings = plugin_instance.settings or {}
            
            # Check if auto-enhance is enabled
            if plugin_settings.get("auto_enhance", True):
                # Run the enrichment in the background
                run_plugin_action.delay(
                    plugin_id=plugin_instance.id,
                    action="enrich",
                    params={},
                    user_id=None  # System-triggered
                )
        except PluginModel.DoesNotExist:
            # Plugin not installed, skip
            pass
        except Exception as e:
            # Log error but don't break EPG processing
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to trigger auto-enhancement for EPG {instance.name}: {e}")
