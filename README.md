# EPG Enhancer

Dispatcharr plugin that enhances EPG programs with metadata (title, year, genres, cast, scores) from TMDB or OMDb/IMDB and appends it to the program description.


## Installation Methods
### Method 1: Git Clone (Recommended)
The easiest and most reliable install path:

```bash
cd /path/to/dispatcharr/data/plugins/
git clone https://github.com/kabobandrice/EPG-Enhancer.git epg_enhancer
docker compose restart dispatcharr
```

Then enable the plugin in Dispatcharr `Settings -> Plugins`.

### Method 2: Download Release Asset
Download `epg_enhancer.zip` from the Releases page, then import via `Settings -> Plugins -> Import`.

## Updating
### Method 1 (Git Clone)
- `cd /path/to/dispatcharr/data/plugins/epg_enhancer`
- `git pull`
- Restart Dispatcharr app and worker processes.
- Run **Enhance Programs** with `Dry Run: true` after upgrades to verify template/settings behavior.

### Method 2 (Release Asset)
- Stop Dispatcharr, remove the existing `epg_enhancer` plugin folder (or delete the plugin from the Dispatcharr UI)
- Import/install the new version from releases.
- Restart Dispatcharr app and worker processes.
- Run **Enhance Programs** with `Dry Run: true` before first enhance on each new version.
- Existing EPG rows may be rebuilt on refresh; enhancements are reapplied by plugin runs.

## Settings
- **Metadata Provider**: `TMDB` (default), `OMDb / IMDB`, or `TMDB + OMDb (fallback)`.
- **Provider Priority**: When using both, choose which provider to try first.
- **Program Type Filter**: `movies`, `series`, or `both` to control which content gets enhanced.
- **TMDB API Key**: Required for TMDB provider. Get one at https://www.themoviedb.org/settings/api
- **OMDb API Key**: Required for OMDb provider. Get one at https://www.omdbapi.com/apikey.aspx
- **API Retry Count**: Retry failed API calls this many times.
- **Retry Backoff (seconds)**: Wait time between retry attempts.
- **Min Title Similarity (TMDB)**: Minimum title similarity to accept a TMDB match (0 = disabled).
- **Subtitle Match Similarity**: Minimum similarity for subtitle-based episode-title fallback matching.
- **Channel Group Name Filter**: Only process channels in this group name (case-insensitive). Leave blank for all.
- **Channel Name Regex**: Optional regex filter on channel names (e.g. `(?i)movie`).
- **Lookahead/Lookback Hours**: Time window to enhance programs (default: +12h / -12h).
- **Max Programs per Run**: Safety cap per invocation (default: 2000).
- **TMDB API Call Limit**: Maximum TMDB API calls per run (0 = unlimited).
- **OMDb API Call Limit**: Maximum OMDb API calls per run (0 = unlimited, default 1000).
- **Enable Metadata Cache**: Reuse metadata across refreshes to reduce API calls.
- **Cache TTL (hours)**: Expire cached metadata after this many hours (0 = never).
- **Cache Max Entries**: Maximum cached items to keep (0 = unlimited).
- **Exports Directory**: Optional absolute path for reports. Leave blank to use the app data exports folder.
- **Dry Run**: Preview without saving changes.
- **Replace Program Title**: Replace the program title using the title template.
- **Title Template**: Template for titles. Tokens: `{title}`, `{year}`, `{genre}`.
- **Series Title Template**: Optional title template override for series entries.
- **Replace Program Subtitle**: Replace `sub_title` using subtitle template.
- **Subtitle Template**: Template for subtitle when subtitle replacement is enabled. Tokens: `{episode_title}`, `{season_episode}`, `{episode}`, `{air_date}`, `{release_date}`.
- **Description Update Mode**: Append metadata block or replace the description entirely.
- **Description Template**: Template for metadata block. See Template token reference for available tokens.
- **Series Description Template**: Optional description template override for series entries.
- **Auto-Enhance on EPG Updates**: Automatically enhance programs when EPG data is updated (default: enabled).

### Template token reference
- `{title}`: Movie or series title.
- `{year}`: Release/air year.
- `{genre}`: First genre.
- `{genres}`: Comma-separated genres.
- `{runtime}`: Runtime.
- `{director}`: Director(s).
- `{writers}`: Writer(s).
- `{cast}`: Top cast list.
- `{scores}`: Ratings summary.
- `{overview}`: Plot summary.
- `{series_title}`: Series name.
- `{episode_title}`: Episode title.
- `{season}`: Season number.
- `{episode}`: Episode number.
- `{season_episode}`: Formatted `SxxEyy`.
- `{air_date}`: Air date (series/episode when available).
- `{release_date}`: Release date (movie when available).
- `{content_type}`: `movie` or `series`.

## Actions
- **Enhance Programs**: Queues enhancement in background by default and returns immediately.
- **Check Progress**: Shows current run progress (attempted, matched, updated, skipped, remaining, API call counts).
- **View Last Run Result**: Shows the last saved run summary and report file path for the most recent run.

- **Clear Exports**: Deletes `epg_enhancer_*.json` report files from `/data/exports` and resets progress/last-result state.
- **Clear Cache**: Clears metadata cache files only.

## Behavior
- Queries `ProgramData` entries within the configured time window, limited to channels that match the group and/or regex filters.
- **Smart Caching**: Uses content hashing to detect program changes and only re-processes when content actually changes.
- **Automatic Triggering**: Can automatically run when EPG sources are updated (when auto-enhance is enabled).
- Updates title/description based on templates and records metadata in `custom_properties`.
- Saves a full per-run report to configured exports path and updates `epg_enhancer_report_latest.json` in that folder.
- Uses TMDB `search/movie` / `search/tv` and details endpoints (with credits/external IDs) or OMDb `t` lookup with `type=movie|series`.
- For series/episodes, the plugin detects season/episode hints (for example `S02E05`) and attempts episode-level enrichment when supported by the provider.
- If no season/episode number is present but subtitle contains episode title, provider fallback matching attempts episode-title resolution.

## Notes
- For best matches, ensure EPG titles include the movie name (optionally with year). The plugin strips trailing `(YYYY)` when present.
- Keep the per-run limit modest if you expect many entries; heavy runs should be scheduled via smaller windows.
- OMDb has a daily request limit of 1,000/day for free accounts. Use **OMDb API Call Limit** and smaller windows to avoid hitting it.
- Network access to TMDB/OMDb must be allowed from the Dispatcharr host.

## Quick Start
Use this baseline for a safe first run:
- Provider: `both`
- Provider Priority: `tmdb_first`
- Add TMDB and OMDb API keys
- Dry Run: `true`
- Lookahead Hours: `4`
- Lookback Hours: `1`
- Max Programs per Run: `10`
- Channel Group Name Filter: `movies` (or your target group)
- Min Title Similarity (TMDB): `0.72`

After dry-run looks good, set `Dry Run` to `false` and run **Enhance Programs** again.

## Troubleshooting
- **No programs matched filters**: widen lookahead/lookback window, clear channel filters, or increase max programs.
- **No metadata found**: check title quality, lower TMDB similarity threshold slightly, and verify API keys.
- **Auto-enhance not triggering**: ensure `auto_enhance` is enabled, EPG source reaches `success`, and worker is running.
- **Rate-limit errors / API call limit reached**: lower run size, increase schedule spacing, and tune TMDB/OMDb call limits.
- **504 Gateway Time-out on run action**: web request timed out before plugin finished; use `Enhance Programs`, reduce `Max Programs per Run`, and use `Check Progress` / `View Last Run Result` to inspect run state and completion summary.
- **Plugin import issues**: plugin folder must be named exactly `epg_enhancer` under Dispatcharr `data/plugins/`.
- **Update import conflicts**: if Dispatcharr says plugin already exists or actions fail on some workers, remove existing `epg_enhancer` plugin first, then re-import and restart app + workers.


## License
This project includes a [`LICENSE`](LICENSE) file.

