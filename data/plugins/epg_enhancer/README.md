# EPG Enhancer

Dispatcharr plugin that enriches EPG programs with metadata (title, year, genres, cast, scores) from TMDB or OMDb/IMDB and appends it to the program description.

## Settings
- **Metadata Provider**: `TMDB` (default) or `OMDb / IMDB`.
- **TMDB API Key**: Required for TMDB provider.
- **OMDb API Key**: Optional, used when provider is OMDb; also adds RT/Metacritic/IMDB scores when available.
- **Channel Group Name Filter**: Only process channels in this group name (case-insensitive). Leave blank for all.
- **Channel Name Regex**: Optional regex filter on channel names (e.g. `(?i)movie`).
- **Lookahead/Lookback Hours**: Time window to enrich programs (default: +12h / -2h).
- **Max Programs per Run**: Safety cap per invocation (default: 50).
- **Dry Run**: Preview without saving changes.
- **Auto-Enhance on EPG Updates**: Automatically enhance programs when EPG data is updated (default: enabled).

## Actions
- **Preview Enrichment**: Lists programs that would be touched and fetched metadata.
- **Enhance Programs**: Fetches metadata and updates descriptions. Confirm modal is shown.

## Behavior
- Queries `ProgramData` entries within the configured time window, limited to channels that match the group and/or regex filters.
- **Smart Caching**: Uses content hashing to detect program changes and only re-processes when content actually changes.
- **Automatic Triggering**: Can automatically run when EPG sources are updated (when auto-enhance is enabled).
- Appends a formatted metadata block to the program description and records metadata in `custom_properties`.
- Uses TMDB `search/movie` + `movie/{id}` (with credits/external IDs) or OMDb `t` lookup.

## Notes
- For best matches, ensure EPG titles include the movie name (optionally with year). The plugin strips trailing `(YYYY)` when present.
- Keep the per-run limit modest if you expect many entries; heavy runs should be scheduled via smaller windows.
- Network access to TMDB/OMDb must be allowed from the Dispatcharr host.
