# EPG Enhancer

Dispatcharr plugin that enriches EPG programs with metadata (title, year, genres, cast, scores) from TMDB or OMDb/IMDB and appends it to the program description.

## Settings
- **Metadata Provider**: `TMDB` (default), `OMDb / IMDB`, or `TMDB + OMDb (fallback)`.
- **Provider Priority**: When using both, choose which provider to try first.
- **TMDB API Key**: Required for TMDB provider. Get one at https://www.themoviedb.org/settings/api
- **OMDb API Key**: Required for OMDb provider. Get one at https://www.omdbapi.com/apikey.aspx
- **API Retry Count**: Retry failed API calls this many times.
- **Retry Backoff (seconds)**: Wait time between retry attempts.
- **Min Title Similarity (TMDB)**: Minimum title similarity to accept a TMDB match (0 = disabled).
- **Channel Group Name Filter**: Only process channels in this group name (case-insensitive). Leave blank for all.
- **Channel Name Regex**: Optional regex filter on channel names (e.g. `(?i)movie`).
- **Lookahead/Lookback Hours**: Time window to enrich programs (default: +12h / -2h).
- **Max Programs per Run**: Safety cap per invocation (default: 50).
- **TMDB API Call Limit**: Maximum TMDB API calls per run (0 = unlimited).
- **OMDb API Call Limit**: Maximum OMDb API calls per run (0 = unlimited, default 1000).
- **Enable Metadata Cache**: Reuse metadata across refreshes to reduce API calls.
- **Cache TTL (hours)**: Expire cached metadata after this many hours (0 = never).
- **Cache Max Entries**: Maximum cached items to keep (0 = unlimited).
- **Dry Run**: Preview without saving changes.
- **Replace Program Title**: Replace the program title using the title template.
- **Title Template**: Template for titles. Tokens: `{title}` (movie title), `{year}` (release year), `{genre}` (first genre).
- **Description Update Mode**: Append metadata block or replace the description entirely.
- **Description Template**: Template for the metadata block. Tokens: `{title}` (movie title), `{year}` (release year), `{genre}` (first genre), `{genres}` (all genres), `{runtime}` (runtime), `{cast}` (top cast list), `{scores}` (ratings summary), `{overview}` (plot).
- **Auto-Enhance on EPG Updates**: Automatically enhance programs when EPG data is updated (default: enabled).

## Actions
- **Preview Enrichment**: Lists programs that would be touched and fetched metadata.
- **Enhance Programs**: Fetches metadata and updates descriptions. Confirm modal is shown.

## Behavior
- Queries `ProgramData` entries within the configured time window, limited to channels that match the group and/or regex filters.
- **Smart Caching**: Uses content hashing to detect program changes and only re-processes when content actually changes.
- **Automatic Triggering**: Can automatically run when EPG sources are updated (when auto-enhance is enabled).
- Updates title/description based on templates and records metadata in `custom_properties`.
- Uses TMDB `search/movie` + `movie/{id}` (with credits/external IDs) or OMDb `t` lookup.

## Notes
- For best matches, ensure EPG titles include the movie name (optionally with year). The plugin strips trailing `(YYYY)` when present.
- Keep the per-run limit modest if you expect many entries; heavy runs should be scheduled via smaller windows.
- OMDb has a daily request limit of 1,000/day for free accounts. Use **OMDb API Call Limit** and smaller windows to avoid hitting it.
- Network access to TMDB/OMDb must be allowed from the Dispatcharr host.
