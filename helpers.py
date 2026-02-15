import re
import difflib


def build_template_context(metadata):
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

    season = metadata.get("season")
    episode = metadata.get("episode")
    season_episode = ""
    if season and episode:
        season_episode = f"S{int(season):02d}E{int(episode):02d}"

    return {
        "title": metadata.get("title") or "",
        "series_title": metadata.get("series_title") or "",
        "episode_title": metadata.get("episode_title") or "",
        "season": str(season or ""),
        "episode": str(episode or ""),
        "season_episode": season_episode,
        "content_type": metadata.get("content_type") or "",
        "year": str(metadata.get("year") or ""),
        "genre": genres_list[0] if genres_list else "",
        "genres": ", ".join(genres_list) if genres_list else "",
        "runtime": str(metadata.get("runtime") or ""),
        "director": metadata.get("director") or "",
        "writers": metadata.get("writers") or "",
        "cast": ", ".join(cast_list) if cast_list else "",
        "scores": " | ".join(rating_bits) if rating_bits else "",
        "overview": metadata.get("overview") or "",
    }


def render_template_from_context(template, context):
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


def render_template(template, metadata):
    context = build_template_context(metadata)
    return render_template_from_context(template, context)


def render_title_template(template, metadata):
    context = build_template_context(metadata)
    title_context = {
        "title": context["title"],
        "year": context["year"],
        "genre": context["genre"],
    }
    return render_template_from_context(template, title_context)


def normalize_title(title):
    value = (title or "").lower()
    value = re.sub(r"\(\d{4}\)", "", value)
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def title_similarity(left, right):
    left_norm = normalize_title(left)
    right_norm = normalize_title(right)
    if not left_norm or not right_norm:
        return 0.0
    return difflib.SequenceMatcher(a=left_norm, b=right_norm).ratio()
