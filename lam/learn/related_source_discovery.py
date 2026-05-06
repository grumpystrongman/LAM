from __future__ import annotations

import re
from typing import Any, Callable, Dict, List
from urllib.parse import urlparse

from .models import LearnMission


def discover_related_sources(mission: LearnMission, context: Dict[str, object] | None = None) -> List[Dict[str, object]]:
    ctx = dict(context or {})
    seeded = [dict(item) for item in list(ctx.get("mock_sources", []) or []) if isinstance(item, dict)]
    if seeded:
        for row in seeded:
            row.setdefault("discovery_mode", "mock")
            row.setdefault("live_collected", False)
        return seeded

    live_rows = _discover_live_sources(mission, ctx)
    if live_rows:
        templated = _template_sources(mission)
        return _merge_sources(live_rows, templated)
    return _template_sources(mission)


def _discover_live_sources(mission: LearnMission, context: Dict[str, object]) -> List[Dict[str, object]]:
    collector = context.get("source_collector")
    if not callable(collector):
        return []
    selected: List[Dict[str, object]] = []
    seen = set()
    for query in _candidate_queries(mission):
        try:
            payload = collector(query=query, topic=mission.topic, seed_url=mission.seed_url)
        except Exception:
            continue
        for row in _rows_from_collector_payload(payload):
            source_url = str(row.get("source_url", "") or "")
            if not source_url or source_url in seen:
                continue
            seen.add(source_url)
            row["live_collected"] = True
            row["discovery_mode"] = "live"
            selected.append(row)
        if len(selected) >= (1 + mission.max_related_videos + mission.max_supporting_sources + 3):
            break
    return selected


def _candidate_queries(mission: LearnMission) -> List[str]:
    topic = mission.topic.strip()
    queries = []
    if mission.seed_url:
        queries.append(f"{topic} tutorial related videos official docs")
    queries.extend(
        [
            f"{topic} step by step tutorial",
            f"{topic} official documentation",
            f"{topic} troubleshooting guide",
            f"{topic} best practices examples",
        ]
    )
    return queries[:5]


def _rows_from_collector_payload(payload: Any) -> List[Dict[str, object]]:
    if not isinstance(payload, dict):
        return []
    rows: List[Dict[str, object]] = []
    for row in list(payload.get("sources", []) or []):
        if isinstance(row, dict):
            rows.append(_normalize_source_row(row))
    for row in list(payload.get("search_results", []) or []):
        if isinstance(row, dict):
            rows.append(_normalize_source_row(row))
    return [row for row in rows if row.get("source_url")]


def _normalize_source_row(row: Dict[str, object]) -> Dict[str, object]:
    url = str(row.get("source_url", "") or row.get("url", "") or "").strip()
    title = str(row.get("title", "") or row.get("name", "") or url)
    snippet = str(row.get("snippet", "") or row.get("summary", "") or row.get("reason", "") or "")
    source_type = str(row.get("source_type", "") or _infer_source_type(url, title, snippet))
    normalized = {
        "source_url": url,
        "title": title,
        "source_type": source_type,
        "snippet": snippet,
        "channel": str(row.get("channel", "") or row.get("source", "") or ""),
        "upload_date": str(row.get("upload_date", "") or row.get("published_at", "") or ""),
        "captions": dict(row.get("captions", {}) or {}),
        "transcript": str(row.get("transcript", "") or ""),
    }
    if source_type == "video" and not normalized["captions"] and snippet:
        normalized["captions"] = {"auto": snippet}
    return normalized


def _template_sources(mission: LearnMission) -> List[Dict[str, object]]:
    topic = mission.topic
    slug = re.sub(r"[^a-z0-9]+", "-", topic.lower()).strip("-") or "topic"
    seed = []
    if mission.seed_url:
        seed.append(
            {
                "source_url": mission.seed_url,
                "title": f"Seed tutorial: {topic}",
                "source_type": "video",
                "channel": "Seed Channel",
                "captions": {"official": f"Open the tool. Build the workflow for {topic}. Validate the result."},
                "snippet": f"Step-by-step tutorial for {topic}.",
                "discovery_mode": "seeded",
                "live_collected": False,
            }
        )
    seed.extend(
        [
            {"source_url": f"https://video.example/{slug}-walkthrough", "title": f"{topic} full walkthrough", "source_type": "video", "channel": "Tutorial Channel", "captions": {"auto": f"Create the project, configure the settings, and review the output for {topic}."}, "snippet": f"Walkthrough for {topic}.", "discovery_mode": "templated", "live_collected": False},
            {"source_url": f"https://video.example/{slug}-advanced", "title": f"Advanced {topic} tips", "source_type": "video", "channel": "Advanced Channel", "captions": {"official": f"Open the advanced options, configure variations, and validate results for {topic}."}, "snippet": f"Advanced guidance for {topic}.", "discovery_mode": "templated", "live_collected": False},
            {"source_url": f"https://docs.example/{slug}", "title": f"Official docs for {topic}", "source_type": "docs", "snippet": f"Official documentation and recommended workflow for {topic}.", "discovery_mode": "templated", "live_collected": False},
            {"source_url": f"https://blog.example/{slug}-guide", "title": f"Practitioner guide to {topic}", "source_type": "blog", "snippet": f"Best practices and common mistakes for {topic}.", "discovery_mode": "templated", "live_collected": False},
            {"source_url": f"https://github.example/{slug}", "title": f"Reference examples for {topic}", "source_type": "github", "snippet": f"Example code and configuration for {topic}.", "discovery_mode": "templated", "live_collected": False},
            {"source_url": f"https://forum.example/{slug}-troubleshooting", "title": f"{topic} troubleshooting thread", "source_type": "forum", "snippet": f"Troubleshooting and caveats for {topic}.", "discovery_mode": "templated", "live_collected": False},
        ]
    )
    return seed


def _merge_sources(primary: List[Dict[str, object]], fallback: List[Dict[str, object]]) -> List[Dict[str, object]]:
    merged: List[Dict[str, object]] = []
    seen = set()
    for row in primary + fallback:
        url = str(row.get("source_url", "") or "")
        if not url or url in seen:
            continue
        seen.add(url)
        merged.append(dict(row))
    return merged


def _infer_source_type(url: str, title: str, snippet: str) -> str:
    host = urlparse(url).netloc.lower()
    low = f"{title} {snippet} {host}".lower()
    if any(item in host for item in ["youtube.com", "youtu.be", "vimeo.com", "loom.com"]):
        return "video"
    if "github.com" in host:
        return "github"
    if "learn.microsoft.com" in host or "docs" in host:
        return "docs"
    if "forum" in host or "stack" in low:
        return "forum"
    if any(item in low for item in ["tutorial", "walkthrough", "how to", "video"]):
        return "video"
    if "blog" in host:
        return "blog"
    return "other"
