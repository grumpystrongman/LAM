from __future__ import annotations

import re
from typing import Any, Dict


def ingest_video_source(source_url: str, supplied: Dict[str, Any] | None = None) -> Dict[str, Any]:
    payload = dict(supplied or {})
    title = str(payload.get("title") or _infer_title(source_url))
    return {
        "source_url": source_url,
        "title": title,
        "channel": str(payload.get("channel", "")),
        "duration": str(payload.get("duration", "")),
        "upload_date": str(payload.get("upload_date", "")),
        "source_type": str(payload.get("source_type", "video")),
        "transcript": str(payload.get("transcript", "")),
        "captions": payload.get("captions", {}),
        "visual_notes": list(payload.get("visual_notes", []) or []),
        "snippet": str(payload.get("snippet", "")),
    }


def _infer_title(source_url: str) -> str:
    low = str(source_url or "").strip()
    match = re.search(r"[?&]v=([^&]+)", low)
    if match:
        return f"Video {match.group(1)}"
    tail = low.rstrip("/").split("/")[-1]
    tail = re.sub(r"[-_]+", " ", tail)
    return tail[:80] or "Seed video"
