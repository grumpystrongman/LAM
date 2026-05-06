from __future__ import annotations

from typing import Any, Dict


def transcribe_audio_fallback(source: Dict[str, Any]) -> Dict[str, Any]:
    title = str(source.get("title", "video") or "video")
    audio_text = str(source.get("audio_text", "") or "").strip()
    snippet = str(source.get("snippet", "") or "").strip()
    text = audio_text or snippet or f"Low-confidence fallback transcript for {title}. Manual transcript verification recommended."
    coverage = 0.65 if audio_text else 0.35
    return {"text": text, "coverage": coverage, "method": "audio_fallback"}
