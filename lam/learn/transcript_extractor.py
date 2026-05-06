from __future__ import annotations

from typing import Any, Dict

from .audio_transcriber import transcribe_audio_fallback


def extract_transcript(source: Dict[str, Any]) -> Dict[str, Any]:
    captions = dict(source.get("captions", {}) or {})
    official = str(captions.get("official", "") or "").strip()
    auto = str(captions.get("auto", "") or "").strip()
    transcript = str(source.get("transcript", "") or "").strip()
    if official:
        return {"text": official, "coverage": 0.98, "method": "official_captions"}
    if transcript:
        return {"text": transcript, "coverage": 0.92, "method": "provided_transcript"}
    if auto:
        return {"text": auto, "coverage": 0.8, "method": "auto_captions"}
    return transcribe_audio_fallback(source)
