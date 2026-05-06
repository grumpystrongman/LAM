from __future__ import annotations

import re
from typing import Dict, List

_ACTION_HINTS = ["click", "open", "type", "select", "run", "configure", "create", "build", "filter", "publish"]


def sample_frames(transcript_text: str, visual_notes: List[Dict[str, str]] | None = None) -> List[Dict[str, str]]:
    notes = list(visual_notes or [])
    samples: List[Dict[str, str]] = []
    sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", transcript_text or "") if s.strip()]
    ts = 0
    for sentence in sentences:
        low = sentence.lower()
        if any(token in low for token in _ACTION_HINTS):
            samples.append({"timestamp": f"00:{ts:02d}", "reason": "action_phrase", "text": sentence[:180]})
            ts += 5
        if len(samples) >= 8:
            break
    for item in notes[:4]:
        samples.append({"timestamp": str(item.get("timestamp", "")), "reason": "provided_visual_note", "text": str(item.get("text", ""))[:180]})
    return samples[:10]
