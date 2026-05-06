from __future__ import annotations

from typing import Dict, List


def extract_best_practices(source_notes: List[Dict[str, object]]) -> List[str]:
    practices: List[str] = []
    for note in source_notes:
        for line in list(note.get("highlights", []) or []):
            text = str(line or "").strip()
            low = text.lower()
            if any(token in low for token in ["best practice", "tip", "recommend", "validate", "check", "naming", "governance", "document"]):
                practices.append(text)
    deduped: List[str] = []
    seen = set()
    for item in practices:
        low = item.lower()
        if low not in seen:
            seen.add(low)
            deduped.append(item)
    return deduped[:12]
