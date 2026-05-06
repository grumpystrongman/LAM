from __future__ import annotations

from typing import Dict, List


class ContradictionDetector:
    def detect(self, source_notes: List[Dict[str, object]]) -> List[Dict[str, str]]:
        contradictions: List[Dict[str, str]] = []
        positive: List[tuple[str, str]] = []
        negative: List[tuple[str, str]] = []
        for note in source_notes:
            title = str(note.get("title", "") or note.get("source_url", ""))
            text = " ".join([str(x) for x in note.get("highlights", []) or []]).lower()
            if any(t in text for t in ["always use", "must use", "recommended to use"]):
                positive.append((title, text))
            if any(t in text for t in ["avoid using", "do not use", "never use"]):
                negative.append((title, text))
        for left_title, left_text in positive:
            for right_title, right_text in negative:
                shared = next((token for token in left_text.split() if len(token) > 4 and token in right_text), "")
                if shared:
                    contradictions.append(
                        {
                            "topic": shared,
                            "left_source": left_title,
                            "right_source": right_title,
                            "resolution": "Likely version or workflow variation. Treat as conditional guidance and verify with official docs.",
                        }
                    )
                    break
        return contradictions[:8]
