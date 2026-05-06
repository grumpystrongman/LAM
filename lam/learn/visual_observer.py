from __future__ import annotations

import re
from typing import Dict, List


def observe_frames(source: Dict[str, object], sampled_frames: List[Dict[str, str]]) -> List[Dict[str, str]]:
    observations: List[Dict[str, str]] = []
    title = str(source.get("title", "") or "")
    app_hint = _app_hint(title)
    for frame in sampled_frames:
        text = str(frame.get("text", "") or "")
        observations.append(
            {
                "timestamp": str(frame.get("timestamp", "")),
                "app_or_site": app_hint,
                "workflow_stage": _stage(text),
                "ui_elements": _ui_tokens(text),
                "uncertainty": "low" if text else "medium",
            }
        )
    return observations


def _app_hint(title: str) -> str:
    low = title.lower()
    if "power bi" in low:
        return "Power BI"
    if "react" in low:
        return "React"
    if "grant" in low:
        return "Document editor / budgeting workflow"
    return "General application"


def _stage(text: str) -> str:
    low = text.lower()
    if any(t in low for t in ["open", "launch"]):
        return "setup"
    if any(t in low for t in ["click", "select", "choose"]):
        return "navigation"
    if any(t in low for t in ["type", "enter", "edit"]):
        return "data_entry"
    if any(t in low for t in ["validate", "check", "review"]):
        return "validation"
    return "workflow"


def _ui_tokens(text: str) -> str:
    words = re.findall(r"[A-Za-z][A-Za-z0-9+/.-]+", text or "")
    return ", ".join(words[:6])
