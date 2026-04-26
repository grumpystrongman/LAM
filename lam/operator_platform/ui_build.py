from __future__ import annotations

from typing import Any, Dict, List


def build_ui_delivery(requirements: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "chat_workspace": {
            "primary_focus": "conversation",
            "cards": ["progress", "approval", "completion"],
        },
        "canvas_panel": {
            "default_open": False,
            "modes": ["browser", "terminal", "artifact", "timeline", "debug"],
        },
        "information_architecture": [
            "sidebar",
            "chat_workspace",
            "composer",
            "canvas_panel",
        ],
        "components": [
            "AppShell",
            "Sidebar",
            "ChatThread",
            "CanvasPanel",
            "ArtifactList",
            "FeedbackControls",
        ],
        "acceptance_checks": [
            "main screen remains chat-first",
            "debug details are hidden by default",
            "canvas opens only when useful",
        ],
        "requirements": dict(requirements),
    }
