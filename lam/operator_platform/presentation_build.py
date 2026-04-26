from __future__ import annotations

from typing import Any, Dict, List


def build_presentation_outline(task_contract: Dict[str, Any], story_package: Dict[str, Any]) -> Dict[str, Any]:
    slides: List[Dict[str, Any]] = [
        {"title": "Title", "bullets": [task_contract.get("user_goal", "Executive briefing")]},
        {"title": "Agenda", "bullets": ["Objective", "Method", "Findings", "Recommendations", "Appendix"]},
        {"title": "Executive Summary", "bullets": [story_package.get("executive_summary", "")]},
        {"title": "Methodology", "bullets": ["Task contract extraction", "Capability planning", "Evidence-backed analysis"]},
        {"title": "Findings", "bullets": list(story_package.get("key_findings", []))[:5]},
        {"title": "Recommendations", "bullets": list(story_package.get("recommended_actions", []))[:5]},
        {"title": "Appendix", "bullets": list(story_package.get("caveats", []))[:5] or ["No appendix notes."]},
    ]
    return {
        "format": "markdown_slides",
        "slides": slides,
        "speaker_notes": {
            "summary": story_package.get("so_what", ""),
            "next_steps": story_package.get("next_steps", []),
        },
    }
