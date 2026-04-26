from __future__ import annotations

from typing import Any, Dict, List


def build_story_package(task_contract: Dict[str, Any], analysis_outputs: Dict[str, Any]) -> Dict[str, Any]:
    audience = str(task_contract.get("audience", "operator"))
    insights = analysis_outputs.get("insights", []) if isinstance(analysis_outputs.get("insights"), list) else []
    caveats = analysis_outputs.get("caveats", []) if isinstance(analysis_outputs.get("caveats"), list) else []
    findings = analysis_outputs.get("findings", []) if isinstance(analysis_outputs.get("findings"), list) else insights[:3]
    executive_summary = f"This {task_contract.get('domain', 'analysis')} package is ready for {audience} review."
    so_what = "The results identify where follow-up work or decisions should focus first."
    actions = analysis_outputs.get("recommended_actions", []) if isinstance(analysis_outputs.get("recommended_actions"), list) else ["Review the flagged findings and validate the underlying data."]
    next_steps = analysis_outputs.get("next_steps", []) if isinstance(analysis_outputs.get("next_steps"), list) else ["Open the artifacts and validate the top issues."]
    return {
        "executive_summary": executive_summary,
        "key_findings": findings[:5],
        "so_what": so_what,
        "recommended_actions": actions[:5],
        "caveats": caveats[:5],
        "next_steps": next_steps[:5],
        "slide_narrative_outline": [
            "Context",
            "Method",
            "What changed",
            "Key findings",
            "Recommended actions",
            "Caveats",
        ],
    }
