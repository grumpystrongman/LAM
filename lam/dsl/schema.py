from __future__ import annotations

from typing import Any, Dict, Set


SUPPORTED_STEP_TYPES: Set[str] = {
    "click",
    "type",
    "hotkey",
    "wait_for",
    "assert_visible",
    "read_cell",
    "set_cell",
    "copy",
    "paste",
    "open_app",
    "focus_window",
    "navigate_url",
    "extract_field",
    "ask_user",
    "require_approval",
    "submit_action",
    "screenshot_redacted",
    "if",
    "for_each_row",
}

WORKFLOW_REQUIRED_FIELDS = {"id", "version", "steps", "publication"}
STEP_REQUIRED_FIELDS = {"id", "type"}


def workflow_schema_summary() -> Dict[str, Any]:
    return {
        "required_workflow_fields": sorted(WORKFLOW_REQUIRED_FIELDS),
        "supported_step_types": sorted(SUPPORTED_STEP_TYPES),
        "sensitivity_fields": ["data_classification", "write_impact", "requires_approval"],
        "control_fields": ["timeout_ms", "retry", "drift_policy"],
    }

