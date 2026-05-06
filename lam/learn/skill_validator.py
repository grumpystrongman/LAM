from __future__ import annotations

from typing import Dict, List

from .models import LearnedSkill


def validate_skill(skill: LearnedSkill) -> Dict[str, object]:
    issues: List[str] = []
    low_conf_steps = [step for step in skill.workflow if float(step.get("confidence", 0.0) or 0.0) < 0.65]
    risky_steps = [step for step in skill.workflow if bool(step.get("approval_required", False)) or str(step.get("risk_level", "low")) != "low"]
    selector_ready_steps = [step for step in skill.workflow if list(step.get("selector_suggestions", []) or [])]
    checkpoint_count = len(list(skill.checkpoints or []))
    if not skill.workflow:
        issues.append("no_workflow_steps")
    if low_conf_steps:
        issues.append("low_confidence_steps")
    if risky_steps:
        issues.append("contains_risky_steps")
    if skill.workflow and checkpoint_count == 0:
        issues.append("no_checkpoints")
    executable = not issues or issues == ["contains_risky_steps"]
    selector_coverage = round(len(selector_ready_steps) / max(1, len(skill.workflow or [])), 3)
    if executable and not risky_steps and selector_coverage >= 0.6 and checkpoint_count:
        status = "checkpoint_guided"
    else:
        status = "guided_only" if skill.workflow else "blocked"
    return {
        "passed": len([x for x in issues if x not in {"contains_risky_steps"}]) == 0,
        "issues": issues,
        "low_confidence_step_count": len(low_conf_steps),
        "risky_step_count": len(risky_steps),
        "selector_ready_step_count": len(selector_ready_steps),
        "selector_coverage": selector_coverage,
        "checkpoint_count": checkpoint_count,
        "executable_status": status,
        "required_repairs": [
            "Add stronger supporting sources for low-confidence steps." if low_conf_steps else "",
            "Require approval before risky steps are executed." if risky_steps else "",
            "Add or refine checkpoint and selector grounding before practice execution." if checkpoint_count == 0 or selector_coverage < 0.5 else "",
        ],
    }
