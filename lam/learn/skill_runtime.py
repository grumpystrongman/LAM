from __future__ import annotations

import re
from typing import Any, Dict, List

from lam.interface.desktop_sequence import SequenceResult, execute_plan


class SkillPracticeRuntime:
    def build_preview(self, skill: Dict[str, Any], *, mode: str = "safe_practice") -> Dict[str, Any]:
        workflow = [dict(item) for item in list(skill.get("workflow", []) or []) if isinstance(item, dict)]
        app_context = dict(skill.get("app_context", {}) or {})
        checkpoints = list(skill.get("checkpoints", []) or []) or self._derive_checkpoints(workflow)
        steps: List[Dict[str, Any]] = []
        app_name = str(app_context.get("app_name", "") or "").strip()
        if app_name and app_name.lower() not in {"browser", "code editor + browser"}:
            steps.append({"action": "open_app", "app": app_name, "checkpoint_name": "Open application", "checkpoint_id": "open_application"})
        for checkpoint in checkpoints:
            step = dict(checkpoint.get("step_payload", {}) or {})
            checkpoint_id = str(checkpoint.get("checkpoint_id", "") or "")
            checkpoint_name = str(checkpoint.get("checkpoint_name", "") or "")
            selectors = [dict(item or {}) for item in list(step.get("selector_suggestions", []) or []) if isinstance(item, dict)]
            expected_state = dict((step.get("ui_grounding", {}) or {}).get("expected_state", {}) or {})
            if selectors or expected_state.get("labels"):
                steps.append(
                    {
                        "action": "assert_state",
                        "candidate_selectors": _state_selectors(expected_state, selectors),
                        "description": f"Verify checkpoint start: {checkpoint_name}",
                        "phase": "pre",
                        "checkpoint_id": checkpoint_id,
                        "checkpoint_name": checkpoint_name,
                        "optional": True,
                        "recovery_hint": f"Re-open the workspace or restore the UI state before {checkpoint_name}.",
                    }
                )
            steps.extend(self._checkpoint_actions(step, checkpoint_id=checkpoint_id, checkpoint_name=checkpoint_name, mode=mode))
            steps.append(
                {
                    "action": "assert_state",
                    "candidate_selectors": _state_selectors(expected_state, selectors),
                    "description": f"Validate checkpoint result: {checkpoint_name}",
                    "phase": "post",
                    "checkpoint_id": checkpoint_id,
                    "checkpoint_name": checkpoint_name,
                    "optional": False,
                    "recovery_hint": f"Checkpoint failed after {checkpoint_name}. Review selector hints or re-teach that step.",
                }
            )
        safe_steps = sum(1 for item in steps if str(item.get("action", "")) not in {"type_text"})
        return {
            "skill_id": str(skill.get("skill_id", "")),
            "skill_name": str(skill.get("skill_name", "")),
            "topic": str(skill.get("topic", "")),
            "mode": mode,
            "app_name": app_name,
            "checkpoints": checkpoints,
            "plan": {
                "instruction": f"Practice learned skill {skill.get('skill_name', skill.get('skill_id', 'skill'))}",
                "app_name": app_name,
                "checkpoint_after_open": False,
                "steps": steps,
            },
            "checkpoint_policy": {
                "enforced": True,
                "policy": "checkpoint_by_checkpoint",
                "checkpoint_count": len(checkpoints),
                "risky_steps_blocked": [cp["checkpoint_id"] for cp in checkpoints if bool(cp.get("approval_required", False))],
            },
            "safe_step_count": safe_steps,
            "blocked_step_count": max(0, len(steps) - safe_steps),
            "can_autorun": bool(checkpoints) and safe_steps > 0,
        }

    def execute_practice(self, skill: Dict[str, Any], *, mode: str = "safe_practice", allow_input_fallback: bool = True, human_like_interaction: bool = False) -> Dict[str, Any]:
        preview = self.build_preview(skill, mode=mode)
        plan = dict(preview.get("plan", {}) or {})
        checkpoints = list(preview.get("checkpoints", []) or [])
        if not checkpoints:
            return {"ok": False, "error": "no_checkpoints", "preview": preview, "checkpoint_runs": []}
        checkpoint_runs = []
        trace: List[Dict[str, Any]] = []
        artifacts: Dict[str, str] = {}
        plan_steps = list(plan.get("steps", []) or [])
        checkpoint_indexes = _checkpoint_indexes(plan_steps)
        for idx, entry in enumerate(checkpoint_indexes):
            start = int(entry["start"])
            end = int(entry["end"])
            subset = {
                "instruction": str(plan.get("instruction", "")),
                "app_name": str(plan.get("app_name", "")),
                "checkpoint_after_open": False,
                "steps": plan_steps[start : end + 1],
            }
            run = execute_plan(
                subset,
                start_index=0,
                step_mode=False,
                allow_input_fallback=allow_input_fallback,
                human_like_interaction=human_like_interaction,
            )
            trace.extend(list(run.trace or []))
            artifacts.update(dict(run.artifacts or {}))
            checkpoint_runs.append(
                {
                    "checkpoint_id": entry["checkpoint_id"],
                    "checkpoint_name": entry["checkpoint_name"],
                    "ok": bool(run.ok),
                    "error": str(run.error or ""),
                    "trace_count": len(list(run.trace or [])),
                }
            )
            if not run.ok:
                return {
                    "ok": False,
                    "preview": preview,
                    "checkpoint_runs": checkpoint_runs,
                    "failed_checkpoint_id": entry["checkpoint_id"],
                    "failed_checkpoint_name": entry["checkpoint_name"],
                    "trace": trace,
                    "artifacts": artifacts,
                    "error": run.error,
                }
            if idx == 0 and plan_steps and str(plan_steps[0].get("action", "")) == "open_app":
                plan_steps[0]["action"] = "note"
        return {"ok": True, "preview": preview, "checkpoint_runs": checkpoint_runs, "trace": trace, "artifacts": artifacts}

    def _derive_checkpoints(self, workflow: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        checkpoints = []
        for idx, step in enumerate(workflow, start=1):
            action = str(step.get("action_type", "workflow") or "workflow")
            checkpoint_name = f"{idx}. {action.title()} - {str(step.get('description', '') or '').strip()[:50]}".strip()
            checkpoints.append(
                {
                    "checkpoint_id": _slug(checkpoint_name),
                    "checkpoint_name": checkpoint_name,
                    "step_index": idx - 1,
                    "approval_required": bool(step.get("approval_required", False)),
                    "risk_level": str(step.get("risk_level", "low")),
                    "step_payload": step,
                }
            )
        return checkpoints

    def _checkpoint_actions(self, step: Dict[str, Any], *, checkpoint_id: str, checkpoint_name: str, mode: str) -> List[Dict[str, Any]]:
        execution_target = dict((step.get("ui_grounding", {}) or {}).get("execution_target", {}) or {})
        action = str(execution_target.get("action", step.get("action_type", "note")) or "note")
        selectors = [dict(item or {}) for item in list(step.get("selector_suggestions", []) or []) if isinstance(item, dict)]
        description = str(step.get("description", "") or "")
        risk = str(step.get("risk_level", "low") or "low")
        if action == "open_app":
            return [{"action": "note", "text": description, "checkpoint_id": checkpoint_id, "checkpoint_name": checkpoint_name}]
        if action == "click" and selectors and risk == "low":
            return [
                {
                    "action": "click",
                    "selector": selectors[0],
                    "fallback_selectors": selectors[1:4],
                    "checkpoint_id": checkpoint_id,
                    "checkpoint_name": checkpoint_name,
                    "recovery_hint": f"Update selector grounding for {checkpoint_name} or run safe preview only.",
                }
            ]
        if action == "assert_state":
            return [{"action": "note", "text": description, "checkpoint_id": checkpoint_id, "checkpoint_name": checkpoint_name}]
        if action == "type_text":
            return [
                {
                    "action": "note",
                    "text": f"Manual input required: {description}",
                    "checkpoint_id": checkpoint_id,
                    "checkpoint_name": checkpoint_name,
                }
            ]
        return [{"action": "note", "text": description, "checkpoint_id": checkpoint_id, "checkpoint_name": checkpoint_name}]


def _slug(text: str) -> str:
    clean = re.sub(r"[^a-z0-9]+", "_", str(text or "").lower()).strip("_")
    return clean[:60] or "checkpoint"


def _state_selectors(expected_state: Dict[str, Any], selectors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    for selector in selectors[:3]:
        kind = str(selector.get("kind", "label") or "label")
        value = str(selector.get("value", "") or "")
        if not value:
            continue
        strategy = "text"
        if kind == "automation_id":
            strategy = "automation_id"
        elif kind == "role":
            strategy = "role"
        candidates.append({"strategy": strategy, "value": value})
    for label in list(expected_state.get("labels", []) or [])[:3]:
        if label:
            candidates.append({"strategy": "text", "value": str(label)})
    return candidates[:5]


def _checkpoint_indexes(steps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    indexes: List[Dict[str, Any]] = []
    current_id = ""
    current_name = ""
    start = 0
    for idx, step in enumerate(steps):
        checkpoint_id = str(step.get("checkpoint_id", "") or "")
        checkpoint_name = str(step.get("checkpoint_name", "") or "")
        if not checkpoint_id:
            continue
        if not current_id:
            current_id = checkpoint_id
            current_name = checkpoint_name
            start = idx
            continue
        if checkpoint_id != current_id:
            indexes.append({"checkpoint_id": current_id, "checkpoint_name": current_name, "start": start, "end": idx - 1})
            current_id = checkpoint_id
            current_name = checkpoint_name
            start = idx
    if current_id:
        indexes.append({"checkpoint_id": current_id, "checkpoint_name": current_name, "start": start, "end": len(steps) - 1})
    return indexes
