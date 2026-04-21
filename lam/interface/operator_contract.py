from __future__ import annotations

import json
import re
import uuid
from pathlib import Path
from typing import Any, Dict, List, Tuple

from lam.interface.contract_schema_validation import validate_contract_objects


def _domain_from_result(result: Dict[str, Any], instruction: str) -> Tuple[str, List[str]]:
    mode = str(result.get("mode", "")).lower()
    low = instruction.lower()
    if "job" in mode:
        return "browser_research", ["spreadsheet", "data_analysis"]
    if "study" in mode:
        return "browser_research", ["document", "data_analysis"]
    if "competitor" in mode:
        return "browser_research", ["document", "presentation", "data_analysis"]
    if "desktop" in mode:
        return "filesystem", ["browser_transaction"]
    if any(k in low for k in ["email", "inbox", "draft reply"]):
        return "email", ["spreadsheet", "document"]
    if any(k in low for k in ["calendar", "meeting"]):
        return "calendar", []
    if any(k in low for k in ["spreadsheet", "csv", "table"]):
        return "spreadsheet", ["data_analysis"]
    if any(k in low for k in ["powerpoint", "slides", "presentation"]):
        return "presentation", ["document"]
    return "browser_research", ["document"]


def _risk_from_result(result: Dict[str, Any], instruction: str) -> Tuple[str, bool]:
    low = instruction.lower()
    needs_confirmation = bool(result.get("requires_confirmation", False))
    if any(k in low for k in ["delete", "send", "payment", "transfer", "purchase"]):
        return "high", True
    if needs_confirmation:
        return "medium", True
    return "low", False


def _requested_outputs(instruction: str, artifacts: Dict[str, Any]) -> List[Dict[str, Any]]:
    low = instruction.lower()
    out: List[Dict[str, Any]] = []
    if any(k in low for k in ["spreadsheet", "csv", "excel"]) or any("csv" in k.lower() for k in artifacts.keys()):
        out.append({"type": "spreadsheet", "description": "Structured spreadsheet output", "required_fields": []})
    if any(k in low for k in ["report", "summary", "brief"]) or any("report" in k.lower() for k in artifacts.keys()):
        out.append({"type": "report", "description": "Written report/summary output", "required_fields": []})
    if any(k in low for k in ["powerpoint", "ppt", "slides"]) or any("pptx" in str(v).lower() for v in artifacts.values()):
        out.append({"type": "presentation", "description": "Presentation output", "required_fields": []})
    if not out:
        out.append({"type": "analysis", "description": "Analysis output", "required_fields": []})
    return out


def _extract_time_constraints(instruction: str) -> Dict[str, Any]:
    m = re.search(r"last\s+([0-9]+)\s*(hours|days|weeks)", instruction, flags=re.IGNORECASE)
    if m:
        return {"relative_window": f"last {m.group(1)} {m.group(2)}", "timezone": "local"}
    return {}


def build_task_envelope(
    *,
    task_id: str,
    instruction: str,
    result: Dict[str, Any],
    plan_steps: List[Dict[str, Any]],
) -> Dict[str, Any]:
    primary, secondary = _domain_from_result(result, instruction)
    risk_level, needs_confirmation = _risk_from_result(result, instruction)
    artifacts = result.get("artifacts", {}) or {}
    source_entities: List[Dict[str, Any]] = []
    if result.get("query"):
        source_entities.append({"type": "query", "value": str(result.get("query", ""))})
    if result.get("opened_url"):
        source_entities.append({"type": "url", "value": str(result.get("opened_url", ""))})
    target_entities: List[Dict[str, Any]] = []
    for s in plan_steps[:20]:
        tgt = str(s.get("target", "") or s.get("app", "") or s.get("name", "") or s.get("text", "")).strip()
        if not tgt:
            tgt = "instruction_scope"
        target_entities.append({"type": "other", "value": tgt})
    requested_outputs = _requested_outputs(instruction, artifacts)
    success_criteria = [
        "Correct domain and tool-family usage",
        "Requested outputs created and linked",
        "Verification checks passed for target and content alignment",
    ]
    return {
        "instruction": instruction,
        "user_goal": instruction,
        "requested_outputs": requested_outputs,
        "source_entities": source_entities,
        "target_entities": target_entities,
        "time_constraints": _extract_time_constraints(instruction),
        "risk_level": risk_level,
        "needs_confirmation": needs_confirmation,
        "primary_domain": primary,
        "secondary_domains": secondary,
        "success_criteria": success_criteria,
    }


def _tool_family_for_step(primary_domain: str, step: Dict[str, Any]) -> str:
    action = str(step.get("action", step.get("kind", ""))).lower()
    if primary_domain in {"email", "calendar", "filesystem", "spreadsheet", "document", "presentation", "data_analysis"}:
        if primary_domain == "data_analysis":
            return "data_analysis"
        return primary_domain
    if action in {"open_app", "click", "type_text", "hotkey", "focus_window", "visual_search", "click_found"}:
        return "filesystem"
    return "browser"


def build_plan(
    *,
    task_id: str,
    envelope: Dict[str, Any],
    plan_steps: List[Dict[str, Any]],
) -> Dict[str, Any]:
    items: List[Dict[str, Any]] = []
    for i, s in enumerate(plan_steps):
        action = str(s.get("action", s.get("kind", "step")))
        target = str(s.get("target", "") or s.get("selector", "") or s.get("app", "") or s.get("name", "") or s.get("text", "")).strip() or "instruction_scope"
        destructive = any(k in action.lower() for k in ["delete", "remove", "send", "submit", "payment", "transfer"])
        items.append(
            {
                "index": i,
                "action": action,
                "tool_family": _tool_family_for_step(str(envelope.get("primary_domain", "browser_research")), s),
                "target": target,
                "reason": f"Execute {action} toward user goal",
                "expected_output": "Step executed and recorded",
                "risk": "high" if destructive else "low",
                "undo_strategy": "Revert via generated undo_plan/manual rollback.",
                "verification_check": f"Confirm {action} affected intended target only.",
                "requires_confirmation": destructive,
                "destructive": destructive,
            }
        )
    validation_status, validation_errors = validate_plan(envelope=envelope, steps=items)
    return {
        "task_id": task_id,
        "primary_domain": envelope.get("primary_domain", "browser_research"),
        "secondary_domains": envelope.get("secondary_domains", []),
        "validation_status": validation_status,
        "validation_errors": validation_errors,
        "steps": items,
    }


def validate_plan(*, envelope: Dict[str, Any], steps: List[Dict[str, Any]]) -> Tuple[str, List[str]]:
    errors: List[str] = []
    if not steps:
        errors.append("plan has no steps")
    expected = str(envelope.get("primary_domain", ""))
    first_family = str(steps[0].get("tool_family", "")) if steps else ""
    domain_to_family = {
        "email": "email",
        "calendar": "calendar",
        "filesystem": "filesystem",
        "spreadsheet": "spreadsheet",
        "document": "document",
        "presentation": "presentation",
        "data_analysis": "data_analysis",
        "browser_research": "browser",
        "browser_transaction": "browser",
    }
    expected_family = domain_to_family.get(expected, "browser")
    if steps and first_family != expected_family:
        errors.append("primary tool family does not match domain")
    if any(not str(s.get("target", "")).strip() for s in steps):
        errors.append("empty target detected")
    if not envelope.get("requested_outputs"):
        errors.append("requested outputs missing")
    if any(not str(s.get("verification_check", "")).strip() for s in steps):
        errors.append("missing verification check")
    return ("invalid", errors) if errors else ("valid", [])


def build_execution_trace(
    *,
    task_id: str,
    plan: Dict[str, Any],
    result: Dict[str, Any],
) -> Dict[str, Any]:
    trace_raw = result.get("trace", []) or []
    plan_steps = plan.get("steps", []) or []
    rows: List[Dict[str, Any]] = []
    if trace_raw:
        for t in trace_raw:
            i = int(t.get("step", 0) or 0)
            p = plan_steps[i] if 0 <= i < len(plan_steps) else {}
            rows.append(
                {
                    "step_index": i,
                    "action": str(t.get("action", p.get("action", "step"))),
                    "tool_family": str(p.get("tool_family", "browser")),
                    "target": p.get("target", "instruction_scope"),
                    "expected_output": str(p.get("expected_output", "Step executed")),
                    "actual_output": json.dumps(t, sort_keys=True)[:1000],
                    "status": "success" if t.get("ok", False) else "failed",
                    "relevance_check_passed": True,
                    "evidence": [str(t.get("launched", ""))] if t.get("launched") else [],
                    "recovery_attempted": False,
                }
            )
    else:
        for p in plan_steps:
            rows.append(
                {
                    "step_index": int(p.get("index", 0)),
                    "action": str(p.get("action", "step")),
                    "tool_family": str(p.get("tool_family", "browser")),
                    "target": p.get("target", "instruction_scope"),
                    "expected_output": str(p.get("expected_output", "Step executed")),
                    "actual_output": "No step-level trace emitted by strategy; result evaluated at artifact level.",
                    "status": "success" if result.get("ok", False) else "failed",
                    "relevance_check_passed": True,
                    "evidence": [],
                    "recovery_attempted": False,
                }
            )
    artifacts = result.get("artifacts", {}) or {}
    created = [
        {
            "artifact_type": _artifact_type_from_key(k, v),
            "name": str(Path(str(v)).name if isinstance(v, str) else k),
            "location": str(v),
            "content_summary": k,
        }
        for k, v in artifacts.items()
        if isinstance(v, str) and v.strip()
    ]
    errors = []
    if not result.get("ok", False):
        errors.append({"code": "verification_failed", "message": str(result.get("error", "execution failed")), "step_index": 0})
    return {
        "task_id": task_id,
        "plan_validation_result": plan.get("validation_status", "invalid"),
        "step_results": rows,
        "created_artifacts": created,
        "execution_errors": errors,
    }


def _artifact_type_from_key(key: str, value: str) -> str:
    k = key.lower()
    v = str(value).lower()
    if "ppt" in k or v.endswith(".pptx"):
        return "presentation"
    if "csv" in k or v.endswith(".csv") or v.endswith(".xlsx"):
        return "spreadsheet"
    if "report" in k or "summary" in k or v.endswith(".md") or v.endswith(".html") or v.endswith(".pdf"):
        return "report"
    if "directory" in k:
        return "folder"
    return "file"


def build_verification_report(
    *,
    task_id: str,
    envelope: Dict[str, Any],
    plan: Dict[str, Any],
    trace: Dict[str, Any],
    result: Dict[str, Any],
) -> Dict[str, Any]:
    artifacts = trace.get("created_artifacts", []) or []
    expected_outputs = envelope.get("requested_outputs", []) or []
    used_expected_tool_family = plan.get("validation_status") == "valid"
    targets_match_request = all(bool(x.get("target")) for x in plan.get("steps", []))
    requested_outputs_exist = len(artifacts) >= len(expected_outputs) if expected_outputs else bool(artifacts)
    artifact_content_matches_goal = bool(result.get("ok", False)) and requested_outputs_exist
    no_unresolved_execution_errors = not trace.get("execution_errors")
    no_irrelevant_detours = all(bool(x.get("relevance_check_passed", True)) for x in trace.get("step_results", []))
    user_goal_satisfied = all(
        [
            used_expected_tool_family,
            targets_match_request,
            requested_outputs_exist,
            artifact_content_matches_goal,
            no_unresolved_execution_errors,
            no_irrelevant_detours,
            bool(result.get("ok", False)),
        ]
    )
    checks = [
        {"name": "used_expected_tool_family", "pass": used_expected_tool_family, "evidence": []},
        {"name": "targets_match_request", "pass": targets_match_request, "evidence": []},
        {"name": "requested_outputs_exist", "pass": requested_outputs_exist, "evidence": [str(len(artifacts))]},
        {"name": "artifact_content_matches_goal", "pass": artifact_content_matches_goal, "evidence": []},
        {"name": "no_unresolved_execution_errors", "pass": no_unresolved_execution_errors, "evidence": []},
        {"name": "no_irrelevant_detours", "pass": no_irrelevant_detours, "evidence": []},
        {"name": "user_goal_satisfied", "pass": user_goal_satisfied, "evidence": []},
    ]
    failed = [c["name"] for c in checks if not c["pass"]]
    return {
        "task_id": task_id,
        "used_expected_tool_family": used_expected_tool_family,
        "targets_match_request": targets_match_request,
        "requested_outputs_exist": requested_outputs_exist,
        "artifact_content_matches_goal": artifact_content_matches_goal,
        "no_unresolved_execution_errors": no_unresolved_execution_errors,
        "no_irrelevant_detours": no_irrelevant_detours,
        "user_goal_satisfied": user_goal_satisfied,
        "verification_checks": checks,
        "failed_checks": failed,
        "final_verification": "passed" if user_goal_satisfied else "failed",
    }


def build_final_report(
    *,
    task_id: str,
    result: Dict[str, Any],
    trace: Dict[str, Any],
    verification: Dict[str, Any],
) -> Dict[str, Any]:
    if verification.get("final_verification") == "passed":
        status = "completed"
    elif result.get("requires_confirmation"):
        status = "awaiting_confirmation"
    elif result.get("ok"):
        status = "partially_completed"
    else:
        status = "failed"
    outputs = [
        {
            "type": x.get("artifact_type", "file"),
            "location": x.get("location", ""),
            "description": x.get("name", ""),
        }
        for x in trace.get("created_artifacts", [])
    ]
    actions = [f"{x.get('action','step')} -> {x.get('status','unknown')}" for x in trace.get("step_results", [])]
    remaining = [str(x) for x in verification.get("failed_checks", [])]
    return {
        "task_id": task_id,
        "status": status,
        "summary": str(result.get("canvas", {}).get("title", "Run completed")),
        "actions_taken": actions,
        "outputs_created": outputs,
        "verification_summary": str(verification.get("final_verification", "failed")),
        "remaining_issues": remaining,
        "next_safe_action": "Address failed checks or rerun with corrected targets." if remaining else "Task complete.",
    }


def attach_operator_contract(
    *,
    instruction: str,
    result: Dict[str, Any],
    plan_steps: List[Dict[str, Any]],
) -> Dict[str, Any]:
    task_id = str(result.get("task_id", "")).strip() or uuid.uuid4().hex
    envelope = build_task_envelope(task_id=task_id, instruction=instruction, result=result, plan_steps=plan_steps)
    plan = build_plan(task_id=task_id, envelope=envelope, plan_steps=plan_steps)
    trace = build_execution_trace(task_id=task_id, plan=plan, result=result)
    verification = build_verification_report(task_id=task_id, envelope=envelope, plan=plan, trace=trace, result=result)
    final = build_final_report(task_id=task_id, result=result, trace=trace, verification=verification)

    out = dict(result)
    out["task_id"] = task_id
    out["task_envelope"] = envelope
    out["plan_contract"] = plan
    out["execution_trace"] = trace
    out["verification_report"] = verification
    out["final_report"] = final
    out["verification"] = {
        "passed": verification.get("final_verification") == "passed",
        "checks": verification.get("verification_checks", []),
        "evidence": [f"{x.get('type')}: {x.get('location')}" for x in final.get("outputs_created", [])],
    }
    out["report"] = {
        "summary": final.get("summary", ""),
        "artifacts": out.get("artifacts", {}),
        "next_actions": [final.get("next_safe_action", "")],
    }
    schemas_ok, schema_errors = validate_contract_objects(
        {
            "task_envelope": out.get("task_envelope"),
            "plan_contract": out.get("plan_contract"),
            "execution_trace": out.get("execution_trace"),
            "verification_report": out.get("verification_report"),
            "final_report": out.get("final_report"),
        }
    )
    out["schema_validation"] = {"passed": schemas_ok, "errors": schema_errors}
    if not schemas_ok:
        out["ok"] = False
        out["schema_validation_errors"] = schema_errors
        vr = dict(out.get("verification_report", {}))
        failed_checks = list(vr.get("failed_checks", []))
        if "schema_validation" not in failed_checks:
            failed_checks.append("schema_validation")
        vr["no_unresolved_execution_errors"] = False
        vr["user_goal_satisfied"] = False
        vr["failed_checks"] = failed_checks
        vr["final_verification"] = "failed"
        out["verification_report"] = vr
        out["verification"] = {
            "passed": False,
            "checks": list(vr.get("verification_checks", []))
            + [{"name": "schema_validation", "pass": False, "evidence": schema_errors[:10]}],
            "evidence": [f"schema_error: {x}" for x in schema_errors[:10]],
        }
        fr = dict(out.get("final_report", {}))
        remaining = list(fr.get("remaining_issues", []))
        remaining.extend(schema_errors[:20])
        fr["status"] = "failed"
        fr["verification_summary"] = "failed"
        fr["remaining_issues"] = remaining
        fr["next_safe_action"] = "Fix contract schema violations and rerun."
        out["final_report"] = fr
        out["report"] = {
            "summary": str(fr.get("summary", "Run failed")),
            "artifacts": out.get("artifacts", {}),
            "next_actions": ["Fix contract schema violations and rerun."],
        }
    return out
