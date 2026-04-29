from __future__ import annotations

import json
import re
import uuid
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

from lam.interface.contract_schema_validation import validate_contract_objects


_INVALID_TARGETS = {
    "",
    "instruction_scope",
    "current_task",
    "user_request",
    "relevant_result",
    "best_match",
    "task_context",
    "task_scope",
    "current_context",
}

_GENERIC_EXPECTED_OUTPUT_PHRASES = {
    "step executed and recorded",
    "progress made",
    "some result returned",
    "artifact level evaluated later",
}

_ACTION_FAMILY_MAP = {
    "web_search": "browser",
    "open_result": "browser",
    "navigate_url": "browser",
    "research": "browser",
    "extract": "data_analysis",
    "analyze": "data_analysis",
    "present": "browser",
    "produce": "document",
    "open_app": "filesystem",
    "focus_window": "filesystem",
    "click": "filesystem",
    "click_found": "filesystem",
    "type": "filesystem",
    "type_text": "filesystem",
    "hotkey": "filesystem",
    "visual_search": "filesystem",
    "capture_clipboard_image": "filesystem",
    "list_recent_messages": "email",
    "read_message": "email",
    "create_draft": "email",
    "read_cell": "spreadsheet",
    "set_cell": "spreadsheet",
    "load_csv": "spreadsheet",
    "save_csv": "spreadsheet",
    "repo_search": "repo",
}

_DOMAIN_FIRST_ALLOWED = {
    "email": {"email"},
    "spreadsheet": {"spreadsheet", "filesystem"},
    "filesystem": {"filesystem"},
    "calendar": {"calendar"},
    "code_repo": {"repo", "shell"},
    "browser_research": {"browser"},
    "browser_transaction": {"browser"},
    "presentation": {"presentation", "document", "filesystem"},
    "document": {"document", "filesystem"},
    "data_analysis": {"data_analysis", "spreadsheet"},
}


def _domain_from_result(result: Dict[str, Any], instruction: str) -> Tuple[str, List[str]]:
    mode = str(result.get("mode", "")).lower()
    plan_domain = str((result.get("plan", {}) or {}).get("domain", "")).lower()
    low = instruction.lower()
    if plan_domain == "email_triage":
        return "email", ["spreadsheet", "document"]
    if plan_domain in {"job_market", "study_pack", "competitor_analysis", "web_research"}:
        return "browser_research", ["document", "data_analysis"]
    if mode == "autonomous_plan_execute":
        return "browser_research", ["document", "data_analysis"]
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
    if any(k in low for k in ["report", "summary", "brief"]) or any("report" in k.lower() or "summary" in k.lower() for k in artifacts.keys()):
        out.append({"type": "report", "description": "Written report/summary output", "required_fields": []})
    if any(k in low for k in ["powerpoint", "ppt", "slides"]) or any(".pptx" in str(v).lower() for v in artifacts.values()):
        out.append({"type": "presentation", "description": "Presentation output", "required_fields": []})
    if not out:
        out.append({"type": "analysis", "description": "Analysis output", "required_fields": []})
    return out


def _extract_time_constraints(instruction: str) -> Dict[str, Any]:
    m = re.search(r"last\s+([0-9]+)\s*(hours|days|weeks)", instruction, flags=re.IGNORECASE)
    if m:
        return {"relative_window": f"last {m.group(1)} {m.group(2)}", "timezone": "local"}
    return {}


def _stringify_target(step: Dict[str, Any]) -> str:
    raw_target = step.get("target")
    if isinstance(raw_target, dict):
        if raw_target.get("url"):
            return f"url:{raw_target['url']}"
        if raw_target.get("path"):
            return f"path:{raw_target['path']}"
        if raw_target.get("query"):
            return f"query:{raw_target['query']}"
        if raw_target.get("id"):
            return f"id:{raw_target['id']}"
        if raw_target.get("type"):
            return f"type:{raw_target['type']}"
    if isinstance(raw_target, str) and raw_target.strip():
        return raw_target.strip()

    selector = step.get("selector")
    if isinstance(selector, dict):
        value = str(selector.get("value", "")).strip()
        if value:
            return f"selector:{value}"
    app = str(step.get("app", "")).strip()
    if app:
        return f"app:{app}"
    name = str(step.get("name", "")).strip()
    if name:
        return f"name:{name}"
    text = str(step.get("text", "")).strip()
    if text:
        return f"text:{text[:120]}"
    source = str(step.get("source", "")).strip()
    if source:
        return f"source:{source}"
    output_path = str(step.get("output_path", "")).strip()
    if output_path:
        return f"path:{output_path}"
    return ""


def _looks_concrete_target(target: str) -> bool:
    t = target.strip()
    if not t:
        return False
    if t.lower() in _INVALID_TARGETS:
        return False
    if t.startswith("url:") and len(t) > 6:
        return True
    if t.startswith("path:") and len(t) > 7:
        return True
    if t.startswith("query:") and len(t) > 8:
        return True
    if t.startswith("id:") and len(t) > 5:
        return True
    if t.startswith("type:") and len(t) > 7:
        return True
    if t.startswith("selector:") and len(t) > 11:
        return True
    if t.startswith("app:") and len(t) > 4:
        return True
    if t.startswith("name:") and len(t) > 5:
        return True
    if t.startswith("text:") and len(t) > 8:
        return True
    if ":\\" in t or t.startswith("/") or "@" in t or "://" in t:
        return True
    return False


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
        tgt = _stringify_target(s)
        if not tgt:
            continue
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
    mapped = _ACTION_FAMILY_MAP.get(action)
    if mapped:
        return mapped
    if primary_domain in {"email", "calendar", "filesystem", "spreadsheet", "document", "presentation", "data_analysis"}:
        if primary_domain == "data_analysis":
            return "data_analysis"
        return primary_domain
    return "browser"


def _expected_output_for_action(action: str, target: str) -> str:
    a = action.lower()
    if a in {"web_search", "research"}:
        return f"Search results collected for {target}"
    if a in {"open_result", "navigate_url", "present"}:
        return f"Opened target {target}"
    if a in {"extract", "extract_field", "read_cell"}:
        return f"Structured data extracted from {target}"
    if a in {"analyze"}:
        return "Ranked analysis dataset generated"
    if a in {"produce"}:
        return "Requested artifacts created in output folder"
    if a in {"open_app"}:
        return f"Application launched for {target}"
    if a in {"click", "click_found", "focus_window"}:
        return f"UI action completed on {target}"
    if a in {"type", "type_text", "hotkey", "set_cell", "paste"}:
        return f"Input applied to {target}"
    return f"Action {action} completed on {target}"


def _verification_check_for_action(action: str, target: str) -> str:
    return f"Verify {action} produced expected output on {target} with concrete evidence."


def build_plan(
    *,
    task_id: str,
    envelope: Dict[str, Any],
    plan_steps: List[Dict[str, Any]],
) -> Dict[str, Any]:
    items: List[Dict[str, Any]] = []
    for i, s in enumerate(plan_steps):
        action = str(s.get("action", s.get("kind", "step")))
        target = _stringify_target(s)
        destructive = any(k in action.lower() for k in ["delete", "remove", "send", "submit", "payment", "transfer"])
        items.append(
            {
                "index": i,
                "action": action,
                "tool_family": _tool_family_for_step(str(envelope.get("primary_domain", "browser_research")), s),
                "target": target,
                "reason": f"Execute {action} toward user goal",
                "expected_output": _expected_output_for_action(action, target or "<missing-target>"),
                "risk": "high" if destructive else "low",
                "undo_strategy": "Revert via generated undo_plan/manual rollback.",
                "verification_check": _verification_check_for_action(action, target or "<missing-target>"),
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


def _domain_expected_first_families(primary_domain: str) -> Set[str]:
    return _DOMAIN_FIRST_ALLOWED.get(primary_domain, {"browser"})


def _action_expected_family(action: str) -> str:
    return _ACTION_FAMILY_MAP.get(action.lower(), "")


def _is_generic_expected_output(text: str) -> bool:
    low = text.strip().lower()
    if low in _GENERIC_EXPECTED_OUTPUT_PHRASES:
        return True
    return "step executed" in low or "recorded" in low


def validate_plan(*, envelope: Dict[str, Any], steps: List[Dict[str, Any]]) -> Tuple[str, List[str]]:
    errors: List[str] = []
    if not steps:
        errors.append("plan has no steps")
        return "invalid", errors

    primary_domain = str(envelope.get("primary_domain", ""))
    first_family = str(steps[0].get("tool_family", ""))
    allowed_first = _domain_expected_first_families(primary_domain)
    if first_family not in allowed_first:
        errors.append(f"domain lock violation: first tool family '{first_family}' not allowed for domain '{primary_domain}'")

    for s in steps:
        idx = s.get("index", "?")
        action = str(s.get("action", "")).strip()
        target = str(s.get("target", "")).strip()
        tool_family = str(s.get("tool_family", "")).strip()
        expected_output = str(s.get("expected_output", "")).strip()
        verification_check = str(s.get("verification_check", "")).strip()

        if not action:
            errors.append(f"step {idx}: missing action")
        if not _looks_concrete_target(target):
            errors.append(f"step {idx}: invalid or placeholder target '{target or '<empty>'}'")
        if _is_generic_expected_output(expected_output):
            errors.append(f"step {idx}: expected_output is generic and non-falsifiable")
        if not verification_check:
            errors.append(f"step {idx}: missing verification_check")

        expected_family = _action_expected_family(action)
        if expected_family and tool_family and tool_family != expected_family:
            errors.append(
                f"step {idx}: action/tool mismatch action='{action}' expected='{expected_family}' actual='{tool_family}'"
            )

    if not envelope.get("requested_outputs"):
        errors.append("requested outputs missing")

    return ("invalid", errors) if errors else ("valid", [])


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


def _trace_evidence_from_event(t: Dict[str, Any]) -> List[str]:
    evidence: List[str] = []
    for key in ["launched", "opened_url", "url", "path", "window", "selector", "message", "artifact", "result_count"]:
        value = t.get(key)
        if isinstance(value, (str, int, float)) and str(value).strip():
            evidence.append(f"{key}={value}")
    if not evidence:
        evidence.append(f"trace_event={json.dumps({k: t[k] for k in sorted(t.keys()) if k in {'step','action','ok'}}, sort_keys=True)}")
    return evidence


def _step_relevance(primary_domain: str, secondary_domains: List[str], tool_family: str, action: str, target: str) -> bool:
    if str(action).lower() in {"present", "open_result", "navigate_url"} and (
        str(target).startswith("id:artifact:") or str(target).startswith("url:")
    ):
        return True
    primary_allowed = _domain_expected_first_families(primary_domain)
    secondary_allowed: Set[str] = set()
    for dom in secondary_domains:
        secondary_allowed.update(_domain_expected_first_families(str(dom)))
    if primary_domain in {"email", "spreadsheet", "filesystem", "calendar", "code_repo"}:
        return tool_family in (primary_allowed.union(secondary_allowed))
    return True


def build_execution_trace(
    *,
    task_id: str,
    plan: Dict[str, Any],
    result: Dict[str, Any],
) -> Dict[str, Any]:
    trace_raw = result.get("trace", []) or []
    plan_steps = plan.get("steps", []) or []
    primary_domain = str(plan.get("primary_domain", "browser_research"))
    secondary_domains = [str(x) for x in (plan.get("secondary_domains", []) or [])]
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

    rows: List[Dict[str, Any]] = []
    if trace_raw:
        for t in trace_raw:
            i = int(t.get("step", 0) or 0)
            p = plan_steps[i] if 0 <= i < len(plan_steps) else {}
            tool_family = str(p.get("tool_family", "browser"))
            target = str(p.get("target", ""))
            action = str(t.get("action", p.get("action", "step")))
            evidence = _trace_evidence_from_event(t)
            ok = bool(t.get("ok", False))
            relevance_ok = _step_relevance(primary_domain, secondary_domains, tool_family, action, target)
            status = "success" if ok and bool(evidence) and relevance_ok else ("failed" if not ok else "blocked")
            rows.append(
                {
                    "step_index": i,
                    "action": action,
                    "tool_family": tool_family,
                    "target": target,
                    "expected_output": str(p.get("expected_output", "")),
                    "actual_output": json.dumps(t, sort_keys=True)[:1000],
                    "status": status,
                    "relevance_check_passed": relevance_ok,
                    "evidence": evidence,
                    "recovery_attempted": False,
                }
            )
    else:
        result_count = int(result.get("results_count", 0) or 0)
        opened_url = str(result.get("opened_url", "") or "")
        for p in plan_steps:
            action = str(p.get("action", "step"))
            tool_family = str(p.get("tool_family", "browser"))
            target = str(p.get("target", ""))
            relevance_ok = _step_relevance(primary_domain, secondary_domains, tool_family, action, target)
            evidence: List[str] = []
            a = action.lower()
            if a in {"research", "web_search"} and result_count > 0:
                evidence.append(f"results_count={result_count}")
            if a in {"produce"} and created:
                evidence.extend([f"artifact={x['location']}" for x in created[:5]])
            if a in {"present", "open_result", "navigate_url"} and opened_url:
                evidence.append(f"opened_url={opened_url}")
            status = "success" if bool(evidence) and bool(result.get("ok", False)) and relevance_ok else "failed"
            rows.append(
                {
                    "step_index": int(p.get("index", 0)),
                    "action": action,
                    "tool_family": tool_family,
                    "target": target,
                    "expected_output": str(p.get("expected_output", "")),
                    "actual_output": "No step-level trace emitted; evaluated against concrete artifacts/results.",
                    "status": status,
                    "relevance_check_passed": relevance_ok,
                    "evidence": evidence,
                    "recovery_attempted": False,
                }
            )

    errors = []
    if plan.get("validation_status") != "valid":
        errors.append({"code": "verification_failed", "message": "plan validation failed", "step_index": 0})
    if any(r.get("status") in {"failed", "blocked"} for r in rows):
        errors.append({"code": "verification_failed", "message": "one or more steps failed verification", "step_index": 0})
    if not result.get("ok", False):
        err_code = str(result.get("error_code", "verification_failed") or "verification_failed")
        allowed = {
            "credential_missing",
            "permission_denied",
            "file_not_found",
            "target_ambiguous",
            "tool_mismatch",
            "irrelevant_result",
            "artifact_creation_failed",
            "verification_failed",
            "unsafe_action_requires_confirmation",
            "unknown",
        }
        if err_code not in allowed:
            err_code = "verification_failed"
        errors.append({"code": err_code, "message": str(result.get("error", "execution failed")), "step_index": 0})
    if any(not bool(r.get("relevance_check_passed", True)) for r in rows):
        errors.append({"code": "irrelevant_result", "message": "irrelevant detour detected", "step_index": 0})

    return {
        "task_id": task_id,
        "plan_validation_result": plan.get("validation_status", "invalid"),
        "step_results": rows,
        "created_artifacts": created,
        "execution_errors": errors,
    }


def _requested_output_types(envelope: Dict[str, Any]) -> Set[str]:
    return {str(x.get("type", "")).strip() for x in envelope.get("requested_outputs", []) if str(x.get("type", "")).strip()}


def _created_output_types(trace: Dict[str, Any]) -> Set[str]:
    mapped: Set[str] = set()
    for x in trace.get("created_artifacts", []) or []:
        t = str(x.get("artifact_type", "")).strip()
        if not t:
            continue
        if t == "file":
            mapped.add("other")
        else:
            mapped.add(t)
    return mapped


def build_verification_report(
    *,
    task_id: str,
    envelope: Dict[str, Any],
    plan: Dict[str, Any],
    trace: Dict[str, Any],
    result: Dict[str, Any],
) -> Dict[str, Any]:
    step_results = trace.get("step_results", []) or []
    created_artifacts = trace.get("created_artifacts", []) or []
    requested_types = _requested_output_types(envelope)
    created_types = _created_output_types(trace)

    used_expected_tool_family = bool(plan.get("validation_status") == "valid")
    targets_match_request = bool(plan.get("validation_status") == "valid") and all(
        _looks_concrete_target(str(x.get("target", ""))) for x in plan.get("steps", [])
    )
    if requested_types:
        requested_outputs_exist = requested_types.issubset(created_types) or requested_types.issubset(
            {"report" if t == "analysis" else t for t in created_types}
        )
    else:
        requested_outputs_exist = bool(created_artifacts)

    all_steps_success = bool(step_results) and all(str(x.get("status", "")) == "success" for x in step_results)
    artifact_content_matches_goal = requested_outputs_exist and all_steps_success
    no_unresolved_execution_errors = not (trace.get("execution_errors") or [])
    no_irrelevant_detours = all(bool(x.get("relevance_check_passed", False)) for x in step_results)
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
        {
            "name": "used_expected_tool_family",
            "pass": used_expected_tool_family,
            "evidence": [f"plan.validation_status={plan.get('validation_status', 'invalid')}"],
        },
        {
            "name": "targets_match_request",
            "pass": targets_match_request,
            "evidence": [f"validated_targets={sum(1 for s in plan.get('steps', []) if _looks_concrete_target(str(s.get('target',''))))}"],
        },
        {
            "name": "requested_outputs_exist",
            "pass": requested_outputs_exist,
            "evidence": [f"requested={sorted(requested_types)}", f"created={sorted(created_types)}"],
        },
        {
            "name": "artifact_content_matches_goal",
            "pass": artifact_content_matches_goal,
            "evidence": [f"all_steps_success={all_steps_success}"],
        },
        {
            "name": "no_unresolved_execution_errors",
            "pass": no_unresolved_execution_errors,
            "evidence": [f"error_count={len(trace.get('execution_errors', []) or [])}"],
        },
        {
            "name": "no_irrelevant_detours",
            "pass": no_irrelevant_detours,
            "evidence": [f"step_count={len(step_results)}"],
        },
        {
            "name": "user_goal_satisfied",
            "pass": user_goal_satisfied,
            "evidence": [f"result_ok={bool(result.get('ok', False))}"],
        },
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

    if verification.get("final_verification") == "passed":
        status = "completed"
    elif result.get("requires_confirmation"):
        status = "awaiting_confirmation"
    elif outputs:
        status = "partially_completed"
    elif trace.get("execution_errors"):
        blocked_codes = {"credential_missing", "permission_denied", "target_ambiguous"}
        err_codes = {str(e.get("code", "")) for e in trace.get("execution_errors", [])}
        status = "blocked" if err_codes.intersection(blocked_codes) else "failed"
    else:
        status = "failed"

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


def _build_anti_drift_diagnostics(plan: Dict[str, Any], trace: Dict[str, Any]) -> Dict[str, Any]:
    per_step: Dict[int, Dict[str, Any]] = {}
    steps = plan.get("steps", []) or []
    for step in steps:
        idx = int(step.get("index", 0))
        per_step[idx] = {
            "step_index": idx,
            "action": str(step.get("action", "")),
            "target": str(step.get("target", "")),
            "failed_rules": [],
            "messages": [],
        }

    plan_errors = [str(x) for x in plan.get("validation_errors", []) or []]
    for err in plan_errors:
        low = err.lower()
        step_idx = 0
        m = re.search(r"step\s+([0-9]+):", err, flags=re.IGNORECASE)
        if m:
            step_idx = int(m.group(1))
        if step_idx not in per_step:
            per_step[step_idx] = {
                "step_index": step_idx,
                "action": "",
                "target": "",
                "failed_rules": [],
                "messages": [],
            }
        if "domain lock violation" in low:
            per_step[step_idx]["failed_rules"].append("domain_lock")
        elif "invalid or placeholder target" in low:
            per_step[step_idx]["failed_rules"].append("target_validity")
        elif "action/tool mismatch" in low:
            per_step[step_idx]["failed_rules"].append("action_tool_mismatch")
        else:
            per_step[step_idx]["failed_rules"].append("plan_validation")
        per_step[step_idx]["messages"].append(err)

    for row in trace.get("step_results", []) or []:
        idx = int(row.get("step_index", 0))
        if idx not in per_step:
            per_step[idx] = {
                "step_index": idx,
                "action": str(row.get("action", "")),
                "target": str(row.get("target", "")),
                "failed_rules": [],
                "messages": [],
            }
        evidence = row.get("evidence", []) or []
        relevance_ok = bool(row.get("relevance_check_passed", False))
        status = str(row.get("status", ""))
        if (status in {"failed", "blocked"}) and not evidence:
            per_step[idx]["failed_rules"].append("evidence_gap")
            per_step[idx]["messages"].append("Step lacks concrete evidence for expected output.")
        if not relevance_ok:
            per_step[idx]["failed_rules"].append("domain_lock")
            per_step[idx]["messages"].append("Step violated relevance/domain constraints.")

    step_rules: List[Dict[str, Any]] = []
    for idx in sorted(per_step.keys()):
        item = per_step[idx]
        dedup_rules = sorted(set(item.get("failed_rules", [])))
        dedup_messages = []
        for msg in item.get("messages", []):
            if msg not in dedup_messages:
                dedup_messages.append(msg)
        item["failed_rules"] = dedup_rules
        item["messages"] = dedup_messages
        step_rules.append(item)

    failed_steps = [x for x in step_rules if x.get("failed_rules")]
    return {
        "has_failures": bool(failed_steps),
        "total_steps": len(step_rules),
        "failed_steps": len(failed_steps),
        "step_rules": step_rules,
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
    out["anti_drift"] = _build_anti_drift_diagnostics(plan=plan, trace=trace)
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

    if plan.get("validation_status") != "valid":
        out["ok"] = False
        out["error"] = "plan_validation_failed"

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
