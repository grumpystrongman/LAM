from __future__ import annotations

from typing import Any, Dict, List


_PLAYBOOKS: Dict[str, Dict[str, Any]] = {
    "email_triage": {
        "id": "email-triage-v1",
        "name": "Email Triage Playbook",
        "primary_tool_family": "email",
        "inspect_first": ["open_tabs", "authenticated_account", "inbox_visibility"],
        "fallback_tree": ["reuse_session", "vault_auth", "imap_api_fallback"],
        "disallowed_first_moves": ["generic_web_search"],
    },
    "competitor_analysis": {
        "id": "competitor-analysis-v1",
        "name": "Competitor Analysis Playbook",
        "primary_tool_family": "browser_research",
        "inspect_first": ["instruction_constraints", "source_quality", "citation_count"],
        "fallback_tree": ["refine_query", "alternative_source_mix", "fail_on_citation_threshold"],
        "disallowed_first_moves": ["single_source_completion"],
    },
    "job_market": {
        "id": "job-market-v1",
        "name": "Job Market Playbook",
        "primary_tool_family": "browser_research",
        "inspect_first": ["location_scope", "salary_fields", "source_coverage"],
        "fallback_tree": ["query_refine", "site_expansion", "dedupe_and_normalize"],
        "disallowed_first_moves": ["one_site_only"],
    },
    "study_pack": {
        "id": "study-pack-v1",
        "name": "Study Pack Playbook",
        "primary_tool_family": "browser_research",
        "inspect_first": ["official_sources", "content_coverage", "artifact_requirements"],
        "fallback_tree": ["query_refine", "source_breadth", "content_validation"],
        "disallowed_first_moves": ["generic_guessing"],
    },
    "payer_pricing_review": {
        "id": "payer-pricing-review-v1",
        "name": "Payer Pricing Review Playbook",
        "primary_tool_family": "payer_rag",
        "inspect_first": ["public_source_scope", "durham_coverage", "artifact_requirements"],
        "fallback_tree": ["provider_source_expansion", "shoppable_service_catalog", "offline_fixture_fallback"],
        "disallowed_first_moves": ["unsupported_fairness_claim"],
    },
    "code_workbench": {
        "id": "code-workbench-v1",
        "name": "Code Workbench Playbook",
        "primary_tool_family": "local_workspace",
        "inspect_first": ["task_scope", "workspace_freshness", "tooling_targets"],
        "fallback_tree": ["fresh_workspace", "editor_launch_fallback", "scaffold_then_smoke_test"],
        "disallowed_first_moves": ["reuse_unrelated_workspace"],
    },
    "web_research": {
        "id": "web-research-v1",
        "name": "Web Research Playbook",
        "primary_tool_family": "browser_research",
        "inspect_first": ["intent_constraints", "candidate_quality", "result_specificity"],
        "fallback_tree": ["refine", "compare", "verify"],
        "disallowed_first_moves": ["first_result_termination"],
    },
    "desktop_sequence": {
        "id": "desktop-sequence-v1",
        "name": "Desktop Sequence Playbook",
        "primary_tool_family": "desktop_uia",
        "inspect_first": ["running_apps", "window_focus", "selector_stability"],
        "fallback_tree": ["uia_selector_retry", "focused_hotkey", "input_fallback_if_allowed"],
        "disallowed_first_moves": ["blind_coordinate_click"],
    },
    "artifact_generation": {
        "id": "artifact-generation-v1",
        "name": "Artifact Generation Playbook",
        "primary_tool_family": "document",
        "inspect_first": ["requested_outputs", "output_paths", "format_support"],
        "fallback_tree": ["format_fallback", "markdown_fallback"],
        "disallowed_first_moves": ["empty_artifact_completion"],
    },
    "general": {
        "id": "general-operator-v1",
        "name": "General Operator Playbook",
        "primary_tool_family": "mixed",
        "inspect_first": ["environment_state", "target_specificity", "risk_gates"],
        "fallback_tree": ["replan", "recover", "block_if_unsafe"],
        "disallowed_first_moves": ["placeholder_targets"],
    },
}

_ALLOWED_STEP_KINDS: Dict[str, List[str]] = {
    "email_triage": ["list_recent_messages", "read_message", "create_draft", "save_csv", "present"],
    "job_market": ["research", "extract", "analyze", "produce", "present"],
    "competitor_analysis": ["research", "extract", "analyze", "produce", "present"],
    "study_pack": ["research", "extract", "analyze", "produce", "present"],
    "payer_pricing_review": ["research", "extract", "analyze", "produce", "present"],
    "code_workbench": ["research", "extract", "analyze", "produce", "present"],
    "web_research": ["research", "extract", "analyze", "produce", "present"],
}

_FIRST_STEP_KIND: Dict[str, List[str]] = {
    "email_triage": ["list_recent_messages"],
    "job_market": ["research"],
    "competitor_analysis": ["research"],
    "study_pack": ["research"],
    "payer_pricing_review": ["research"],
    "code_workbench": ["research"],
    "web_research": ["research"],
}

_TRANSITIONS: Dict[str, List[tuple[str, str]]] = {
    "email_triage": [
        ("list_recent_messages", "read_message"),
        ("read_message", "create_draft"),
        ("create_draft", "save_csv"),
        ("save_csv", "present"),
    ],
    "job_market": [
        ("research", "extract"),
        ("extract", "analyze"),
        ("analyze", "produce"),
        ("produce", "present"),
    ],
    "competitor_analysis": [
        ("research", "extract"),
        ("extract", "analyze"),
        ("analyze", "produce"),
        ("produce", "present"),
    ],
    "study_pack": [
        ("research", "extract"),
        ("extract", "analyze"),
        ("analyze", "produce"),
        ("produce", "present"),
    ],
    "payer_pricing_review": [
        ("research", "extract"),
        ("extract", "analyze"),
        ("analyze", "produce"),
        ("produce", "present"),
    ],
    "code_workbench": [
        ("research", "extract"),
        ("extract", "analyze"),
        ("analyze", "produce"),
        ("produce", "present"),
    ],
    "web_research": [
        ("research", "extract"),
        ("extract", "analyze"),
        ("analyze", "produce"),
        ("produce", "present"),
    ],
}

_STEP_OBLIGATIONS: Dict[str, Dict[str, Dict[str, Any]]] = {
    "email_triage": {
        "list_recent_messages": {"required_source": "gmail_ui"},
        "read_message": {"required_results_min": 1},
        "create_draft": {"no_external_send": True},
        "save_csv": {"required_artifacts_any": ["email_tasks_csv", "task_list_csv"]},
        "present": {"required_opened_url_or_artifact": True},
    },
    "job_market": {
        "research": {"required_results_min": 1},
        "produce": {"required_artifacts_min": 1},
        "present": {"required_opened_url_or_artifact": True},
    },
    "competitor_analysis": {
        "research": {"required_results_min": 1},
        "produce": {"required_artifacts_min": 1},
        "present": {"required_opened_url_or_artifact": True},
    },
    "study_pack": {
        "research": {"required_results_min": 1},
        "produce": {"required_artifacts_min": 1},
        "present": {"required_opened_url_or_artifact": True},
    },
    "payer_pricing_review": {
        "research": {"required_results_min": 1},
        "produce": {"required_artifacts_min": 4},
        "present": {"required_opened_url_or_artifact": True},
    },
    "code_workbench": {
        "produce": {"required_artifacts_min": 4},
        "present": {"required_opened_url_or_artifact": True},
    },
    "web_research": {
        "research": {"required_results_min": 1},
        "produce": {"required_artifacts_min": 1},
        "present": {"required_opened_url_or_artifact": True},
    },
}


def list_playbooks() -> List[Dict[str, Any]]:
    return [dict(v) for v in _PLAYBOOKS.values()]


def select_playbook(domain: str, instruction: str = "") -> Dict[str, Any]:
    _ = instruction
    key = str(domain or "").strip().lower()
    if key not in _PLAYBOOKS:
        key = "general"
    return dict(_PLAYBOOKS[key])


def validate_plan_steps(domain: str, steps: List[Dict[str, Any]]) -> Dict[str, Any]:
    key = str(domain or "").strip().lower()
    allowed = _ALLOWED_STEP_KINDS.get(key, [])
    first_allowed = _FIRST_STEP_KIND.get(key, [])
    errors: List[str] = []
    if not steps:
        errors.append("plan has no steps")
        return {"ok": False, "errors": errors}
    first_kind = str((steps[0] or {}).get("kind", "")).strip().lower()
    if first_allowed and first_kind not in first_allowed:
        errors.append(f"playbook first-step violation: expected one of {first_allowed}, got '{first_kind}'")
    if allowed:
        for idx, step in enumerate(steps):
            kind = str((step or {}).get("kind", "")).strip().lower()
            if kind not in allowed:
                errors.append(f"playbook step violation at index {idx}: kind '{kind}' is not allowed for domain '{key}'")
    return {"ok": len(errors) == 0, "errors": errors}


def validate_transition_graph(domain: str, steps: List[Dict[str, Any]]) -> Dict[str, Any]:
    key = str(domain or "").strip().lower()
    transitions = set(_TRANSITIONS.get(key, []))
    if not steps:
        return {"ok": False, "errors": ["plan has no steps"]}
    errors: List[str] = []
    kinds = [str((s or {}).get("kind", "")).strip().lower() for s in steps]
    for idx in range(len(kinds) - 1):
        edge = (kinds[idx], kinds[idx + 1])
        if transitions and edge not in transitions:
            errors.append(f"playbook transition violation at index {idx}: '{edge[0]}' -> '{edge[1]}'")
    return {"ok": len(errors) == 0, "errors": errors}


def build_step_obligations(domain: str, steps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    key = str(domain or "").strip().lower()
    lookup = _STEP_OBLIGATIONS.get(key, {})
    out: List[Dict[str, Any]] = []
    for idx, step in enumerate(steps):
        kind = str((step or {}).get("kind", "")).strip().lower()
        obligations = dict(lookup.get(kind, {}))
        out.append({"index": idx, "kind": kind, "obligations": obligations})
    return out


def evaluate_step_obligations(
    *,
    domain: str,
    steps: List[Dict[str, Any]],
    obligations: List[Dict[str, Any]],
    result: Dict[str, Any],
) -> Dict[str, Any]:
    _ = (domain, steps)
    errors: List[str] = []
    artifacts = result.get("artifacts", {}) if isinstance(result.get("artifacts"), dict) else {}
    source_status = result.get("source_status", {}) if isinstance(result.get("source_status"), dict) else {}
    results_count = int(result.get("results_count", 0) or 0)
    opened_url = str(result.get("opened_url", "") or "").strip()
    for spec in obligations:
        idx = int(spec.get("index", -1))
        kind = str(spec.get("kind", "")).strip().lower()
        req = spec.get("obligations", {}) if isinstance(spec.get("obligations"), dict) else {}
        required_source = str(req.get("required_source", "")).strip()
        if required_source and required_source not in source_status:
            errors.append(f"obligation failed step {idx} ({kind}): missing source '{required_source}'")
        required_results_min = req.get("required_results_min")
        if isinstance(required_results_min, int) and results_count < required_results_min:
            errors.append(
                f"obligation failed step {idx} ({kind}): results_count {results_count} < {required_results_min}"
            )
        required_artifacts_min = req.get("required_artifacts_min")
        if isinstance(required_artifacts_min, int) and len(artifacts) < required_artifacts_min:
            errors.append(
                f"obligation failed step {idx} ({kind}): artifacts {len(artifacts)} < {required_artifacts_min}"
            )
        required_artifacts_any = req.get("required_artifacts_any", [])
        if isinstance(required_artifacts_any, list) and required_artifacts_any:
            if not any(str(k) in artifacts for k in required_artifacts_any):
                errors.append(
                    f"obligation failed step {idx} ({kind}): none of required artifacts found {required_artifacts_any}"
                )
        if bool(req.get("required_opened_url_or_artifact", False)) and not (opened_url or artifacts):
            errors.append(f"obligation failed step {idx} ({kind}): missing opened_url/artifact evidence")
        if bool(req.get("no_external_send", False)):
            # Guardrail flag for future external send channels.
            if str(result.get("mode", "")).lower() in {"email_send", "message_send"}:
                errors.append(f"obligation failed step {idx} ({kind}): external send not allowed")
    return {"ok": len(errors) == 0, "errors": errors}
