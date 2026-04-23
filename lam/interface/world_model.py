from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List

from lam.interface.session_manager import SessionManager


@dataclass(slots=True)
class LiveWorldModel:
    ts: float = field(default_factory=time.time)
    scope: str = "run"
    domain: str = "general"
    mode: str = "unknown"
    instruction: str = ""
    signals: Dict[str, Any] = field(default_factory=dict)
    candidate_targets: List[str] = field(default_factory=list)
    rejected_targets: List[str] = field(default_factory=list)
    created_outputs: List[str] = field(default_factory=list)
    what_i_noticed: List[str] = field(default_factory=list)
    narration: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ts": self.ts,
            "scope": self.scope,
            "domain": self.domain,
            "mode": self.mode,
            "instruction": self.instruction,
            "signals": dict(self.signals),
            "candidate_targets": [str(x) for x in self.candidate_targets[:20]],
            "rejected_targets": [str(x) for x in self.rejected_targets[:20]],
            "created_outputs": [str(x) for x in self.created_outputs[:40]],
            "what_i_noticed": [str(x) for x in self.what_i_noticed[:12]],
            "narration": [str(x) for x in self.narration[:10]],
        }


def _summarize_source_status(source_status: Dict[str, Any]) -> str:
    if not source_status:
        return ""
    key = next(iter(source_status.keys()))
    return f"{key}={source_status.get(key)}"


def _quality_signal(summary: Dict[str, Any]) -> str:
    judgment = summary.get("judgment", {}) if isinstance(summary.get("judgment"), dict) else {}
    if not judgment:
        return ""
    score = float(judgment.get("score", 0.0) or 0.0)
    return f"{score:.2f}"


def build_run_world_model(
    *,
    instruction: str,
    mode: str,
    domain: str,
    playbook: Dict[str, Any],
    opened_url: str,
    paused_for_credentials: bool,
    pause_reason: str,
    auth_session_id: str,
    artifacts: Dict[str, Any],
    summary: Dict[str, Any],
    source_status: Dict[str, Any],
    decision_log: List[str],
    results_count: int,
    playbook_validation: Dict[str, Any] | None = None,
    playbook_graph_validation: Dict[str, Any] | None = None,
    playbook_step_obligations: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    artifact_paths = {k: v for k, v in artifacts.items() if isinstance(v, str)}
    summary_safe = dict(summary)
    source_safe = dict(source_status)
    pb_validate = dict(playbook_validation or {})
    pb_graph = dict(playbook_graph_validation or {})
    pb_obligations = dict(playbook_step_obligations or {})
    session_snapshot = SessionManager().snapshot()

    noticed: List[str] = []
    if opened_url:
        noticed.append(f"Working target detected: {opened_url}")
    if paused_for_credentials:
        noticed.append("Task paused at auth checkpoint.")
    if auth_session_id:
        noticed.append(f"Auth session in focus: {auth_session_id}")
    if artifact_paths:
        noticed.append(f"Created outputs: {', '.join(sorted(artifact_paths.keys())[:4])}")
    source_line = _summarize_source_status(source_safe)
    if source_line:
        noticed.append(f"Primary source status: {source_line}")
    quality = _quality_signal(summary_safe)
    if quality:
        noticed.append(f"Quality critic score: {quality}")
    reuse_mode = str(summary_safe.get("artifact_reuse_mode", "")).strip()
    if reuse_mode:
        reused = summary_safe.get("reused_existing_outputs")
        reused_label = "reused" if bool(reused) else "regenerated"
        noticed.append(f"Freshness policy: {reuse_mode} ({reused_label})")
    if pb_graph:
        noticed.append(f"Playbook graph validation: {pb_graph.get('status', 'unknown')}")

    candidate_targets = [x for x in [opened_url, str(session_snapshot.get("latest_tab", {}).get("url", ""))] if str(x).strip()]
    rejected_targets: List[str] = []
    for line in [str(x) for x in (decision_log or [])][-20:]:
        if "skipped" in line.lower() or "blocked" in line.lower():
            rejected_targets.append(line)

    model = LiveWorldModel(
        scope="run",
        domain=domain,
        mode=mode,
        instruction=instruction,
        signals={
            "paused_for_credentials": bool(paused_for_credentials),
            "auth_session_id": auth_session_id,
            "results_count": int(results_count or 0),
            "artifacts_count": len(artifact_paths),
            "playbook_id": str(playbook.get("id", "")),
            "playbook_validation_status": pb_validate.get("status", ""),
            "playbook_graph_validation_status": pb_graph.get("status", ""),
            "playbook_obligations_status": pb_obligations.get("status", ""),
            "session_tabs_count": int(session_snapshot.get("tabs_count", 0) or 0),
            "session_auth_attempts_count": int(session_snapshot.get("auth_attempts_count", 0) or 0),
            "artifact_reuse_mode": reuse_mode,
            "reused_existing_outputs": bool(summary_safe.get("reused_existing_outputs", False)),
        },
        candidate_targets=candidate_targets,
        rejected_targets=rejected_targets,
        created_outputs=sorted(artifact_paths.values()),
        what_i_noticed=noticed,
        narration=[
            f"Using playbook {str(playbook.get('id', 'general'))}.",
            "Credential checkpoint is active." if paused_for_credentials else "No credential checkpoint active.",
            f"Observed {int(results_count or 0)} result(s) and {len(artifact_paths)} artifact(s).",
        ],
    )

    out = model.to_dict()
    out.update(
        {
            "playbook_id": str(playbook.get("id", "")),
            "state": {
                "paused_for_credentials": paused_for_credentials,
                "pause_reason": pause_reason,
                "auth_session_id": auth_session_id,
            },
            "environment": {
                "opened_url": opened_url,
                "source_status": source_safe,
                "session": session_snapshot,
            },
            "execution": {
                "results_count": int(results_count or 0),
                "artifacts_count": len(artifact_paths),
                "artifact_keys": sorted(artifact_paths.keys()),
            },
            "summary": summary_safe,
            "decision_log_tail": [str(x) for x in (decision_log or [])][-8:],
            "playbook_validation": pb_validate,
            "playbook_graph_validation": pb_graph,
            "playbook_step_obligations": pb_obligations,
        }
    )
    return out


def build_ui_world_model(
    *,
    control_granted: bool,
    paused_for_credentials: bool,
    pause_reason: str,
    pending_auth_url: str,
    pending_auth_session_id: str,
    current_task: Dict[str, Any],
    history: List[Dict[str, Any]],
    preflight: Dict[str, Any],
) -> Dict[str, Any]:
    recent_runs = [x for x in (history or []) if isinstance(x, dict)][-5:]
    last_modes = [str(x.get("mode", "unknown")) for x in recent_runs]
    recent_artifacts: List[Dict[str, str]] = []
    for run in reversed(recent_runs):
        artifacts = run.get("artifacts", {})
        if not isinstance(artifacts, dict):
            continue
        for key, value in artifacts.items():
            if isinstance(value, str) and value.strip():
                recent_artifacts.append({"name": str(key), "path": value})
    recent_artifacts = recent_artifacts[:8]

    task_state = {
        "id": str(current_task.get("id", "")),
        "status": str(current_task.get("status", "")),
        "progress": int(current_task.get("progress", 0) or 0),
        "message": str(current_task.get("message", "")),
    }
    session_snapshot = SessionManager().snapshot()
    narration: List[str] = []
    noticed: List[str] = []
    if paused_for_credentials:
        narration.append("Paused on auth checkpoint; waiting for user action.")
        if pending_auth_url:
            noticed.append(f"Auth target is ready: {pending_auth_url}")
    if control_granted:
        narration.append("Control granted; operator can execute actions.")
    else:
        narration.append("Control not granted; execution is blocked.")
    if preflight.get("required", False):
        if preflight.get("green", False):
            narration.append("Preflight gate is GREEN.")
        else:
            narration.append(f"Preflight gate is BLOCKED: {preflight.get('reason', '')}")
    if task_state["id"]:
        narration.append(f"Active task status: {task_state['status']} ({task_state['progress']}%).")
    if session_snapshot.get("tabs_count", 0):
        noticed.append(f"Session memory has {session_snapshot.get('tabs_count', 0)} tab record(s).")
    if session_snapshot.get("auth_attempts_count", 0):
        noticed.append(f"Session memory has {session_snapshot.get('auth_attempts_count', 0)} auth attempt(s).")
    if recent_artifacts:
        first = recent_artifacts[0]
        noticed.append(f"Latest artifact detected: {first.get('name')} -> {first.get('path')}")

    candidate_targets = [x for x in [pending_auth_url, str(session_snapshot.get("latest_tab", {}).get("url", ""))] if str(x).strip()]
    created_outputs = [x.get("path", "") for x in recent_artifacts if str(x.get("path", "")).strip()]

    model = LiveWorldModel(
        scope="ui",
        domain=str(current_task.get("mode", "general") or "general"),
        mode=str(current_task.get("status", "idle") or "idle"),
        instruction=str(current_task.get("instruction", "") or ""),
        signals={
            "control_granted": bool(control_granted),
            "paused_for_credentials": bool(paused_for_credentials),
            "preflight_green": bool(preflight.get("green", False)),
            "history_count": len(history or []),
        },
        candidate_targets=candidate_targets,
        created_outputs=created_outputs,
        what_i_noticed=noticed,
        narration=narration,
    )

    out = model.to_dict()
    out.update(
        {
            "workspace": {
                "control_granted": bool(control_granted),
                "paused_for_credentials": bool(paused_for_credentials),
                "pause_reason": pause_reason,
                "pending_auth_url": pending_auth_url,
                "pending_auth_session_id": pending_auth_session_id,
                "preflight": dict(preflight),
            },
            "task": task_state,
            "recent": {
                "history_count": len(history or []),
                "last_modes": last_modes,
                "artifacts": recent_artifacts,
            },
            "narration": narration[:8],
            "what_i_noticed": noticed[:10],
            "environment": {"session": session_snapshot},
        }
    )
    return out
