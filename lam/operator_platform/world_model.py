from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List


@dataclass(slots=True)
class WorldModel:
    open_apps: List[str] = field(default_factory=list)
    windows: List[str] = field(default_factory=list)
    browser_tabs: List[str] = field(default_factory=list)
    active_sessions: List[str] = field(default_factory=list)
    files: List[str] = field(default_factory=list)
    folders: List[str] = field(default_factory=list)
    terminal_contexts: List[str] = field(default_factory=list)
    repos: List[str] = field(default_factory=list)
    artifacts: List[str] = field(default_factory=list)
    credentials_availability: Dict[str, Any] = field(default_factory=dict)
    current_run_folder: str = ""
    tried_paths: List[str] = field(default_factory=list)
    failed_paths: List[str] = field(default_factory=list)
    rejected_artifacts: List[str] = field(default_factory=list)
    reusable_artifacts: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class WorldModelBuilder:
    @staticmethod
    def from_run(
        *,
        session_snapshot: Dict[str, Any],
        artifacts: Dict[str, Any],
        task_contract: Dict[str, Any],
        summary: Dict[str, Any],
        opened_url: str,
    ) -> WorldModel:
        artifact_paths = [str(v) for v in artifacts.values() if isinstance(v, str) and str(v).strip()]
        current_run_folder = ""
        if artifact_paths:
            current_run_folder = artifact_paths[0]
        latest_tab = str((session_snapshot or {}).get("latest_tab", {}).get("url", "") or "")
        active_sessions = []
        latest_auth = (session_snapshot or {}).get("latest_auth_attempt", {}) or {}
        if latest_auth:
            active_sessions.append(f"{latest_auth.get('domain','')}:{latest_auth.get('status','')}")
        return WorldModel(
            open_apps=[],
            windows=[],
            browser_tabs=[x for x in [opened_url, latest_tab] if str(x).strip()],
            active_sessions=active_sessions,
            files=[x for x in artifact_paths if "." in x.split("\\")[-1]],
            folders=[x for x in artifact_paths if "." not in x.split("\\")[-1]],
            terminal_contexts=[],
            repos=[],
            artifacts=artifact_paths,
            credentials_availability={"auth_attempts_count": int((session_snapshot or {}).get("auth_attempts_count", 0) or 0)},
            current_run_folder=current_run_folder,
            tried_paths=list(summary.get("candidate_targets", [])) if isinstance(summary.get("candidate_targets"), list) else [],
            failed_paths=list(summary.get("decision_log_tail", [])) if isinstance(summary.get("decision_log_tail"), list) else [],
            rejected_artifacts=list(summary.get("invalidated_artifacts", [])) if isinstance(summary.get("invalidated_artifacts"), list) else [],
            reusable_artifacts=list(summary.get("reusable_artifacts", [])) if isinstance(summary.get("reusable_artifacts"), list) else [],
        )
