from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List


def build_platform_cards(result: Dict[str, Any]) -> Dict[str, Any]:
    task_contract = result.get("task_contract", {}) if isinstance(result.get("task_contract"), dict) else {}
    artifacts = result.get("artifacts", {}) if isinstance(result.get("artifacts"), dict) else {}
    critics = (
        (result.get("critics", {}) or {}).get("platform", {})
        if isinstance((result.get("critics", {}) or {}).get("platform", {}), dict)
        else {}
    )
    graph = result.get("capability_execution_graph", {}) if isinstance(result.get("capability_execution_graph"), dict) else {}
    memory_context = result.get("memory_context", {}) if isinstance(result.get("memory_context"), dict) else {}
    manifest = result.get("artifact_manifest", {}) if isinstance(result.get("artifact_manifest"), dict) else {}
    return {
        "task_contract": {
            "title": "Task Contract",
            "compact": True,
            "goal": str(task_contract.get("user_goal", "")),
            "audience": str(task_contract.get("audience", "")),
            "domain": str(task_contract.get("domain", "")),
            "geography": str(task_contract.get("geography", "")),
            "timeframe": str(task_contract.get("timeframe", "")),
            "requested_outputs": list(task_contract.get("requested_outputs", []) or []),
            "constraints": list(task_contract.get("constraints", []) or []),
            "safety_rules": list(task_contract.get("safety_rules", []) or []),
            "invalidation_keys": dict(task_contract.get("invalidation_keys", {}) or {}),
        },
        "artifact_manifest": {
            "title": "Artifact Manifest",
            "compact": True,
            "validation_status": str(manifest.get("validation_status", result.get("verification", {}).get("passed", ""))),
            "created_at": str(manifest.get("created_at", datetime.now().isoformat(timespec="seconds"))),
            "items": list(manifest.get("items", []) or [])
            or [
                {
                    "key": key,
                    "path": value,
                    "status": "ready",
                    "type": "file",
                    "title": key,
                    "evidence_summary": "",
                    "validation_state": "ready",
                }
                for key, value in artifacts.items()
                if isinstance(value, str) and value.strip()
            ],
            "source_data": list(manifest.get("source_data", []) or []),
        },
        "critic_results": {
            "title": "Critic Results",
            "compact": True,
            "items": [
                {
                    "critic": name,
                    "passed": bool((payload or {}).get("passed", False)),
                    "score": float((payload or {}).get("score", 0.0) or 0.0),
                    "reason": str((payload or {}).get("reason", "")),
                    "required_fix": str((payload or {}).get("required_fix", "")),
                }
                for name, payload in critics.items()
                if isinstance(payload, dict)
            ],
            "revisions": list(result.get("revisions_performed", []) or []),
        },
        "execution_graph": {
            "title": "Execution Graph",
            "compact": True,
            "status": str(graph.get("status", "")),
            "current_node": next(
                (str(node.get("capability", "")) for node in (graph.get("nodes", []) or []) if str(node.get("status", "")) == "running"),
                "",
            ),
            "nodes": [
                {
                    "node_id": str(node.get("node_id", "")),
                    "capability": str(node.get("capability", "")),
                    "status": str(node.get("status", "")),
                    "attempts": int(node.get("attempts", 0) or 0),
                }
                for node in (graph.get("nodes", []) or [])
                if isinstance(node, dict)
            ],
            "events_count": len(graph.get("events", []) or []),
        },
        "memory_context": {
            "title": "Memory Context",
            "compact": True,
            "used": list(memory_context.get("used", []) or []),
            "rejected": list(memory_context.get("rejected", []) or []),
            "project_preferences": list(memory_context.get("project_preferences", []) or []),
            "retrieval_confidence": float(memory_context.get("retrieval_confidence", 0.0) or 0.0),
        },
    }
