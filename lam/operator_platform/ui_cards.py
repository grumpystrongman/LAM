from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List


def _group_runtime_events(runtime_events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []
    for item in runtime_events:
        if not isinstance(item, dict):
            continue
        node_id = str(item.get("node_id", "") or "")
        capability = str(item.get("capability", "") or "")
        key = node_id or capability or "graph"
        if key not in grouped:
            grouped[key] = {
                "node_id": node_id,
                "capability": capability,
                "status": "",
                "events": [],
                "critics": {},
            }
            order.append(key)
        bucket = grouped[key]
        event_payload = {
            "event": str(item.get("event", "")),
            "critic": str(item.get("critic", "")),
            "status": str(item.get("status", "")),
            "ts": item.get("ts", ""),
        }
        bucket["events"].append(event_payload)
        if event_payload["status"]:
            bucket["status"] = event_payload["status"]
        if event_payload["critic"]:
            critic_bucket = bucket["critics"].setdefault(event_payload["critic"], [])
            critic_bucket.append(event_payload)
    return [
        {
            "node_id": grouped[key]["node_id"],
            "capability": grouped[key]["capability"],
            "status": grouped[key]["status"],
            "events": list(grouped[key]["events"][-8:]),
            "critics": [
                {"critic": critic_name, "events": list(events[-6:])}
                for critic_name, events in grouped[key]["critics"].items()
            ],
        }
        for key in order[-10:]
    ]


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
    runtime_events = result.get("runtime_events", []) if isinstance(result.get("runtime_events"), list) else []
    validation_results = result.get("validation_results", {}) if isinstance(result.get("validation_results"), dict) else {}
    final_output_gate = result.get("final_output_gate", {}) if isinstance(result.get("final_output_gate"), dict) else {}
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
                    "validation_history": [],
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
        "runtime_timeline": {
            "title": "Runtime Timeline",
            "compact": True,
            "groups": _group_runtime_events(runtime_events),
            "items": [
                {
                    "event": str(item.get("event", "")),
                    "node_id": str(item.get("node_id", "")),
                    "capability": str(item.get("capability", "")),
                    "critic": str(item.get("critic", "")),
                    "status": str(item.get("status", "")),
                    "ts": item.get("ts", ""),
                }
                for item in runtime_events[-20:]
                if isinstance(item, dict)
            ],
        },
        "memory_context": {
            "title": "Memory Context",
            "compact": True,
            "used": list(memory_context.get("used", []) or []),
            "rejected": list(memory_context.get("rejected", []) or []),
            "project_preferences": list(memory_context.get("project_preferences", []) or []),
            "retrieval_confidence": float(memory_context.get("retrieval_confidence", 0.0) or 0.0),
        },
        "validation": {
            "title": "Validation",
            "compact": True,
            "final_output_gate": dict(final_output_gate),
            "items": [
                {
                    "name": "Geography validation",
                    "passed": bool((validation_results.get("geography", {}) or {}).get("passed", False)),
                    "severity": str((validation_results.get("geography", {}) or {}).get("severity", "")),
                    "issue_count": int((validation_results.get("geography", {}) or {}).get("issue_count", 0) or 0),
                    "repair_attempted": bool((validation_results.get("geography", {}) or {}).get("repair_attempted", False)),
                    "final_status": str((validation_results.get("geography", {}) or {}).get("final_status", "")),
                },
                {
                    "name": "Service scope validation",
                    "passed": bool((validation_results.get("service_scope", {}) or {}).get("passed", False)),
                    "severity": str((validation_results.get("service_scope", {}) or {}).get("severity", "")),
                    "issue_count": int((validation_results.get("service_scope", {}) or {}).get("issue_count", 0) or 0),
                    "repair_attempted": bool((validation_results.get("service_scope", {}) or {}).get("repair_attempted", False)),
                    "final_status": str((validation_results.get("service_scope", {}) or {}).get("final_status", "")),
                },
                {
                    "name": "Source relevance validation",
                    "passed": bool((validation_results.get("source_relevance", {}) or {}).get("passed", False)),
                    "severity": str((validation_results.get("source_relevance", {}) or {}).get("severity", "")),
                    "issue_count": int((validation_results.get("source_relevance", {}) or {}).get("issue_count", 0) or 0),
                    "repair_attempted": bool((validation_results.get("source_relevance", {}) or {}).get("repair_attempted", False)),
                    "final_status": str((validation_results.get("source_relevance", {}) or {}).get("final_status", "")),
                },
                {
                    "name": "Artifact contamination validation",
                    "passed": bool((validation_results.get("artifact_contamination", {}) or {}).get("passed", False)),
                    "severity": str((validation_results.get("artifact_contamination", {}) or {}).get("severity", "")),
                    "issue_count": int((validation_results.get("artifact_contamination", {}) or {}).get("issue_count", 0) or 0),
                    "repair_attempted": bool((validation_results.get("artifact_contamination", {}) or {}).get("repair_attempted", False)),
                    "final_status": str((validation_results.get("artifact_contamination", {}) or {}).get("final_status", "")),
                },
                {
                    "name": "Final output gate",
                    "passed": bool(final_output_gate.get("passed", False)),
                    "severity": str(final_output_gate.get("severity", "")),
                    "issue_count": int(final_output_gate.get("issue_count", 0) or 0),
                    "repair_attempted": bool(result.get("repair_state")),
                    "final_status": "passed" if bool(final_output_gate.get("passed", False)) else "blocked",
                },
            ],
            "blocking_failures": list(final_output_gate.get("blocking_failures", []) or []),
            "required_repairs": list(final_output_gate.get("required_repairs", []) or []),
        },
    }
