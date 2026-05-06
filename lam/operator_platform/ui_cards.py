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
    mission_contract = result.get("mission_contract", {}) if isinstance(result.get("mission_contract"), dict) else {}
    research_strategy = result.get("research_strategy", {}) if isinstance(result.get("research_strategy"), dict) else {}
    evidence_map = result.get("evidence_map", {}) if isinstance(result.get("evidence_map"), dict) else {}
    artifact_plan = result.get("artifact_plan", []) if isinstance(result.get("artifact_plan"), list) else []
    final_package = result.get("final_package", {}) if isinstance(result.get("final_package"), dict) else {}
    output_truth = result.get("output_truth", {}) if isinstance(result.get("output_truth"), dict) else {}
    recovery = result.get("recovery", {}) if isinstance(result.get("recovery"), dict) else {}
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
    learn_mission = result.get("learn_mission", {}) if isinstance(result.get("learn_mission"), dict) else {}
    source_discovery = result.get("source_discovery", {}) if isinstance(result.get("source_discovery"), dict) else {}
    video_analysis = result.get("video_analysis", {}) if isinstance(result.get("video_analysis"), dict) else {}
    topic_model = result.get("topic_model", {}) if isinstance(result.get("topic_model"), dict) else {}
    consensus_workflow = result.get("consensus_workflow", []) if isinstance(result.get("consensus_workflow"), list) else []
    contradictions = result.get("contradictions", []) if isinstance(result.get("contradictions"), list) else []
    learned_skill = result.get("learned_skill", {}) if isinstance(result.get("learned_skill"), dict) else {}
    mastery_guide = result.get("mastery_guide", {}) if isinstance(result.get("mastery_guide"), dict) else {}
    practice_plan = result.get("practice_plan", {}) if isinstance(result.get("practice_plan"), dict) else {}
    topic_critics = result.get("critic_results", {}) if isinstance(result.get("critic_results"), dict) else {}
    cards = {
        "mission_contract": {
            "title": "Mission",
            "compact": True,
            "mission_type": str(mission_contract.get("mission_type", "")),
            "goal": str(mission_contract.get("user_goal", "")),
            "audience": str(mission_contract.get("audience", "")),
            "deliverable_mode": str(mission_contract.get("deliverable_mode", "")),
            "requested_outputs": list(mission_contract.get("requested_outputs", []) or []),
            "quality_bar": str(mission_contract.get("quality_bar", "")),
            "allowed_fallbacks": list(mission_contract.get("allowed_fallbacks", []) or []),
        },
        "research_strategy": {
            "title": "Research Strategy",
            "compact": True,
            "research_questions": list(research_strategy.get("research_questions", []) or []),
            "source_categories": list(research_strategy.get("source_categories", []) or []),
            "minimum_evidence_threshold": dict(research_strategy.get("minimum_evidence_threshold", {}) or {}),
            "search_paths": list(research_strategy.get("search_paths", []) or []),
        },
        "evidence_map": {
            "title": "Evidence Map",
            "compact": True,
            "summary": dict(evidence_map.get("summary", {}) or {}),
            "accepted_sources": [
                item for item in (evidence_map.get("entries", []) or []) if isinstance(item, dict) and bool(item.get("allowed_as_evidence", True))
            ][:8],
            "supported_claims": dict(evidence_map.get("claims", {}) or {}),
            "rejected_sources": [
                item for item in (evidence_map.get("entries", []) or []) if isinstance(item, dict) and not bool(item.get("allowed_as_evidence", True))
            ][:6],
        },
        "artifact_plan": {
            "title": "Artifact Plan",
            "compact": True,
            "items": [
                {
                    "name": str(item.get("name", "")),
                    "artifact_type": str(item.get("artifact_type", "")),
                    "evidence_required": bool(item.get("evidence_required", False)),
                }
                for item in artifact_plan
                if isinstance(item, dict)
            ],
        },
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
        "output_truth": {
            "title": "Output Truth",
            "compact": True,
            "status": str(output_truth.get("status", result.get("mission_status", result.get("completion_status", "")))),
            "summary": str(final_package.get("summary", "")),
            "real_evidence_sources": int(output_truth.get("real_evidence_sources", 0) or 0),
            "attempted_collection": bool(output_truth.get("attempted_collection", False)),
            "reason": str(output_truth.get("reason", "")),
        },
        "recovery": {
            "title": "Recovery",
            "compact": True,
            "status": str(recovery.get("status", result.get("mission_status", ""))),
            "reason": str(recovery.get("reason", "")),
            "strategy": str(recovery.get("strategy", "")),
            "fallback_used": str(recovery.get("fallback_used", "")),
            "revisions": list(result.get("revisions_performed", []) or []),
            "next_steps": list(final_package.get("next_steps", []) or []),
        },
        "final_package": {
            "title": "Final Package",
            "compact": True,
            "status": str(final_package.get("status", "")),
            "summary": str(final_package.get("summary", "")),
            "next_steps": list(final_package.get("next_steps", []) or []),
            "artifacts": [
                {"key": key, "path": value}
                for key, value in artifacts.items()
                if isinstance(value, str) and value.strip()
            ][:10],
        },
    }
    if learn_mission:
        cards.update(
            {
                "learn_mission": {
                    "title": "Learn Mission",
                    "compact": True,
                    "topic": str(learn_mission.get("topic", "")),
                    "seed_url": str(learn_mission.get("seed_url", "")),
                    "learning_depth": str(learn_mission.get("learning_depth", "")),
                    "input_mode": str(learn_mission.get("input_mode", "")),
                    "expected_outputs": list(learn_mission.get("expected_outputs", []) or []),
                },
                "source_discovery": {
                    "title": "Source Discovery",
                    "compact": True,
                    "sources_found": int(source_discovery.get("found", 0) or 0),
                    "sources_selected": int(source_discovery.get("selected", 0) or 0),
                    "sources_rejected": int(source_discovery.get("rejected", 0) or 0),
                    "discovery_mode": str(source_discovery.get("discovery_mode", "")),
                    "adapter_summary": dict(source_discovery.get("adapter_summary", {}) or {}),
                    "selected_sources": [item for item in list(source_discovery.get("sources", []) or []) if isinstance(item, dict) and bool(item.get("selected", False))][:8],
                    "rejected_sources": [item for item in list(source_discovery.get("sources", []) or []) if isinstance(item, dict) and not bool(item.get("selected", False))][:6],
                },
                "video_analysis": {
                    "title": "Video Analysis",
                    "compact": True,
                    "transcript_coverage": float(video_analysis.get("transcript_coverage", 0.0) or 0.0),
                    "visual_sampling_coverage": int(video_analysis.get("visual_sampling_coverage", 0) or 0),
                    "key_timestamps": list(video_analysis.get("key_timestamps", []) or []),
                    "confidence": float(video_analysis.get("confidence", 0.0) or 0.0),
                },
                "topic_model_card": {
                    "title": "Topic Model",
                    "compact": True,
                    "topic": str(topic_model.get("topic", "")),
                    "core_concepts": list(topic_model.get("core_concepts", []) or []),
                    "required_tools": list(topic_model.get("required_tools", []) or []),
                    "prerequisites": list(topic_model.get("prerequisites", []) or []),
                    "variations": list(topic_model.get("variations", []) or []),
                },
                "consensus_workflow": {
                    "title": "Consensus Workflow",
                    "compact": True,
                    "steps": list(consensus_workflow[:10]),
                    "confidence": float(topic_model.get("confidence", 0.0) or 0.0),
                    "source_support": int(source_discovery.get("selected", 0) or 0),
                },
                "contradictions_card": {
                    "title": "Contradictions",
                    "compact": True,
                    "items": list(contradictions[:8]),
                },
                "learned_skill_card": {
                    "title": "Learned Skill",
                    "compact": True,
                    "skill_name": str(learned_skill.get("skill_name", "")),
                    "topic": str(learned_skill.get("topic", "")),
                    "version": str(learned_skill.get("version", "")),
                    "steps": list(learned_skill.get("workflow", []) or [])[:10],
                    "checkpoints": list(learned_skill.get("checkpoints", []) or [])[:10],
                    "safety_gates": list(learned_skill.get("safety_gates", []) or []),
                    "validation_checks": list(learned_skill.get("validation_checks", []) or []),
                    "app_context": dict(learned_skill.get("app_context", {}) or {}),
                    "practice_policy": dict(learned_skill.get("practice_policy", {}) or {}),
                    "refresh_policy": dict(learned_skill.get("refresh_policy", {}) or {}),
                    "save_enabled": True,
                    "practice_enabled": True,
                    "run_enabled": str((result.get("skill_validation", {}) or {}).get("executable_status", "")) in {"checkpoint_guided", "fully_executable"},
                },
                "mastery_guide_card": {
                    "title": "Mastery Guide",
                    "compact": True,
                    "path": str(mastery_guide.get("path", "")),
                    "summary": str(mastery_guide.get("summary", "")),
                    "practice_plan_path": str(practice_plan.get("path", "")),
                    "practice_preview": dict(result.get("practice_preview", {}) or {}),
                    "refresh_plan": dict(result.get("refresh_plan", {}) or {}),
                },
                "topic_mastery_critics": {
                    "title": "Critic Results",
                    "compact": True,
                    "items": [
                        {
                            "critic": name,
                            "passed": bool((payload or {}).get("passed", False)),
                            "score": float((payload or {}).get("score", 0.0) or 0.0),
                            "reason": str((payload or {}).get("reason", "")),
                        }
                        for name, payload in topic_critics.items()
                        if isinstance(payload, dict)
                    ],
                },
            }
        )
    return cards
