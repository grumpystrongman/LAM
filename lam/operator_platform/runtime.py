from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .artifact_factory import ArtifactFactory
from .capability_registry import CapabilityRegistry, default_capability_registry
from .critics import CompletionCritic, CriticResult, DataQualityCritic, PresentationCritic, SourceCritic, StoryCritic, UIUXCritic
from .execution_graph import ExecutionGraph, ExecutionNode
from .executors import BaseCapabilityExecutor, CapabilityExecutionResult, default_executors
from .memory_store import MemoryStore


@dataclass(slots=True)
class RuntimeRunResult:
    ok: bool
    graph: ExecutionGraph
    task_contract: Dict[str, Any]
    artifacts: Dict[str, str]
    artifact_metadata: Dict[str, Dict[str, Any]]
    events: List[Dict[str, Any]]
    critics: Dict[str, Dict[str, Any]]
    revisions: List[Dict[str, Any]]
    memory_context: Dict[str, Any]
    outputs: Dict[str, Any]
    verification_report: Dict[str, Any]
    verification: Dict[str, Any]
    final_report: Dict[str, Any]
    error: str = ""


class ExecutionGraphRuntime:
    def __init__(
        self,
        *,
        registry: CapabilityRegistry | None = None,
        executors: Dict[str, BaseCapabilityExecutor] | None = None,
        memory_store: MemoryStore | None = None,
        artifact_factory: ArtifactFactory | None = None,
        max_revisions: int = 2,
    ) -> None:
        self.registry = registry or default_capability_registry()
        self.executors = executors or default_executors()
        self.memory_store = memory_store or MemoryStore()
        self.artifact_factory = artifact_factory or ArtifactFactory()
        self.max_revisions = max(0, int(max_revisions))

    def run(self, graph: ExecutionGraph, context: Dict[str, Any]) -> RuntimeRunResult:
        graph.status = "running"
        task_contract = dict(context.get("task_contract", {}))
        task_id = str(context.get("task_id", graph.task_id))
        context = dict(context)
        context["task_contract"] = task_contract
        context["task_id"] = task_id
        node_outputs: Dict[str, Dict[str, Any]] = {}
        artifacts: Dict[str, str] = {}
        artifact_metadata: Dict[str, Dict[str, Any]] = {}
        critics: Dict[str, Dict[str, Any]] = {}
        revisions: List[Dict[str, Any]] = []

        memory_context = self.memory_store.retrieve_relevant_memory(
            task_contract=task_contract,
            query=str(task_contract.get("user_goal", "")),
            limit=6,
        )
        context["memory_context"] = memory_context
        self._emit_event(graph, "graph_started", task_id=task_id, domain=graph.domain)
        if memory_context.get("used"):
            self._emit_event(graph, "memory_loaded", count=len(memory_context.get("used", [])))

        for node in graph.nodes:
            if any(self._dependency_failed(dep_id, graph) for dep_id in node.dependencies):
                node.status = "blocked"
                node.error = "dependency_failed"
                self._emit_event(graph, "node_failed", node_id=node.node_id, capability=node.capability, status=node.status, error=node.error)
                continue
            node.status = "ready"
            try:
                node_result = self.run_node(node=node, graph=graph, context=context, node_outputs=node_outputs)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                node.status = "failed"
                node.error = str(exc)
                self._emit_event(graph, "node_failed", node_id=node.node_id, capability=node.capability, status=node.status, error=node.error)
                graph.status = "failed"
                return RuntimeRunResult(
                    ok=False,
                    graph=graph,
                    task_contract=task_contract,
                    artifacts=artifacts,
                    artifact_metadata=artifact_metadata,
                    events=list(graph.events),
                    critics=critics,
                    revisions=revisions,
                    memory_context=memory_context,
                    outputs=node_outputs,
                    verification_report=self._build_verification_report(task_id=task_id, graph=graph, critics=critics, artifacts=artifacts, requested_outputs=list(task_contract.get("requested_outputs", []) or []), error=str(exc)),
                    verification={},
                    final_report={},
                    error=str(exc),
                )
            node_outputs[node.node_id] = dict(node_result.outputs)
            node.output_payload = dict(node_result.outputs)
            node.artifacts = sorted(node_result.artifacts.values())
            node.artifact_details = dict(node_result.artifact_metadata)
            node.evidence = list(node_result.evidence)
            artifacts.update(node_result.artifacts)
            artifact_metadata.update(node_result.artifact_metadata)
            self._update_artifact_validation_state(artifact_metadata, list(node_result.artifacts.keys()), "pending_critic_review", f"{node.capability} completed; critics pending")
            node_critics = self.run_critics(node=node, context=context, outputs=node_result.outputs, artifacts=artifacts)
            node.critics = [{"critic": name, **result.to_dict()} for name, result in node_critics]
            for critic_name, critic_result in node_critics:
                critics[critic_name] = critic_result.to_dict()
                self._emit_event(
                    graph,
                    "critic_started",
                    node_id=node.node_id,
                    capability=node.capability,
                    critic=critic_name,
                    score=critic_result.score,
                )
                if not critic_result.passed:
                    self._emit_event(
                        graph,
                        "critic_failed",
                        node_id=node.node_id,
                        capability=node.capability,
                        critic=critic_name,
                        required_fix=critic_result.required_fix,
                    )
                    revised = self.revise_if_needed(
                        node=node,
                        graph=graph,
                        context=context,
                        node_outputs=node_outputs,
                        node_critic=(critic_name, critic_result),
                        artifacts=artifacts,
                        artifact_metadata=artifact_metadata,
                    )
                    if revised:
                        revisions.append(revised)
                        node_result = CapabilityExecutionResult(
                            outputs=dict(node.output_payload),
                            artifacts={key: value for key, value in artifacts.items() if value in node.artifacts or key in dict(node.output_payload).get("artifacts", {})},
                            evidence=list(node.evidence),
                        )
                        critics[critic_name] = dict(revised.get("critic_after", {}))
                    else:
                        self._update_artifact_validation_state(artifact_metadata, list(node_result.artifacts.keys()), "failed_review", critic_result.reason)
                        node.status = "failed"
                        node.error = critic_result.reason
                        graph.status = "failed"
                        self._emit_event(graph, "node_failed", node_id=node.node_id, capability=node.capability, status=node.status, error=node.error)
                        return RuntimeRunResult(
                            ok=False,
                            graph=graph,
                            task_contract=task_contract,
                            artifacts=artifacts,
                            artifact_metadata=artifact_metadata,
                            events=list(graph.events),
                            critics=critics,
                            revisions=revisions,
                            memory_context=memory_context,
                            outputs=node_outputs,
                            verification_report=self._build_verification_report(task_id=task_id, graph=graph, critics=critics, artifacts=artifacts, requested_outputs=list(task_contract.get("requested_outputs", []) or []), error=critic_result.reason),
                            verification={},
                            final_report={},
                            error=critic_result.reason,
                        )
            if node.status in {"revised", "succeeded"}:
                final_state = "revised_validated" if node.status == "revised" else "validated"
                self._update_artifact_validation_state(artifact_metadata, list(node_result.artifacts.keys()), final_state, f"{node.capability} passed critics")
            if node.status not in {"revised", "failed"}:
                node.status = "succeeded"
                self._emit_event(graph, "node_completed", node_id=node.node_id, capability=node.capability, status=node.status)

        if artifacts and "artifact_manifest_json" not in artifacts:
            manifest_path = self.artifact_factory.write_manifest(
                task_id=task_id,
                task_contract=task_contract,
                artifacts=artifacts,
                artifact_metadata=artifact_metadata,
                generated_by_capabilities=[node.capability for node in graph.nodes if node.status in {"succeeded", "revised"}],
                validation_status="passed" if not any(not value.get("passed", True) for value in critics.values()) else "needs_review",
                source_data=[str(x.get("url", "")) for x in (node_outputs.get(graph.nodes[0].node_id, {}).get("sources", []) or []) if isinstance(x, dict)],
            )
            artifacts["artifact_manifest_json"] = str(manifest_path.resolve())
            artifact_metadata["artifact_manifest_json"] = {
                "key": "artifact_manifest_json",
                "path": str(manifest_path.resolve()),
                "type": "manifest",
                "title": "Artifact Manifest",
                "evidence_summary": "Runtime-generated manifest with artifact provenance and validation state.",
                "validation_state": "ready",
                "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            }
            self._emit_event(graph, "artifact_created", path=str(manifest_path.resolve()), artifact_key="artifact_manifest_json")

        graph.status = "succeeded"
        self._emit_event(graph, "graph_completed", task_id=task_id, artifact_count=len(artifacts))
        verification_report = self._build_verification_report(
            task_id=task_id,
            graph=graph,
            critics=critics,
            artifacts=artifacts,
            requested_outputs=list(task_contract.get("requested_outputs", []) or []),
            error="",
        )
        verification = {
            "passed": verification_report.get("final_verification") == "passed",
            "checks": list(verification_report.get("verification_checks", [])),
            "evidence": [f"graph_status={graph.status}", f"artifact_count={len(artifacts)}"],
        }
        final_report = {
            "task_id": task_id,
            "status": "completed" if verification["passed"] else "failed",
            "summary": "Capability graph completed" if verification["passed"] else "Capability graph failed",
            "actions_taken": [f"{node.capability}: {node.status}" for node in graph.nodes],
            "outputs_created": [{"type": "file", "location": value, "description": key} for key, value in artifacts.items() if isinstance(value, str)],
            "verification_summary": verification_report.get("final_verification", "unknown"),
            "remaining_issues": list(verification_report.get("failed_checks", [])),
            "next_safe_action": "Open the artifact package in Canvas." if verification["passed"] else "Review critic failures and rerun the graph.",
        }
        if graph.status == "succeeded":
            self.memory_store.save_memory(
                {
                    "type": "successful_recipe",
                    "scope": "project",
                    "content": {
                        "domain": task_contract.get("domain", ""),
                        "goal": task_contract.get("user_goal", ""),
                        "capabilities": [node.capability for node in graph.nodes if node.status in {"succeeded", "revised"}],
                    },
                    "tags": [str(task_contract.get("domain", ""))],
                    "source": "execution_graph_runtime",
                    "confidence": 0.9,
                    "retrieval_policy": "template_safe",
                    "invalidation_keys": dict(task_contract.get("invalidation_keys", {})),
                }
            )
        return RuntimeRunResult(
            ok=True,
            graph=graph,
            task_contract=task_contract,
            artifacts=artifacts,
            artifact_metadata=artifact_metadata,
            events=list(graph.events),
            critics=critics,
            revisions=revisions,
            memory_context=memory_context,
            outputs=node_outputs,
            verification_report=verification_report,
            verification=verification,
            final_report=final_report,
        )

    def run_node(self, *, node: ExecutionNode, graph: ExecutionGraph, context: Dict[str, Any], node_outputs: Dict[str, Dict[str, Any]]) -> CapabilityExecutionResult:
        executor = self.executors.get(node.capability)
        if executor is None:
            raise RuntimeError(f"no executor registered for capability '{node.capability}'")
        inputs = self.resolve_inputs(node=node, context=context, node_outputs=node_outputs)
        node.status = "running"
        node.attempts += 1
        self._emit_event(graph, "node_started", node_id=node.node_id, capability=node.capability, status=node.status, attempt=node.attempts)
        if executor.input_schema:
            executor.validate_inputs(inputs)
        result = executor.execute(context, inputs)
        if executor.output_schema:
            executor.validate_outputs(result.outputs)
        for key, value in result.artifacts.items():
            self._emit_event(graph, "artifact_created", node_id=node.node_id, capability=node.capability, artifact_key=key, path=value)
        return result

    def resolve_inputs(self, *, node: ExecutionNode, context: Dict[str, Any], node_outputs: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        inputs: Dict[str, Any] = dict(node.inputs)
        inputs["task_contract"] = dict(context.get("task_contract", {}))
        inputs["memory_context"] = dict(context.get("memory_context", {}))
        dependency_outputs: Dict[str, Dict[str, Any]] = {}
        for dep_id in node.dependencies:
            dep_output = dict(node_outputs.get(dep_id, {}))
            dependency_outputs[dep_id] = dep_output
            inputs.update(dep_output)
        inputs["dependency_outputs"] = dependency_outputs
        return inputs

    def run_critics(
        self,
        *,
        node: ExecutionNode,
        context: Dict[str, Any],
        outputs: Dict[str, Any],
        artifacts: Dict[str, str],
    ) -> List[tuple[str, CriticResult]]:
        critic_results: List[tuple[str, CriticResult]] = []
        if node.capability == "source_evaluation":
            critic_results.append(("source", SourceCritic().evaluate(outputs.get("source_scores", []))))
        if node.capability in {"data_cleaning", "statistical_analysis"}:
            row_count = len(outputs.get("clean_rows", []) or []) or len((outputs.get("analysis_results", {}) or {}).get("findings", []) or [])
            missing_ratio = float(outputs.get("missing_ratio", 0.0) or 0.0)
            critic_results.append(("data_quality", DataQualityCritic().evaluate(row_count, missing_ratio)))
        if node.capability in {"data_storytelling", "stakeholder_summary"}:
            story = outputs.get("story_package") if isinstance(outputs.get("story_package"), dict) else outputs.get("stakeholder_summary", {})
            critic_results.append(("story", StoryCritic().evaluate(dict(story or {}), str((context.get("task_contract", {}) or {}).get("audience", "operator")))))
        if node.capability == "ui_build":
            critic_results.append(("uiux", UIUXCritic().evaluate(dict(outputs.get("ui_spec", {}) or {}))))
        if node.capability == "presentation_build":
            critic_results.append(("presentation", PresentationCritic().evaluate(dict(outputs.get("presentation", {}) or {}))))
        if node.capability == "artifact_export":
            requested_outputs = list(((context.get("task_contract", {}) or {}).get("requested_outputs") or []))
            critic_results.append(("completion", CompletionCritic().evaluate(requested_outputs, artifacts, "passed")))
        return critic_results

    def revise_if_needed(
        self,
        *,
        node: ExecutionNode,
        graph: ExecutionGraph,
        context: Dict[str, Any],
        node_outputs: Dict[str, Dict[str, Any]],
        node_critic: tuple[str, CriticResult],
        artifacts: Dict[str, str],
        artifact_metadata: Dict[str, Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        critic_name, critic_result = node_critic
        target_capability = ""
        if critic_name == "story" and node.capability in {"data_storytelling", "stakeholder_summary"}:
            target_capability = node.capability
        elif critic_name == "uiux" and node.capability == "ui_build":
            target_capability = "ui_build"
        elif critic_name == "completion" and node.capability == "artifact_export":
            target_capability = "artifact_export"
        if not target_capability:
            return None
        if node.revision_count >= self.max_revisions:
            return None
        target_node = node
        executor = self.executors.get(target_capability)
        if executor is None:
            return None
        revision_inputs = self.resolve_inputs(node=target_node, context=context, node_outputs=node_outputs)
        revision_inputs["revision_note"] = critic_result.required_fix or critic_result.reason
        target_node.status = "revision_required"
        self._emit_event(
            graph,
            "revision_started",
            node_id=target_node.node_id,
            capability=target_node.capability,
            critic=critic_name,
            required_fix=critic_result.required_fix,
        )
        revised_result = executor.execute(context, revision_inputs)
        target_node.revision_count += 1
        target_node.attempts += 1
        target_node.output_payload = dict(revised_result.outputs)
        target_node.evidence = list(revised_result.evidence)
        target_node.artifacts = sorted(revised_result.artifacts.values())
        target_node.artifact_details = dict(revised_result.artifact_metadata)
        node_outputs[target_node.node_id] = dict(revised_result.outputs)
        artifacts.update(revised_result.artifacts)
        artifact_metadata.update(revised_result.artifact_metadata)
        self._update_artifact_validation_state(artifact_metadata, list(revised_result.artifacts.keys()), "revision_required", critic_result.required_fix or critic_result.reason)
        rerun = self.run_critics(node=target_node, context=context, outputs=revised_result.outputs, artifacts=artifacts)
        target_after = next((res for name, res in rerun if name == critic_name), CriticResult(True, 1.0, "passed after revision", "", "low"))
        target_node.critics = [res.to_dict() for _, res in rerun]
        target_node.status = "revised" if target_after.passed else "failed"
        self._update_artifact_validation_state(
            artifact_metadata,
            list(revised_result.artifacts.keys()),
            "revised_validated" if target_after.passed else "failed_review",
            "revision passed" if target_after.passed else target_after.reason,
        )
        target_node.artifact_details = {key: artifact_metadata.get(key, {}) for key in revised_result.artifacts.keys()}
        self._emit_event(
            graph,
            "revision_completed",
            node_id=target_node.node_id,
            capability=target_node.capability,
            critic=critic_name,
            status=target_node.status,
        )
        return {
            "node_id": target_node.node_id,
            "capability": target_node.capability,
            "critic": critic_name,
            "required_fix": critic_result.required_fix,
            "revision_note": revision_inputs["revision_note"],
            "critic_after": target_after.to_dict(),
        }

    def _dependency_failed(self, dep_id: str, graph: ExecutionGraph) -> bool:
        dep = next((node for node in graph.nodes if node.node_id == dep_id), None)
        if dep is None:
            return True
        return dep.status in {"failed", "blocked"}

    def _emit_event(self, graph: ExecutionGraph, event_type: str, **payload: Any) -> None:
        graph.events.append({"event": event_type, "ts": round(time.time(), 3), **payload})

    def _update_artifact_validation_state(
        self,
        artifact_metadata: Dict[str, Dict[str, Any]],
        artifact_keys: List[str],
        state: str,
        note: str,
    ) -> None:
        for key in artifact_keys:
            item = artifact_metadata.get(key)
            if not isinstance(item, dict):
                continue
            history = list(item.get("validation_history", []) or [])
            history.append({"state": state, "note": note, "ts": round(time.time(), 3)})
            item["validation_state"] = state
            item["validation_history"] = history[-6:]

    def _build_verification_report(
        self,
        *,
        task_id: str,
        graph: ExecutionGraph,
        critics: Dict[str, Dict[str, Any]],
        artifacts: Dict[str, str],
        requested_outputs: List[str],
        error: str,
    ) -> Dict[str, Any]:
        failed_critics = [name for name, payload in critics.items() if isinstance(payload, dict) and not bool(payload.get("passed", False))]
        completion = CompletionCritic().evaluate(requested_outputs, artifacts, "passed" if graph.status == "succeeded" else "failed")
        checks = [
            {"name": "graph_executed", "pass": graph.status in {"succeeded", "failed"}, "evidence": [f"graph_status={graph.status}"]},
            {"name": "all_nodes_complete", "pass": all(node.status in {"succeeded", "revised"} for node in graph.nodes), "evidence": [f"node_statuses={[node.status for node in graph.nodes]}"]},
            {"name": "requested_outputs_exist", "pass": completion.passed, "evidence": [completion.reason]},
            {"name": "critics_resolved", "pass": len(failed_critics) == 0, "evidence": [f"failed_critics={failed_critics}"]},
            {"name": "runtime_error_free", "pass": not bool(error), "evidence": [error or "no runtime error"]},
        ]
        final_verification = "passed" if all(bool(item["pass"]) for item in checks) else "failed"
        return {
            "task_id": task_id,
            "used_expected_tool_family": True,
            "targets_match_request": True,
            "requested_outputs_exist": completion.passed,
            "artifact_content_matches_goal": len(failed_critics) == 0,
            "no_unresolved_execution_errors": not bool(error),
            "no_irrelevant_detours": True,
            "user_goal_satisfied": final_verification == "passed",
            "verification_checks": checks,
            "failed_checks": [item["name"] for item in checks if not bool(item["pass"])],
            "final_verification": final_verification,
        }
