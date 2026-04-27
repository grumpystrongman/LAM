from __future__ import annotations

import hashlib
from typing import Dict, List

from .capability_registry import CapabilityRegistry, default_capability_registry
from .execution_graph import ExecutionGraph, ExecutionNode
from .task_contract_engine import TaskContract


class CapabilityPlanner:
    def __init__(self, registry: CapabilityRegistry | None = None) -> None:
        self.registry = registry or default_capability_registry()

    def plan(self, contract: TaskContract) -> ExecutionGraph:
        sequence = self._sequence_for_contract(contract)
        task_id = hashlib.sha1(contract.user_goal.encode("utf-8")).hexdigest()[:16]
        graph_id = hashlib.sha1(f"{task_id}:{contract.domain}:{contract.audience}".encode("utf-8")).hexdigest()[:16]
        nodes: List[ExecutionNode] = []
        capability_nodes: Dict[str, str] = {}
        for idx, name in enumerate(sequence, start=1):
            spec = self.registry.get(name)
            node_id = f"n{idx:02d}_{spec.name}"
            nodes.append(
                ExecutionNode(
                    node_id=node_id,
                    capability=spec.name,
                    inputs={"task_contract_domain": contract.domain, "requested_outputs": list(contract.requested_outputs)},
                    outputs=list(spec.outputs),
                    dependencies=self._dependencies_for(spec.name, capability_nodes),
                    success_criteria=list(spec.success_criteria),
                    verification_method=spec.verification_method,
                )
            )
            capability_nodes[spec.name] = node_id
        return ExecutionGraph(
            graph_id=graph_id,
            task_id=task_id,
            domain=contract.domain,
            nodes=nodes,
            metadata={
                "audience": contract.audience,
                "geography": contract.geography,
                "requested_outputs": list(contract.requested_outputs),
                "invalidation_keys": dict(contract.invalidation_keys),
            },
        )

    def _sequence_for_contract(self, contract: TaskContract) -> List[str]:
        outputs = set(contract.requested_outputs)
        if contract.domain == "artifact_generation":
            sequence = ["deep_research", "report_build", "stakeholder_summary"]
            if "presentation" in outputs:
                sequence.insert(2, "data_storytelling")
                sequence.insert(3, "presentation_build")
            if any(item in outputs for item in {"dashboard", "ui"}):
                sequence.extend(["ui_build"])
            sequence.append("artifact_export")
            return self._dedupe(sequence)
        if contract.domain == "competitor_analysis":
            sequence = ["deep_research", "source_evaluation", "data_storytelling", "presentation_build", "report_build", "stakeholder_summary", "artifact_export"]
            return self._dedupe(sequence)
        sequence: List[str] = ["deep_research", "source_evaluation"]
        if contract.domain in {"payer_pricing_review", "deep_analysis"}:
            sequence.extend(["file_inspection", "data_cleaning", "statistical_analysis"])
        if "rag_index" in outputs or contract.domain == "payer_pricing_review":
            sequence.extend(["rag_build", "rag_query"])
        if contract.domain in {"deep_analysis", "ui_build"} or "code" in outputs:
            sequence.extend(["code_write", "code_test", "code_fix"])
        if "dashboard" in outputs or contract.domain == "ui_build":
            sequence.extend(["data_visualization", "ui_build"])
        if "presentation" in outputs or contract.domain == "presentation_build":
            sequence.extend(["data_storytelling", "presentation_build"])
            if "report" in outputs or contract.audience == "stakeholder":
                sequence.extend(["report_build", "stakeholder_summary"])
        else:
            sequence.extend(["report_build", "stakeholder_summary"])
        if "spreadsheet" in outputs:
            sequence.append("spreadsheet_build")
        sequence.append("artifact_export")
        if any(token in contract.user_goal.lower() for token in ["send", "submit", "publish", "purchase"]):
            sequence.append("approval_gate")
        return self._dedupe(sequence)

    def _dedupe(self, sequence: List[str]) -> List[str]:
        deduped: List[str] = []
        for item in sequence:
            if item not in deduped and self.registry.has(item):
                deduped.append(item)
        return deduped

    def _dependencies_for(self, capability: str, capability_nodes: Dict[str, str]) -> List[str]:
        dependency_order = {
            "source_evaluation": ["deep_research"],
            "file_inspection": ["deep_research"],
            "data_cleaning": ["deep_research"],
            "statistical_analysis": ["data_cleaning"],
            "data_visualization": ["statistical_analysis"],
            "rag_build": ["deep_research", "statistical_analysis"],
            "rag_query": ["rag_build"],
            "code_write": ["deep_research", "rag_build"],
            "code_test": ["code_write"],
            "code_fix": ["code_test"],
            "data_storytelling": ["statistical_analysis", "deep_research"],
            "report_build": ["data_storytelling", "statistical_analysis", "deep_research", "code_fix"],
            "stakeholder_summary": ["report_build", "data_storytelling"],
            "presentation_build": ["data_storytelling"],
            "spreadsheet_build": ["statistical_analysis"],
            "ui_build": ["data_visualization", "data_storytelling", "deep_research"],
            "artifact_export": ["report_build", "stakeholder_summary", "presentation_build", "spreadsheet_build", "ui_build", "rag_build", "code_write"],
            "approval_gate": ["artifact_export"],
        }
        wanted = dependency_order.get(capability, [])
        return [capability_nodes[name] for name in wanted if name in capability_nodes]
