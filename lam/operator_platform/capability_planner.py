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
        nodes: List[ExecutionNode] = []
        completed: List[str] = []
        for name in sequence:
            spec = self.registry.get(name)
            nodes.append(
                ExecutionNode(
                    capability=spec.name,
                    inputs={"task_contract_domain": contract.domain, "requested_outputs": list(contract.requested_outputs)},
                    outputs=list(spec.outputs),
                    dependencies=list(completed[-2:]),
                    success_criteria=list(spec.success_criteria),
                    verification_method=spec.verification_method,
                )
            )
            completed.append(spec.name)
        return ExecutionGraph(
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
        else:
            sequence.extend(["report_build", "stakeholder_summary"])
        if "spreadsheet" in outputs:
            sequence.append("spreadsheet_build")
        sequence.append("artifact_export")
        if any(token in contract.user_goal.lower() for token in ["send", "submit", "publish", "purchase"]):
            sequence.append("approval_gate")
        deduped: List[str] = []
        for item in sequence:
            if item not in deduped and self.registry.has(item):
                deduped.append(item)
        return deduped
