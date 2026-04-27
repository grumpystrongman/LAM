from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List


@dataclass(slots=True)
class ExecutionNode:
    node_id: str
    capability: str
    inputs: Dict[str, Any] = field(default_factory=dict)
    outputs: List[str] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)
    success_criteria: List[str] = field(default_factory=list)
    verification_method: str = ""
    status: str = "pending"
    attempts: int = 0
    critics: List[Dict[str, Any]] = field(default_factory=list)
    artifacts: List[str] = field(default_factory=list)
    artifact_details: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    evidence: List[str] = field(default_factory=list)
    output_payload: Dict[str, Any] = field(default_factory=dict)
    revision_count: int = 0
    error: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ExecutionGraph:
    graph_id: str
    task_id: str
    domain: str
    nodes: List[ExecutionNode] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    status: str = "pending"
    events: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "graph_id": self.graph_id,
            "task_id": self.task_id,
            "domain": self.domain,
            "status": self.status,
            "nodes": [node.to_dict() for node in self.nodes],
            "metadata": dict(self.metadata),
            "events": list(self.events),
        }
