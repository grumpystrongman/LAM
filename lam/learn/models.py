from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List


@dataclass(slots=True)
class LearnMission:
    topic: str
    seed_url: str = ""
    input_mode: str = "topic"
    learning_depth: str = "normal"
    expected_outputs: List[str] = field(default_factory=list)
    max_related_videos: int = 5
    max_supporting_sources: int = 3

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class LearnSource:
    source_url: str
    title: str
    source_type: str
    score: float = 0.0
    rank: int = 0
    reason_selected: str = ""
    expected_use: str = ""
    selected: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)
    transcript: str = ""
    summary: str = ""
    platform: str = ""
    adapter: Dict[str, Any] = field(default_factory=dict)
    adapter_capabilities: Dict[str, Any] = field(default_factory=dict)
    canonical_url: str = ""
    authority_level: str = ""
    version_sensitive: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ProcedureStep:
    description: str
    action_type: str
    target: str = ""
    supporting_sources: List[str] = field(default_factory=list)
    timestamp_refs: List[str] = field(default_factory=list)
    confidence: float = 0.0
    risk_level: str = "low"
    approval_required: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class TopicModel:
    topic: str
    core_concepts: List[str] = field(default_factory=list)
    terminology: List[str] = field(default_factory=list)
    required_tools: List[str] = field(default_factory=list)
    prerequisites: List[str] = field(default_factory=list)
    mental_model: List[str] = field(default_factory=list)
    common_use_cases: List[str] = field(default_factory=list)
    variations: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class LearnedSkill:
    skill_id: str
    skill_name: str
    topic: str
    purpose: str
    domain: str
    source_urls: List[str] = field(default_factory=list)
    source_summary: List[str] = field(default_factory=list)
    prerequisites: List[str] = field(default_factory=list)
    required_tools: List[str] = field(default_factory=list)
    workflow: List[Dict[str, Any]] = field(default_factory=list)
    decision_points: List[str] = field(default_factory=list)
    safety_gates: List[str] = field(default_factory=list)
    validation_checks: List[str] = field(default_factory=list)
    troubleshooting: List[str] = field(default_factory=list)
    common_mistakes: List[str] = field(default_factory=list)
    variations: List[str] = field(default_factory=list)
    confidence_score: float = 0.0
    limitations: List[str] = field(default_factory=list)
    version: str = "1.0"
    executable_status: str = "guided_only"
    app_context: Dict[str, Any] = field(default_factory=dict)
    feedback_summary: Dict[str, Any] = field(default_factory=dict)
    next_review_at: str = ""
    checkpoints: List[Dict[str, Any]] = field(default_factory=list)
    source_adapter_summary: Dict[str, Any] = field(default_factory=dict)
    refresh_policy: Dict[str, Any] = field(default_factory=dict)
    practice_policy: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
