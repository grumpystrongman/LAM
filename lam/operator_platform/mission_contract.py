from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List

from .task_contract_engine import TaskContractEngine


DeliverableMode = str


@dataclass(slots=True)
class MissionContract:
    user_goal: str
    mission_type: str
    domain: str
    subdomain: str
    audience: str
    deliverable_mode: DeliverableMode
    requested_outputs: List[str] = field(default_factory=list)
    success_criteria: List[str] = field(default_factory=list)
    constraints: List[str] = field(default_factory=list)
    source_requirements: Dict[str, Any] = field(default_factory=dict)
    evidence_requirements: Dict[str, Any] = field(default_factory=dict)
    user_context_needed: List[str] = field(default_factory=list)
    quality_bar: str = "professional"
    safety_requirements: Dict[str, Any] = field(default_factory=dict)
    allowed_fallbacks: List[str] = field(default_factory=list)
    artifact_plan: List[Dict[str, Any]] = field(default_factory=list)
    revision_policy: Dict[str, Any] = field(default_factory=dict)
    scope_dimensions: Dict[str, Any] = field(default_factory=dict)
    invalidation_keys: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class MissionContractEngine:
    def __init__(self, task_contract_engine: TaskContractEngine | None = None) -> None:
        self.task_contract_engine = task_contract_engine or TaskContractEngine()

    def extract(self, instruction: str, context: Dict[str, Any] | None = None) -> MissionContract:
        task_contract = self.task_contract_engine.extract(instruction, context=context)
        low = str(instruction or "").lower()
        mission_type = self._mission_type(low, task_contract.domain)
        deliverable_mode = self._deliverable_mode(low, mission_type, task_contract.requested_outputs)
        requested_outputs = self._requested_outputs(low, mission_type, task_contract.requested_outputs)
        artifact_plan = self._artifact_plan(mission_type, requested_outputs)
        return MissionContract(
            user_goal=task_contract.user_goal,
            mission_type=mission_type,
            domain=task_contract.domain,
            subdomain=task_contract.subdomain,
            audience=task_contract.audience,
            deliverable_mode=deliverable_mode,
            requested_outputs=requested_outputs,
            success_criteria=self._success_criteria(mission_type, deliverable_mode, requested_outputs),
            constraints=list(task_contract.constraints),
            source_requirements=dict(task_contract.source_requirements),
            evidence_requirements=dict(task_contract.evidence_requirements),
            user_context_needed=self._user_context_needed(mission_type, task_contract.scope_dimensions),
            quality_bar=self._quality_bar(deliverable_mode, task_contract.audience),
            safety_requirements=dict(task_contract.safety_requirements),
            allowed_fallbacks=self._allowed_fallbacks(mission_type, task_contract.allowed_fallbacks),
            artifact_plan=artifact_plan,
            revision_policy=self._revision_policy(deliverable_mode),
            scope_dimensions=dict(task_contract.scope_dimensions),
            invalidation_keys=dict(task_contract.invalidation_keys),
        )

    def to_task_contract_patch(self, contract: MissionContract) -> Dict[str, Any]:
        return {
            "user_goal": contract.user_goal,
            "domain": contract.domain,
            "subdomain": contract.subdomain,
            "audience": contract.audience,
            "requested_outputs": list(contract.requested_outputs),
            "constraints": list(contract.constraints),
            "source_requirements": dict(contract.source_requirements),
            "evidence_requirements": dict(contract.evidence_requirements),
            "safety_requirements": dict(contract.safety_requirements),
            "allowed_fallbacks": list(contract.allowed_fallbacks),
            "scope_dimensions": dict(contract.scope_dimensions),
            "invalidation_keys": dict(contract.invalidation_keys),
            "mission_type": contract.mission_type,
            "deliverable_mode": contract.deliverable_mode,
        }

    def _mission_type(self, low: str, domain: str) -> str:
        if any(token in low for token in ["tailor my resume", "tailor resume", "cover letter", "apply for", "application checklist", "application package", "next career move"]):
            return "job_search_package"
        if "grant" in low and any(token in low for token in ["proposal", "application", "eligibility", "funder", "submission"]):
            return "grant_application_package"
        if any(token in low for token in ["executive briefing", "executive brief", "brief my vp", "brief leadership", "recommendations"]):
            return "executive_research_brief"
        if any(token in low for token in ["find the story", "data story", "executive summary", "charts"]) and any(token in low for token in ["dataset", "analyze", "analysis"]):
            return "data_storytelling"
        if "ui" in low and any(token in low for token in ["redesign", "commercial", "implement", "frontend"]):
            return "ui_build"
        if domain == "job_market":
            return "job_search_package"
        if domain == "deep_analysis":
            return "data_analysis"
        if domain == "presentation_build":
            return "presentation_build"
        if domain == "email_triage":
            return "email_triage"
        return "generic_operator_task"

    def _deliverable_mode(self, low: str, mission_type: str, requested_outputs: List[str]) -> DeliverableMode:
        if mission_type in {"job_search_package", "grant_application_package"}:
            return "application_ready_package"
        if mission_type == "executive_research_brief" or "presentation" in requested_outputs:
            return "executive_ready_deck"
        if mission_type in {"data_storytelling", "data_analysis"}:
            return "stakeholder_package"
        if mission_type == "ui_build":
            return "production_ready_code"
        if mission_type == "email_triage":
            return "analysis_ready_dataset"
        if any(token in low for token in ["demo", "mock", "example template"]):
            return "demo_package"
        return "research_brief"

    def _requested_outputs(self, low: str, mission_type: str, requested_outputs: List[str]) -> List[str]:
        outputs = list(dict.fromkeys(requested_outputs or []))
        if mission_type == "job_search_package":
            outputs.extend(["job_tracker", "resume", "cover_letter", "application_checklist"])
            if any(token in low for token in ["executive briefing", "executive brief"]):
                outputs.append("executive_brief")
            if any(token in low for token in ["dashboard", "html report"]):
                outputs.append("dashboard")
        elif mission_type == "grant_application_package":
            outputs.extend(["grant_tracker", "proposal", "submission_checklist"])
        elif mission_type == "executive_research_brief":
            outputs.extend(["executive_brief", "presentation"])
        elif mission_type == "data_storytelling":
            outputs.extend(["data_story", "dashboard", "executive_brief"])
        elif mission_type == "ui_build":
            outputs.extend(["ui_spec", "code"])
        if not outputs:
            outputs = ["report"]
        return list(dict.fromkeys(outputs))

    def _artifact_plan(self, mission_type: str, outputs: List[str]) -> List[Dict[str, Any]]:
        plan: List[Dict[str, Any]] = []
        for name in outputs:
            artifact_type = {
                "job_tracker": "spreadsheet",
                "grant_tracker": "spreadsheet",
                "resume": "document",
                "cover_letter": "document",
                "proposal": "document",
                "executive_brief": "document",
                "data_story": "document",
                "presentation": "presentation",
                "dashboard": "dashboard",
                "ui_spec": "spec",
                "code": "code",
                "application_checklist": "checklist",
                "submission_checklist": "checklist",
            }.get(name, "document")
            plan.append(
                {
                    "artifact_type": artifact_type,
                    "name": name,
                    "quality_criteria": self._quality_criteria(name, mission_type),
                    "evidence_required": name not in {"ui_spec", "code"},
                }
            )
        return plan

    def _quality_criteria(self, artifact_name: str, mission_type: str) -> List[str]:
        criteria = ["clear structure", "audience-appropriate tone", "truthful limitations"]
        if artifact_name in {"resume", "cover_letter"}:
            criteria.extend(["tailored to role", "specific achievements", "no generic filler"])
        if artifact_name in {"proposal"}:
            criteria.extend(["eligibility alignment", "funder priorities reflected", "submission-ready structure"])
        if artifact_name in {"executive_brief", "data_story"}:
            criteria.extend(["decision-oriented", "so what is explicit", "supported recommendations"])
        if artifact_name in {"presentation"}:
            criteria.extend(["executive flow", "slide-worthy bullets", "speaker-note-ready structure"])
        if mission_type == "ui_build":
            criteria.append("commercial UI quality")
        return list(dict.fromkeys(criteria))

    def _success_criteria(self, mission_type: str, deliverable_mode: DeliverableMode, outputs: List[str]) -> List[str]:
        criteria = [
            "artifact set matches the mission",
            "evidence is mapped to the final claims",
            "artifact-specific critics pass or remaining issues are disclosed",
            f"deliverable mode satisfied: {deliverable_mode}",
        ]
        if mission_type == "job_search_package":
            criteria.append("job targets, tailored artifacts, and next application actions are present")
        if mission_type == "grant_application_package":
            criteria.append("ranked opportunities and a proposal draft are present")
        if outputs:
            criteria.append(f"requested outputs delivered: {', '.join(outputs)}")
        return criteria

    def _user_context_needed(self, mission_type: str, scope_dimensions: Dict[str, Any]) -> List[str]:
        needed: List[str] = []
        if mission_type == "job_search_package":
            needed.extend(["career_history", "resume_facts", "writing_tone"])
        if mission_type == "grant_application_package":
            needed.extend(["organization_context", "project_background", "budget_constraints"])
        if mission_type in {"executive_research_brief", "data_storytelling"}:
            needed.extend(["stakeholder_preferences", "preferred_executive_style"])
        if scope_dimensions.get("file_target"):
            needed.append("target_file")
        return list(dict.fromkeys(needed))

    def _quality_bar(self, deliverable_mode: DeliverableMode, audience: str) -> str:
        if deliverable_mode in {"application_ready_package", "executive_ready_deck", "production_ready_code"}:
            return "high"
        if audience == "stakeholder":
            return "professional"
        return "standard"

    def _allowed_fallbacks(self, mission_type: str, task_fallbacks: List[str]) -> List[str]:
        fallbacks = list(task_fallbacks or [])
        if mission_type in {"job_search_package", "grant_application_package", "executive_research_brief", "data_storytelling"}:
            fallbacks.extend(["template_package", "demo_package", "real_partial"])
        return list(dict.fromkeys(fallbacks))

    def _revision_policy(self, deliverable_mode: DeliverableMode) -> Dict[str, Any]:
        max_revisions = 2
        if deliverable_mode in {"application_ready_package", "executive_ready_deck", "production_ready_code"}:
            max_revisions = 3
        return {"max_revisions_per_artifact": max_revisions, "auto_repair": True, "critic_blocking": True}


def mission_contract_json(contract: MissionContract) -> str:
    return json.dumps(contract.to_dict(), indent=2)
