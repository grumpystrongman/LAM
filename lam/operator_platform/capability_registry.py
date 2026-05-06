from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List


@dataclass(slots=True)
class CapabilitySpec:
    name: str
    description: str
    inputs: List[str]
    outputs: List[str]
    tools: List[str]
    safety_level: str
    success_criteria: List[str]
    common_failure_modes: List[str]
    verification_method: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class CapabilityRegistry:
    def __init__(self, specs: List[CapabilitySpec] | None = None) -> None:
        self._specs: Dict[str, CapabilitySpec] = {spec.name: spec for spec in (specs or [])}

    def register(self, spec: CapabilitySpec) -> None:
        self._specs[spec.name] = spec

    def get(self, name: str) -> CapabilitySpec:
        return self._specs[name]

    def has(self, name: str) -> bool:
        return name in self._specs

    def list(self) -> List[CapabilitySpec]:
        return [self._specs[name] for name in sorted(self._specs.keys())]


def default_capability_registry() -> CapabilityRegistry:
    rows = [
        ("deep_research", "Collect and synthesize multi-source evidence.", ["task_contract"], ["research_notes"], ["browser", "search"], "medium"),
        ("topic_mastery_learn", "Analyze a seed video or topic across multiple sources and build a reusable learned skill package.", ["task_contract"], ["learned_skill"], ["browser", "search", "filesystem"], "medium"),
        ("research_collection", "Collect search results, browser notes, and recommendation evidence for a research task.", ["task_contract"], ["research_notes"], ["browser", "search"], "medium"),
        ("mission_research", "Collect mission-scoped evidence for professional work products such as jobs, grants, and executive briefs.", ["task_contract"], ["mission_research"], ["browser", "search"], "medium"),
        ("mission_work_product", "Build mission-scoped professional artifacts with evidence mapping and revision.", ["task_contract"], ["mission_package"], ["filesystem", "language"], "medium"),
        ("competitor_research", "Research and rank competitor evidence for a target market.", ["task_contract"], ["research_notes"], ["browser", "search"], "medium"),
        ("source_evaluation", "Score source credibility and fit.", ["research_notes"], ["source_scores"], ["critic"], "low"),
        ("web_browse", "Interact with browser targets and collect observations.", ["url", "query"], ["browser_notes"], ["browser", "playwright"], "medium"),
        ("file_inspection", "Inspect files, folders, and local inputs.", ["paths"], ["file_inventory"], ["filesystem"], "low"),
        ("spreadsheet_build", "Create spreadsheet deliverables.", ["structured_rows"], ["spreadsheet"], ["excel", "csv"], "low"),
        ("data_cleaning", "Normalize and validate tabular data.", ["raw_rows"], ["clean_rows"], ["python", "spreadsheet"], "low"),
        ("statistical_analysis", "Run descriptive and comparative analysis.", ["clean_rows"], ["analysis_results"], ["python"], "medium"),
        ("data_visualization", "Build chart specs and dashboards.", ["analysis_results"], ["chart_specs"], ["html", "spreadsheet"], "low"),
        ("rag_build", "Build local retrieval indexes over source corpora.", ["documents"], ["rag_index"], ["sqlite", "vector"], "medium"),
        ("rag_query", "Answer questions against built indexes.", ["rag_index", "question"], ["rag_answer"], ["sqlite", "retriever"], "low"),
        ("code_write", "Create or modify code artifacts.", ["task_contract"], ["code_changes"], ["filesystem", "editor"], "medium"),
        ("code_test", "Run smoke tests or unit tests.", ["code_changes"], ["test_results"], ["shell"], "medium"),
        ("code_fix", "Apply fixes based on failing tests or verification.", ["test_results"], ["code_fixes"], ["filesystem", "shell"], "medium"),
        ("ui_build", "Create commercial-grade UI structures and components.", ["requirements"], ["ui_spec"], ["frontend"], "medium"),
        ("presentation_build", "Build executive presentation outlines or decks.", ["story_package"], ["presentation"], ["markdown", "slides"], "low"),
        ("report_build", "Produce reports and summaries.", ["analysis_results"], ["report"], ["markdown", "html"], "low"),
        ("data_storytelling", "Turn analysis into audience-aware narrative.", ["analysis_results"], ["story_package"], ["language"], "low"),
        ("artifact_export", "Write final files and manifests.", ["artifacts"], ["export_bundle"], ["filesystem"], "low"),
        ("stakeholder_summary", "Prepare stakeholder-ready answer and next steps.", ["report"], ["stakeholder_summary"], ["language"], "low"),
        ("approval_gate", "Require user approval for risky external actions.", ["planned_action"], ["approval_state"], ["policy"], "high"),
    ]
    registry = CapabilityRegistry()
    for name, description, inputs, outputs, tools, safety in rows:
        registry.register(
            CapabilitySpec(
                name=name,
                description=description,
                inputs=inputs,
                outputs=outputs,
                tools=tools,
                safety_level=safety,
                success_criteria=[f"{name} produced expected outputs"],
                common_failure_modes=["missing_input", "verification_failed"],
                verification_method="artifact and critic check",
            )
        )
    return registry
