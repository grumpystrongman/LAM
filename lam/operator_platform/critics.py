from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterable, List


@dataclass(slots=True)
class CriticResult:
    passed: bool
    score: float
    reason: str
    required_fix: str
    severity: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class ActionCritic:
    def evaluate(self, next_action: str, current_context: Dict[str, Any] | None = None) -> CriticResult:
        ctx = current_context or {}
        if not str(next_action or "").strip():
            return CriticResult(False, 0.0, "No next action selected.", "Choose a concrete next action.", "high")
        if ctx.get("loop_risk"):
            return CriticResult(False, 0.2, "Action repeats a known failed path.", "Choose a different capability or target.", "medium")
        return CriticResult(True, 0.9, "Action is concrete.", "", "low")


class SourceCritic:
    def evaluate(self, sources: Iterable[Dict[str, Any]]) -> CriticResult:
        rows = [s for s in sources if isinstance(s, dict)]
        credible = [s for s in rows if str(s.get("url", "")).startswith(("http://", "https://", "file://", "sample://", "user://"))]
        if not credible:
            return CriticResult(False, 0.1, "No source references available.", "Add source-backed evidence.", "high")
        return CriticResult(True, min(1.0, 0.4 + len(credible) * 0.1), "Source set is referenced.", "", "low")


class DataQualityCritic:
    def evaluate(self, row_count: int, missing_ratio: float) -> CriticResult:
        if row_count <= 0:
            return CriticResult(False, 0.0, "No rows available for analysis.", "Collect or ingest usable data.", "high")
        if missing_ratio > 0.35:
            return CriticResult(False, 0.35, "Missingness is too high for confident analysis.", "Clean data or lower confidence.", "medium")
        return CriticResult(True, max(0.4, 1.0 - missing_ratio), "Data quality is sufficient for current pass.", "", "low")


class StatsCritic:
    def evaluate(self, sample_size: int, methods: Iterable[str]) -> CriticResult:
        method_list = [str(x) for x in methods]
        if sample_size < 3 and any(x in method_list for x in ["correlation", "regression"]):
            return CriticResult(False, 0.2, "Sample size is too small for the selected statistical method.", "Use descriptive stats or gather more data.", "medium")
        return CriticResult(True, 0.8, "Statistical method selection is reasonable for the sample.", "", "low")


class StoryCritic:
    def evaluate(self, story: Dict[str, Any], audience: str) -> CriticResult:
        required = ["executive_summary", "key_findings", "recommended_actions", "caveats"]
        missing = [item for item in required if not str(story.get(item, "")).strip()]
        if missing:
            return CriticResult(False, 0.3, f"Story is missing sections: {', '.join(missing)}.", "Fill the missing narrative sections.", "medium")
        if audience == "stakeholder" and not str(story.get("so_what", "")).strip():
            return CriticResult(False, 0.45, "Stakeholder story is missing the so-what.", "Add consequence and decision framing.", "medium")
        return CriticResult(True, 0.9, "Story answers the audience question.", "", "low")


class UIUXCritic:
    def evaluate(self, ui_spec: Dict[str, Any]) -> CriticResult:
        has_chat = bool(ui_spec.get("chat_workspace"))
        has_canvas = bool(ui_spec.get("canvas_panel"))
        if not (has_chat and has_canvas):
            return CriticResult(False, 0.2, "UI spec is missing core chat/canvas structure.", "Add chat-first layout and progressive canvas.", "high")
        return CriticResult(True, 0.88, "UI spec matches commercial assistant structure.", "", "low")


class PresentationCritic:
    def evaluate(self, outline: Dict[str, Any]) -> CriticResult:
        slides = outline.get("slides", []) if isinstance(outline.get("slides"), list) else []
        if len(slides) < 5:
            return CriticResult(False, 0.25, "Presentation outline is too thin for executive use.", "Add summary, findings, recommendations, and appendix slides.", "medium")
        return CriticResult(True, 0.86, "Presentation outline is executive-ready for a first pass.", "", "low")


class CompletionCritic:
    def evaluate(self, requested_outputs: Iterable[str], artifacts: Dict[str, Any], validation_status: str = "") -> CriticResult:
        requested = [str(x) for x in requested_outputs]
        artifact_keys = {str(k) for k in (artifacts or {}).keys()}
        missing: List[str] = []
        mapping = {
            "spreadsheet": {"spreadsheet", "workbook_xlsx", "decision_matrix_csv", "outreach_candidates_csv"},
            "report": {"report_md", "summary_report_md", "workspace_readme_md", "recommendation_md", "document_md", "executive_summary_md"},
            "executive_summary": {"executive_summary_md", "summary_report_md", "report_md", "document_md"},
            "dashboard": {"dashboard_html", "payer_dashboard_html", "shopping_dashboard_html"},
            "presentation": {"presentation_md", "slides_md", "powerpoint_pptx"},
            "rag_index": {"rag_index_db"},
            "code": {"analysis_script_py"},
            "ui": {"ui_spec_json", "dashboard_html"},
        }
        for item in requested:
            expected = mapping.get(item, {item})
            if not artifact_keys.intersection(expected):
                missing.append(item)
        if validation_status and validation_status not in {"passed", "ok", "valid"}:
            return CriticResult(False, 0.35, f"Validation status is {validation_status}.", "Resolve validation failures before completion.", "high")
        if missing:
            return CriticResult(False, 0.4, f"Missing requested outputs: {', '.join(missing)}.", "Generate the missing deliverables.", "medium")
        return CriticResult(True, 0.92, "Requested outputs are present.", "", "low")
