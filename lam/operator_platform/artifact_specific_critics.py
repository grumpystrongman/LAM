from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List


@dataclass(slots=True)
class ArtifactCriticResult:
    passed: bool
    score: float
    missing_elements: List[str] = field(default_factory=list)
    weak_sections: List[str] = field(default_factory=list)
    unsupported_claims: List[str] = field(default_factory=list)
    revision_instructions: List[str] = field(default_factory=list)
    severity: str = "low"
    auto_repair_allowed: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class ResearchQualityCritic:
    def evaluate(self, research_strategy: Dict[str, Any], evidence_map: Dict[str, Any]) -> ArtifactCriticResult:
        missing: List[str] = []
        if not list(research_strategy.get("research_questions", []) or []):
            missing.append("research_questions")
        if int((evidence_map.get("summary", {}) or {}).get("accepted_count", 0) or 0) <= 0:
            missing.append("accepted_sources")
        passed = not missing
        return ArtifactCriticResult(
            passed=passed,
            score=0.88 if passed else 0.35,
            missing_elements=missing,
            revision_instructions=["Add accepted evidence and explicit research questions."] if missing else [],
            severity="medium" if missing else "low",
        )


class SourceCredibilityCritic:
    def evaluate(self, evidence_map: Dict[str, Any]) -> ArtifactCriticResult:
        summary = dict(evidence_map.get("summary", {}) or {})
        accepted = int(summary.get("accepted_count", 0) or 0)
        avg = float(summary.get("average_confidence", 0.0) or 0.0)
        passed = accepted > 0 and avg >= 0.55
        return ArtifactCriticResult(
            passed=passed,
            score=min(1.0, avg),
            missing_elements=[] if accepted > 0 else ["credible_evidence"],
            revision_instructions=["Replace weak sources with primary or more specific sources."] if not passed else [],
            severity="high" if not passed else "low",
        )


class ResumeCritic:
    def evaluate(self, content: str) -> ArtifactCriticResult:
        missing = [item for item in ["Summary", "Experience", "Impact Highlights", "Role Fit"] if item.lower() not in content.lower()]
        passed = not missing and len(content.split()) >= 120
        return ArtifactCriticResult(
            passed=passed,
            score=0.9 if passed else 0.45,
            missing_elements=missing,
            weak_sections=["resume_too_generic"] if "tailored" not in content.lower() and not passed else [],
            revision_instructions=["Add role-fit framing and quantified impact highlights."] if not passed else [],
            severity="medium" if not passed else "low",
        )


class CoverLetterCritic:
    def evaluate(self, content: str) -> ArtifactCriticResult:
        missing = [item for item in ["Why this role", "Why me", "Next step"] if item.lower() not in content.lower()]
        passed = not missing and len(content.split()) >= 100
        return ArtifactCriticResult(
            passed=passed,
            score=0.88 if passed else 0.4,
            missing_elements=missing,
            revision_instructions=["Make the letter specific to the role and end with a clear next-step close."] if not passed else [],
            severity="medium" if not passed else "low",
        )


class GrantProposalCritic:
    def evaluate(self, content: str) -> ArtifactCriticResult:
        missing = [item for item in ["Need", "Approach", "Budget", "Outcomes", "Eligibility"] if item.lower() not in content.lower()]
        passed = not missing and len(content.split()) >= 180
        return ArtifactCriticResult(
            passed=passed,
            score=0.9 if passed else 0.35,
            missing_elements=missing,
            revision_instructions=["Add funder-fit sections, budget framing, and measurable outcomes."] if not passed else [],
            severity="high" if not passed else "low",
        )


class JobFitCritic:
    def evaluate(self, rows: List[Dict[str, Any]]) -> ArtifactCriticResult:
        passed = len(rows) >= 2 and all("fit_score" in row for row in rows)
        return ArtifactCriticResult(
            passed=passed,
            score=0.85 if passed else 0.3,
            missing_elements=[] if passed else ["fit_scores"],
            revision_instructions=["Add explicit job-fit scoring and rationale per opportunity."] if not passed else [],
            severity="medium" if not passed else "low",
        )


class ExecutiveBriefCritic:
    def evaluate(self, content: str) -> ArtifactCriticResult:
        missing = [item for item in ["Executive Summary", "Key Findings", "Recommendations", "Caveats"] if item.lower() not in content.lower()]
        passed = not missing and "so what" in content.lower()
        return ArtifactCriticResult(
            passed=passed,
            score=0.92 if passed else 0.42,
            missing_elements=missing,
            weak_sections=["missing_so_what"] if "so what" not in content.lower() else [],
            revision_instructions=["Add a direct so-what and decision-oriented recommendations."] if not passed else [],
            severity="high" if not passed else "low",
        )


class DataStoryCritic:
    def evaluate(self, content: str) -> ArtifactCriticResult:
        missing = [item for item in ["Story", "Evidence", "Caveats", "Recommended Actions"] if item.lower() not in content.lower()]
        passed = not missing
        return ArtifactCriticResult(
            passed=passed,
            score=0.9 if passed else 0.4,
            missing_elements=missing,
            revision_instructions=["Tie findings to a single narrative and include caveats."] if not passed else [],
            severity="medium" if not passed else "low",
        )


class PresentationCritic:
    def evaluate(self, content: str) -> ArtifactCriticResult:
        missing = [item for item in ["Title Slide", "Executive Summary", "Findings", "Recommendations", "Appendix"] if item.lower() not in content.lower()]
        passed = not missing
        return ArtifactCriticResult(
            passed=passed,
            score=0.88 if passed else 0.38,
            missing_elements=missing,
            revision_instructions=["Expand the deck outline to include executive flow and appendix support."] if not passed else [],
            severity="medium" if not passed else "low",
        )


class UIUXCritic:
    def evaluate(self, content: str) -> ArtifactCriticResult:
        low = content.lower()
        ui_spec_pass = all(token in low for token in ["information architecture", "chat", "canvas", "artifact viewer"])
        dashboard_pass = all(token in low for token in ["role ranking", "fit score", "top opportunities", "next actions"])
        passed = ui_spec_pass or dashboard_pass
        return ArtifactCriticResult(
            passed=passed,
            score=0.9 if passed else 0.35,
            missing_elements=[] if passed else ["commercial_ui_structure"],
            revision_instructions=["Add information architecture, clear hierarchy, top opportunities, fit scores, and next-action guidance."] if not passed else [],
            severity="medium" if not passed else "low",
        )


class StatisticalAnalysisCritic:
    def evaluate(self, content: str) -> ArtifactCriticResult:
        passed = all(token in content.lower() for token in ["descriptive statistics", "outlier", "caveat"])
        return ArtifactCriticResult(
            passed=passed,
            score=0.86 if passed else 0.32,
            missing_elements=[] if passed else ["analysis_method_sections"],
            revision_instructions=["Document descriptive stats, outlier logic, and caveats explicitly."] if not passed else [],
            severity="medium" if not passed else "low",
        )


class CompletionCritic:
    def evaluate(self, artifact_plan: List[Dict[str, Any]], artifacts: Dict[str, str]) -> ArtifactCriticResult:
        expected = [str(item.get("name", "")) for item in artifact_plan if item.get("name")]
        artifact_keys = set(artifacts.keys())
        suffixes = ["_md", "_csv", "_html", "_xlsx", "_json", "_py"]
        missing = [name for name in expected if name not in artifact_keys and not any(f"{name}{suffix}" in artifact_keys for suffix in suffixes)]
        passed = not missing
        return ArtifactCriticResult(
            passed=passed,
            score=0.94 if passed else 0.4,
            missing_elements=missing,
            revision_instructions=["Generate the missing planned artifacts."] if missing else [],
            severity="high" if missing else "low",
        )
