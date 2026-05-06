from __future__ import annotations

from typing import Any, Dict, List

from .mission_contract import MissionContract


class ResearchStrategist:
    def build(self, contract: MissionContract) -> Dict[str, Any]:
        mission_type = contract.mission_type
        outputs = list(contract.requested_outputs)
        base = {
            "mission_type": mission_type,
            "deliverable_mode": contract.deliverable_mode,
            "research_questions": self._research_questions(contract),
            "source_categories": self._source_categories(contract),
            "search_paths": self._search_paths(contract),
            "source_priority": self._source_priority(contract),
            "expected_evidence": self._expected_evidence(contract),
            "minimum_evidence_threshold": self._minimum_threshold(contract),
            "fallback_source_strategy": self._fallbacks(contract),
            "stopping_criteria": self._stopping_criteria(contract, outputs),
        }
        return base

    def candidate_queries(self, contract: MissionContract, limit: int = 4) -> List[str]:
        scope = contract.scope_dimensions
        location = str(scope.get("location", "")).strip()
        queries: List[str] = [contract.user_goal]
        if contract.mission_type == "job_search_package":
            base = "senior data ai leadership jobs"
            if location:
                queries.extend([f"{base} {location}", f"site:linkedin.com/jobs {base} {location}", f"site:indeed.com {base} {location}"])
            else:
                queries.extend([base, f"{base} remote"])
        elif contract.mission_type == "grant_application_package":
            base = f"{str(scope.get('domain', contract.domain)).replace('_', ' ')} grant"
            queries.extend([base, f"{base} eligibility", f"{base} deadline"])
        elif contract.mission_type == "executive_research_brief":
            queries.extend([contract.user_goal, f"{contract.user_goal} market size", f"{contract.user_goal} competitors"])
        elif contract.mission_type == "data_storytelling":
            queries.extend([contract.user_goal, "dataset data dictionary", "analysis caveats"])
        elif contract.mission_type == "ui_build":
            queries.extend([contract.user_goal, "commercial assistant UI patterns", "chat canvas artifact viewer"])
        else:
            queries.extend(self._research_questions(contract)[:2])
        out: List[str] = []
        seen: set[str] = set()
        for item in queries:
            value = " ".join(str(item or "").split()).strip()
            if not value:
                continue
            key = value.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(value)
            if len(out) >= max(1, int(limit)):
                break
        return out

    def _research_questions(self, contract: MissionContract) -> List[str]:
        mission_type = contract.mission_type
        if mission_type == "job_search_package":
            return [
                "Which roles match the user's seniority and domain background?",
                "What requirements recur across the strongest roles?",
                "Which resume evidence should be emphasized for the top roles?",
            ]
        if mission_type == "grant_application_package":
            return [
                "Which funding opportunities fit the project and eligibility constraints?",
                "What funder priorities and deadlines matter most?",
                "What proposal claims need direct source support?",
            ]
        if mission_type == "executive_research_brief":
            return [
                "What are the main market facts leadership needs?",
                "Which claims are primary-source supported?",
                "What decision options follow from the evidence?",
            ]
        if mission_type == "data_storytelling":
            return [
                "What is the strongest defensible story in the data?",
                "What caveats or data-quality limits affect that story?",
                "Which visuals best communicate the decision implications?",
            ]
        if mission_type == "ui_build":
            return [
                "What user flows matter most?",
                "What interface structure best supports the mission?",
                "What quality bar must the UI meet for commercial use?",
            ]
        return [
            "What is the user actually trying to accomplish?",
            "What evidence is needed to support a useful answer?",
            "What artifact package best satisfies the request?",
        ]

    def _source_categories(self, contract: MissionContract) -> List[str]:
        mission_type = contract.mission_type
        if mission_type == "job_search_package":
            return ["job_boards", "company_career_sites", "salary_sources", "company_background", "user_resume_facts"]
        if mission_type == "grant_application_package":
            return ["grant_portals", "foundation_sites", "eligibility_docs", "deadline_sources", "prior_award_examples"]
        if mission_type == "executive_research_brief":
            return ["official_sources", "company_sites", "public_datasets", "industry_commentary", "competitor_materials"]
        if mission_type == "data_storytelling":
            return ["dataset", "data_dictionary", "quality_notes", "analysis_outputs", "prior_stakeholder_context"]
        if mission_type == "ui_build":
            return ["product_requirements", "existing_ui_patterns", "artifact_viewer_requirements", "commercial_quality_examples"]
        return ["user_instruction", "local_files", "public_sources", "project_memory"]

    def _search_paths(self, contract: MissionContract) -> List[str]:
        scope = contract.scope_dimensions
        paths = []
        if scope.get("location"):
            paths.append(f"location_scoped:{scope.get('location')}")
        if scope.get("account"):
            paths.append(f"account_scoped:{scope.get('account')}")
        if scope.get("file_target"):
            paths.append(f"file_scoped:{scope.get('file_target')}")
        paths.append(f"domain:{contract.domain}")
        paths.append(f"mission:{contract.mission_type}")
        return paths

    def _source_priority(self, contract: MissionContract) -> List[str]:
        if contract.mission_type in {"job_search_package", "grant_application_package", "executive_research_brief"}:
            return ["primary", "official", "recent", "specific", "secondary"]
        return ["specific", "relevant", "recent", "supporting"]

    def _expected_evidence(self, contract: MissionContract) -> List[str]:
        if contract.mission_type == "job_search_package":
            return ["role_urls", "requirements", "company_context", "fit_reasons"]
        if contract.mission_type == "grant_application_package":
            return ["opportunity_urls", "eligibility_rules", "deadline_evidence", "priority_language"]
        if contract.mission_type == "executive_research_brief":
            return ["market_facts", "competitor_evidence", "recommendation_support"]
        if contract.mission_type == "data_storytelling":
            return ["data_quality_metrics", "analysis_findings", "chart_support", "caveats"]
        if contract.mission_type == "ui_build":
            return ["user_flow_requirements", "ui_quality_requirements", "implementation_constraints"]
        return ["supporting_notes", "artifact_requirements"]

    def _minimum_threshold(self, contract: MissionContract) -> Dict[str, Any]:
        threshold = {"min_sources": 2, "min_supported_claims": 2, "min_primary_sources": 0}
        if contract.mission_type in {"job_search_package", "grant_application_package", "executive_research_brief"}:
            threshold.update({"min_sources": 4, "min_supported_claims": 4, "min_primary_sources": 2})
        if contract.mission_type == "data_storytelling":
            threshold.update({"min_supported_claims": 3})
        return threshold

    def _fallbacks(self, contract: MissionContract) -> List[str]:
        return [
            "search broader within mission scope",
            "use project memory as template support only",
            "downgrade to partial or template package if evidence is insufficient",
        ]

    def _stopping_criteria(self, contract: MissionContract, outputs: List[str]) -> List[str]:
        return [
            "minimum evidence threshold met",
            "artifact plan can be supported by the evidence map",
            f"artifact set prepared for: {', '.join(outputs)}",
        ]
