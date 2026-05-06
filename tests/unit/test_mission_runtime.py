import json
import shutil
import unittest
from pathlib import Path
from unittest.mock import patch

from lam.interface.search_agent import execute_instruction
from lam.operator_platform import (
    EvidenceMap,
    MissionContractEngine,
    MissionRuntime,
    ResearchStrategist,
    RevisionRuntime,
    SourceQualityScorer,
    UserProjectMemory,
    WorkProductEngine,
    build_platform_cards,
    default_capability_registry,
    default_executors,
)
from lam.operator_platform.artifact_specific_critics import ExecutiveBriefCritic
from lam.operator_platform.capability_planner import CapabilityPlanner
from lam.operator_platform.memory_store import MemoryStore
from lam.operator_platform.mission_research import collect_mission_research
from lam.operator_platform.research_strategist import ResearchStrategist
from lam.operator_platform.task_contract_engine import TaskContractEngine


class TestMissionRuntime(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path("data") / "test_artifacts" / "mission_runtime_case"
        shutil.rmtree(self.root, ignore_errors=True)
        self.root.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def test_mission_contract_job_package(self) -> None:
        contract = MissionContractEngine().extract(
            "Find 5 senior data/AI leadership roles I should apply for, build a tracker, tailor my resume to the top 2, and draft cover letters."
        )
        self.assertEqual(contract.mission_type, "job_search_package")
        self.assertEqual(contract.deliverable_mode, "application_ready_package")
        self.assertIn("resume", contract.requested_outputs)
        self.assertIn("cover_letter", contract.requested_outputs)

    def test_mission_contract_grant_package(self) -> None:
        contract = MissionContractEngine().extract(
            "Find grants for a healthcare analytics AI project, rank them, and draft the top proposal."
        )
        self.assertEqual(contract.mission_type, "grant_application_package")
        self.assertEqual(contract.deliverable_mode, "application_ready_package")
        self.assertIn("proposal", contract.requested_outputs)

    def test_research_strategist_job_package(self) -> None:
        contract = MissionContractEngine().extract(
            "Find 5 senior data/AI leadership roles I should apply for, build a tracker, tailor my resume to the top 2, and draft cover letters."
        )
        strategy = ResearchStrategist().build(contract)
        self.assertTrue(strategy["research_questions"])
        self.assertIn("job_boards", strategy["source_categories"])
        self.assertGreaterEqual(strategy["minimum_evidence_threshold"]["min_sources"], 4)
        self.assertTrue(ResearchStrategist().candidate_queries(contract))

    def test_source_quality_and_evidence_map(self) -> None:
        contract = MissionContractEngine().extract(
            "Research the market for AI desktop agents and build an executive briefing with recommendations."
        )
        scorer = SourceQualityScorer()
        scored = scorer.score(
            contract,
            {
                "source": "OpenAI",
                "source_type": "official",
                "url": "https://openai.com/research",
                "title": "Research",
                "snippet": "AI desktop agents market context",
            },
        )
        self.assertGreater(scored["overall_score"], 0.5)
        evidence = EvidenceMap()
        evidence.add_scored_source(
            contract=contract,
            source={
                "source": "OpenAI",
                "source_type": "official",
                "url": "https://openai.com/research",
                "title": "Research",
                "snippet": "AI desktop agents market context",
            },
            supported_claims=["market_facts", "recommendation_support"],
        )
        payload = evidence.to_dict()
        self.assertEqual(payload["summary"]["accepted_count"], 1)
        self.assertIn("market_facts", payload["claims"])

    def test_instruction_and_memory_are_context_only_not_evidence(self) -> None:
        contract = MissionContractEngine().extract(
            "Research the market for AI desktop agents and build an executive briefing with recommendations."
        )
        evidence = EvidenceMap()
        evidence.add_scored_source(
            contract=contract,
            source={"source": "user_instruction", "source_type": "instruction", "url": "user://instruction", "title": "Instruction", "snippet": contract.user_goal},
            supported_claims=["market_facts"],
        )
        evidence.add_scored_source(
            contract=contract,
            source={"source": "memory:resume", "source_type": "user_context", "url": "user://memory/1", "title": "Memory", "snippet": "prior context"},
            supported_claims=["market_facts"],
        )
        payload = evidence.to_dict()
        self.assertEqual(payload["summary"]["accepted_count"], 0)
        self.assertEqual(payload["summary"]["context_only_count"], 2)

    def test_work_product_engine_builds_job_artifacts(self) -> None:
        contract = MissionContractEngine().extract(
            "Find 5 senior data/AI leadership roles I should apply for, build a tracker, tailor my resume to the top 2, and draft cover letters."
        )
        strategy = ResearchStrategist().build(contract)
        evidence = EvidenceMap()
        evidence.add_scored_source(
            contract=contract,
            source={"source": "LinkedIn", "source_type": "job_board", "url": "https://linkedin.com/jobs/1", "title": "Role", "snippet": "VP Data AI role"},
            supported_claims=["role_urls", "requirements"],
        )
        artifacts, metadata = WorkProductEngine().build(
            contract=contract,
            strategy=strategy,
            evidence_map=evidence.to_dict(),
            memory_context={},
            workspace_dir=self.root / "job_package",
        )
        self.assertIn("job_tracker_csv", artifacts)
        self.assertIn("resume_md", artifacts)
        self.assertIn("cover_letter_md", artifacts)
        self.assertIn("final_package_summary_md", artifacts)
        self.assertIn("resume_md", metadata)

    def test_work_product_engine_builds_full_career_package_from_structured_roles(self) -> None:
        contract = MissionContractEngine().extract(
            "Find 10 high-quality VP / Head of Data / Chief Data & AI / Analytics executive roles that are realistic matches for me. Then produce a complete application package for the top 3 roles, build an executive briefing, and build a simple local dashboard."
        )
        strategy = ResearchStrategist().build(contract)
        evidence = EvidenceMap()
        source_records = [
            {
                "title": "VP, Data & AI",
                "company": "Impact Advisors",
                "job_url": "https://jobs.example.com/impact-vp-data-ai",
                "location": "Remote - United States",
                "remote_status": "Remote",
                "source_type": "company_site",
                "source_name": "Impact Advisors Careers",
                "source_date": "2026-05-04",
                "key_responsibilities": ["Lead healthcare data and AI strategy", "Advise executive clients on analytics modernization"],
                "required_qualifications": ["Healthcare analytics leadership", "AI strategy", "Power BI / Azure"],
                "compensation": "$265,000 - $310,000",
                "fit_rationale": "Healthcare advisory and platform modernization fit.",
                "risks_or_gaps": ["Need explicit consulting sell-through story"],
            },
            {
                "title": "VP, Clinical Data & Analytics",
                "company": "MedImpact",
                "job_url": "https://jobs.example.com/medimpact-vp-clinical-data-analytics",
                "location": "San Diego, CA / Hybrid",
                "remote_status": "Hybrid",
                "source_type": "company_site",
                "source_name": "MedImpact Careers",
                "source_date": "2026-05-03",
                "key_responsibilities": ["Own clinical analytics strategy", "Lead enterprise reporting and AI enablement"],
                "required_qualifications": ["Healthcare analytics leadership", "Clinical analytics", "Executive stakeholder management"],
                "compensation": "$250,000 - $295,000",
                "fit_rationale": "Strong healthcare relevance and analytics leadership.",
                "risks_or_gaps": ["PBM domain depth may need sharpening"],
            },
            {
                "title": "VP of Data and AI",
                "company": "Lyra Health",
                "job_url": "https://jobs.example.com/lyra-vp-data-ai",
                "location": "Remote - United States",
                "remote_status": "Remote",
                "source_type": "job_board",
                "source_name": "LinkedIn Jobs",
                "source_date": "2026-05-02",
                "key_responsibilities": ["Own data and AI strategy", "Scale platform and executive decision support"],
                "required_qualifications": ["Data and AI leadership", "Healthcare or digital health", "Platform modernization"],
                "compensation": "$300,000+",
                "fit_rationale": "High AI alignment and executive scope.",
                "risks_or_gaps": ["Behavioral health domain specifics may need framing"],
            },
        ]
        for row in source_records:
            evidence.add_scored_source(contract=contract, source=row, supported_claims=["role_urls", "requirements", "fit"])
        artifacts, metadata = WorkProductEngine().build(
            contract=contract,
            strategy=strategy,
            evidence_map=evidence.to_dict(),
            memory_context={},
            workspace_dir=self.root / "career_package",
            source_records=source_records,
            extra_context={"candidate_profile": {"candidate_name": "C. M. Jeff"}},
        )
        self.assertIn("candidate_profile_md", artifacts)
        self.assertIn("job_tracker_csv", artifacts)
        self.assertIn("source_manifest_csv", artifacts)
        self.assertIn("executive_brief_md", artifacts)
        self.assertIn("dashboard_html", artifacts)
        self.assertTrue(any(key.startswith("resume_top_1_") for key in artifacts))
        self.assertTrue(any(key.startswith("cover_letter_top_1_") for key in artifacts))
        self.assertIn("dashboard_html", metadata)

    def test_revision_runtime_repairs_executive_brief(self) -> None:
        artifact_path = self.root / "brief.md"
        artifact_path.write_text("# Executive Brief\n\n## Executive Summary\nShort summary.\n\n## Key Findings\n- One finding\n\n## Recommendations\n- One recommendation\n\n## Caveats\n- One caveat\n", encoding="utf-8")
        runtime = RevisionRuntime(engine=WorkProductEngine(), max_revisions=2)
        revision = runtime.revise_until_pass(
            artifact_key="executive_brief_md",
            artifact_path=artifact_path,
            critic_name="executive_brief",
            evaluate=ExecutiveBriefCritic().evaluate,
        )
        self.assertGreaterEqual(len(revision["history"]), 2)
        self.assertIn("executive_brief", revision["critic"])

    def test_user_project_memory_retrieval_policy(self) -> None:
        store = MemoryStore(path=self.root / "memory.db")
        memory = UserProjectMemory(store=store)
        memory.save_profile_fact(
            user_id="user1",
            fact_type="resume_fact",
            content={"achievement": "Scaled analytics operations"},
            tags=["resume", "career"],
        )
        memory.save_profile_fact(
            user_id="user1",
            fact_type="style_preference",
            content={"tone": "executive concise"},
            tags=["style"],
            sensitive=True,
        )
        contract = MissionContractEngine().extract("Find jobs and tailor my resume to the top 2 roles.")
        retrieved = memory.retrieve_for_mission(mission_contract=contract, query=contract.user_goal, project_id="user1", allow_sensitive=False)
        self.assertTrue(retrieved["used"])
        self.assertTrue(any(item["reason"] == "sensitive_memory_not_allowed" for item in retrieved["rejected"]))

    def test_build_platform_cards_for_mission_runtime(self) -> None:
        result = MissionRuntime(memory=UserProjectMemory(MemoryStore(path=self.root / "cards.db"))).run(
            "Research the market for AI desktop agents and build an executive briefing with recommendations.",
            context={"workspace_dir": str(self.root / "cards_workspace")},
        )
        cards = build_platform_cards(result)
        self.assertIn("mission_contract", cards)
        self.assertIn("research_strategy", cards)
        self.assertIn("evidence_map", cards)
        self.assertIn("artifact_plan", cards)
        self.assertIn("final_package", cards)
        self.assertIn("output_truth", cards)
        self.assertIn("recovery", cards)

    def test_mission_runtime_job_package_route(self) -> None:
        result = execute_instruction(
            "Find 5 senior data/AI leadership roles I should apply for, build a tracker, tailor my resume to the top 2, and draft cover letters.",
            control_granted=True,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "mission_runtime")
        self.assertIn("mission_contract", result)
        self.assertIn("research_strategy", result)
        self.assertIn("evidence_map", result)
        self.assertIn("artifacts", result)
        self.assertIn("ui_cards", result)
        self.assertIn(result.get("mission_status"), {"template_complete", "real_partial", "real_complete", "no_result_found_with_sufficient_search"})

    def test_mission_runtime_executive_brief_route(self) -> None:
        result = execute_instruction(
            "Research the market for AI desktop agents and build an executive briefing with recommendations.",
            control_granted=True,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "mission_runtime")
        self.assertEqual(result.get("deliverable_mode"), "executive_ready_deck")
        self.assertIn("executive_brief_md", result.get("artifacts", {}))
        self.assertIn(result.get("mission_status"), {"template_complete", "real_partial", "no_result_found_with_sufficient_search"})

    @patch("lam.interface.search_agent.platform_collect_generic_research")
    def test_execute_instruction_mission_runtime_uses_default_collector(self, mock_collect) -> None:
        mock_collect.return_value = {
            "ok": True,
            "query": "senior ai leadership jobs",
            "sources": [
                {"name": "Company Careers", "source_type": "company_site", "url": "https://example.com/careers/1", "snippet": "Senior AI role"},
                {"name": "LinkedIn Jobs", "source_type": "job_board", "url": "https://linkedin.com/jobs/view/1", "snippet": "VP AI"},
                {"name": "Indeed", "source_type": "job_board", "url": "https://indeed.com/viewjob?jk=1", "snippet": "Director Data AI"},
                {"name": "Salary Survey", "source_type": "official", "url": "https://example.org/salary", "snippet": "Compensation context"},
            ],
            "search_results": [],
        }
        result = execute_instruction(
            "Find 5 senior data/AI leadership roles I should apply for, build a tracker, tailor my resume to the top 2, and draft cover letters.",
            control_granted=True,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "mission_runtime")
        self.assertEqual(result["mission_status"], "real_complete")
        self.assertGreaterEqual(result.get("output_truth", {}).get("real_evidence_sources", 0), 4)
        self.assertGreaterEqual(mock_collect.call_count, 1)
        self.assertIn("candidate_profile_md", result.get("artifacts", {}))
        self.assertIn("source_manifest_csv", result.get("artifacts", {}))
        self.assertIn("critic_results_json", result.get("artifacts", {}))
        self.assertIn("validation_summary_md", result.get("artifacts", {}))

    def test_mission_runtime_with_collector_can_reach_real_complete(self) -> None:
        runtime = MissionRuntime(
            memory=UserProjectMemory(MemoryStore(path=self.root / "collector.db")),
            source_collector=lambda **kwargs: {
                "sources": [
                    {"name": "Company Careers", "source_type": "company_site", "url": "https://example.com/careers/1", "snippet": "Senior AI role"},
                    {"name": "LinkedIn Jobs", "source_type": "job_board", "url": "https://linkedin.com/jobs/view/1", "snippet": "VP AI"},
                    {"name": "Indeed", "source_type": "job_board", "url": "https://indeed.com/viewjob?jk=1", "snippet": "Director Data AI"},
                    {"name": "Salary Survey", "source_type": "official", "url": "https://example.org/salary", "snippet": "Compensation context"},
                ]
            },
        )
        result = runtime.run(
            "Find 5 senior data/AI leadership roles I should apply for, build a tracker, tailor my resume to the top 2, and draft cover letters.",
            context={"workspace_dir": str(self.root / "collector_workspace")},
        )
        self.assertEqual(result["mission_status"], "real_complete")
        self.assertTrue(result["output_truth"]["real_evidence_sources"] >= 4)

    def test_mission_runtime_no_result_after_attempted_collection(self) -> None:
        runtime = MissionRuntime(
            memory=UserProjectMemory(MemoryStore(path=self.root / "empty_collector.db")),
            source_collector=lambda **kwargs: {"sources": []},
        )
        result = runtime.run(
            "Research the market for AI desktop agents and build an executive briefing with recommendations.",
            context={"workspace_dir": str(self.root / "empty_collector_workspace")},
        )
        self.assertEqual(result["mission_status"], "no_result_found_with_sufficient_search")
        self.assertTrue(result["output_truth"]["attempted_collection"])

    def test_human_operator_scenarios_include_mission_benchmarks(self) -> None:
        payload = json.loads(Path("config/human_operator_scenarios.json").read_text(encoding="utf-8"))
        scenario_ids = {item["scenario_id"] for item in payload.get("scenarios", [])}
        self.assertTrue({"M1", "M2", "M3", "M4", "M5"}.issubset(scenario_ids))

    def test_mission_research_capability_registered_and_planned(self) -> None:
        registry = default_capability_registry()
        self.assertTrue(registry.has("mission_research"))
        contract = TaskContractEngine().extract("Find grants for a healthcare analytics AI project, rank them, and draft the top proposal.")
        graph = CapabilityPlanner(registry=registry).plan(contract).to_dict()
        capabilities = [str(node.get("capability", "")) for node in graph.get("nodes", [])]
        self.assertIn("mission_research", capabilities)

    def test_mission_research_executor_collects_sources(self) -> None:
        executor = default_executors()["mission_research"]
        with patch("lam.operator_platform.executors.collect_generic_research") as mock_collect:
            mock_collect.return_value = {
                "ok": True,
                "query": "healthcare analytics ai grant",
                "sources": [
                    {"name": "Grants.gov", "source_type": "grant_portal", "url": "https://grants.gov/example", "snippet": "eligibility and deadline"},
                    {"name": "Foundation", "source_type": "official", "url": "https://foundation.org/grant", "snippet": "priority areas"},
                ],
                "search_results": [],
            }
            result = executor.execute(
                {"browser_worker_mode": "local", "human_like_interaction": False},
                {"task_contract": TaskContractEngine().extract("Find grants for a healthcare analytics AI project, rank them, and draft the top proposal.").to_dict()},
            )
        self.assertIn("mission_research", result.outputs)
        self.assertGreaterEqual(len(result.outputs.get("sources", []) or []), 2)

    def test_collect_mission_research_job_specialization_filters_irrelevant_sources(self) -> None:
        mission = MissionContractEngine().extract(
            "Find 5 senior data/AI leadership roles I should apply for, build a tracker, tailor my resume to the top 2, and draft cover letters."
        )
        strategy = ResearchStrategist().build(mission)
        payload = collect_mission_research(
            mission=mission,
            strategy=strategy,
            context={},
            strategist=ResearchStrategist(),
            collector=lambda **kwargs: {
                "ok": True,
                "query": kwargs.get("query", ""),
                "sources": [
                    {"name": "LinkedIn Jobs", "source_type": "reference", "url": "https://linkedin.com/jobs/view/1", "snippet": "VP AI role"},
                    {"name": "Random Product Review", "source_type": "reference", "url": "https://example.com/reviews/widget", "snippet": "best consumer gadget"},
                ],
            },
        )
        urls = [str(item.get("url", "")) for item in payload.get("sources", [])]
        self.assertIn("https://linkedin.com/jobs/view/1", urls)
        self.assertNotIn("https://example.com/reviews/widget", urls)

    def test_collect_mission_research_grant_specialization_uses_grant_queries(self) -> None:
        mission = MissionContractEngine().extract(
            "Find grants for a healthcare analytics AI project, rank them, and draft the top proposal."
        )
        strategy = ResearchStrategist().build(mission)
        seen_queries = []
        collect_mission_research(
            mission=mission,
            strategy=strategy,
            context={},
            strategist=ResearchStrategist(),
            collector=lambda **kwargs: seen_queries.append(str(kwargs.get("query", ""))) or {"ok": True, "query": kwargs.get("query", ""), "sources": []},
        )
        joined = " | ".join(seen_queries).lower()
        self.assertIn("grants.gov", joined)
        self.assertIn("eligibility", joined)

    @patch("lam.interface.search_agent.platform_collect_generic_research")
    def test_mission_runtime_grant_route_uses_default_collector(self, mock_collect) -> None:
        mock_collect.return_value = {
            "ok": True,
            "query": "healthcare analytics ai grant",
            "sources": [
                {"name": "Grants.gov", "source_type": "grant_portal", "url": "https://grants.gov/example", "snippet": "eligibility and deadline"},
                {"name": "Foundation", "source_type": "official", "url": "https://foundation.org/grant", "snippet": "priority areas"},
                {"name": "State Portal", "source_type": "official", "url": "https://state.example/grant", "snippet": "deadline"},
                {"name": "Award Archive", "source_type": "public_dataset", "url": "https://data.example/awards", "snippet": "prior awards"},
            ],
            "search_results": [],
        }
        result = execute_instruction(
            "Find grants for a healthcare analytics AI project, rank them, and draft the top proposal.",
            control_granted=True,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "mission_runtime")
        self.assertEqual(result["mission_status"], "real_complete")
        self.assertIn("proposal_md", result.get("artifacts", {}))


if __name__ == "__main__":
    unittest.main()
