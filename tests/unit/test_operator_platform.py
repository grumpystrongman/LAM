import json
import shutil
import unittest
from pathlib import Path

from lam.operator_platform import (
    ArtifactFactory,
    ArtifactContaminationValidator,
    CapabilityPlanner,
    CompletionCritic,
    DataQualityCritic,
    ExecutionGraphRuntime,
    FinalOutputGate,
    GeographyValidator,
    MemoryStore,
    PresentationCritic,
    ServiceScopeValidator,
    SourceRelevanceValidator,
    SourceCritic,
    StoryCritic,
    TaskContractEngine,
    UIUXCritic,
    build_platform_cards,
    build_presentation_outline,
    build_story_package,
    build_ui_delivery,
    chart_recommendation,
    cohort_group_comparison,
    correlation_analysis,
    data_profile,
    default_capability_registry,
    descriptive_statistics,
    detect_outliers,
    default_executors,
    generate_chart_spec,
    insight_generation,
    missing_value_report,
    simple_regression,
    trend_analysis,
)


class TestOperatorPlatform(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path("data") / "test_artifacts" / "operator_platform_case"
        shutil.rmtree(self.root, ignore_errors=True)
        self.root.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def test_task_contract_extraction(self) -> None:
        contract = TaskContractEngine().extract(
            "Build a payer pricing package for Fairfax, VA with a RAG index, spreadsheet, report, and stakeholder deck."
        )
        self.assertEqual(contract.geography, "Fairfax, VA")
        self.assertEqual(contract.domain, "payer_pricing_review")
        self.assertIn("spreadsheet", contract.requested_outputs)
        self.assertIn("presentation", contract.requested_outputs)
        self.assertEqual(contract.invalidation_keys["geography"], "Fairfax, VA")

    def test_stale_artifact_rejection(self) -> None:
        engine = TaskContractEngine()
        contract = engine.extract("Build a payer pricing package for Fairfax, VA.")
        stale = {
            "task_contract": {
                "invalidation_keys": {
                    "geography": "Durham, NC",
                    "domain": contract.domain,
                    "timeframe": contract.timeframe,
                    "audience": contract.audience,
                    "outputs": contract.invalidation_keys["outputs"],
                }
            }
        }
        self.assertFalse(engine.artifact_matches_contract(stale, contract))

    def test_capability_registry_lookup_and_planner(self) -> None:
        registry = default_capability_registry()
        self.assertTrue(registry.has("deep_research"))
        contract = TaskContractEngine().extract(
            "Research public hospital pricing data, build a RAG model, write and test the code, build a report and presentation."
        )
        self.assertEqual(contract.domain, "deep_analysis")
        graph = CapabilityPlanner(registry=registry).plan(contract)
        capability_names = [node.capability for node in graph.nodes]
        self.assertIn("deep_research", capability_names)
        self.assertIn("rag_build", capability_names)
        self.assertIn("code_write", capability_names)
        self.assertIn("presentation_build", capability_names)

    def test_critics(self) -> None:
        self.assertFalse(SourceCritic().evaluate([]).passed)
        self.assertFalse(DataQualityCritic().evaluate(10, 0.5).passed)
        self.assertFalse(StoryCritic().evaluate({"executive_summary": ""}, "stakeholder").passed)
        self.assertTrue(UIUXCritic().evaluate({"chat_workspace": True, "canvas_panel": True}).passed)
        self.assertTrue(PresentationCritic().evaluate({"slides": [{"title": str(i)} for i in range(6)]}).passed)
        self.assertFalse(CompletionCritic().evaluate(["spreadsheet"], {}).passed)

    def test_artifact_factory_metadata_validation(self) -> None:
        artifact_dir = self.root / "artifacts"
        artifact_dir.mkdir(parents=True, exist_ok=True)
        report_path = artifact_dir / "summary_report.md"
        report_path.write_text("# report\n", encoding="utf-8")
        manifest = ArtifactFactory(manifests_root=self.root / "manifests").write_manifest(
            task_id="task123",
            task_contract=TaskContractEngine().extract("Create a report.").to_dict(),
            artifacts={"summary_report_md": str(report_path.resolve())},
            generated_by_capabilities=["report_build"],
            validation_status="passed",
            source_data=["sample://note"],
        )
        ok, errors = ArtifactFactory().validate_manifest(manifest)
        self.assertTrue(ok, errors)

    def test_memory_store_round_trip(self) -> None:
        store = MemoryStore(path=self.root / "memory.db")
        store.put("prefs", "writing_style", {"tone": "concise"})
        self.assertEqual(store.get("prefs", "writing_style")["tone"], "concise")
        store.remember_artifact(
            task_id="task1",
            path="C:\\tmp\\artifact.md",
            domain="deep_analysis",
            geography="",
            invalidation_key="abc",
            status="created",
            metadata={"artifact_key": "report"},
        )
        self.assertEqual(len(store.recent_artifacts("abc")), 1)
        memory_id = store.save_memory(
            {
                "type": "project_context",
                "scope": "project",
                "project_id": "proj1",
                "content": {"note": "Fairfax payer package"},
                "tags": ["payer", "fairfax"],
                "source": "unit_test",
                "confidence": 0.8,
                "retrieval_policy": "strict",
                "invalidation_keys": {"geography": "Fairfax, VA", "domain": "payer_pricing_review"},
            }
        )
        retrieved = store.retrieve_relevant_memory(
            task_contract=TaskContractEngine().extract("Build a payer pricing package for Fairfax, VA.").to_dict(),
            query="Fairfax payer package",
            limit=5,
            project_id="proj1",
        )
        self.assertTrue(any(item.get("memory_id") == memory_id for item in retrieved.get("used", [])))
        rejected = store.retrieve_relevant_memory(
            task_contract=TaskContractEngine().extract("Build a payer pricing package for Durham, NC.").to_dict(),
            query="Durham payer package",
            limit=5,
            project_id="proj1",
        )
        self.assertTrue(any("conflicts on geography" in item.get("reason", "") for item in rejected.get("rejected", [])))

    def test_data_science_functions(self) -> None:
        rows = [
            {"month": 1, "value": 10, "group": "A", "other": 1},
            {"month": 2, "value": 12, "group": "A", "other": 2},
            {"month": 3, "value": 60, "group": "B", "other": 3},
            {"month": 4, "value": 14, "group": "B", "other": 4},
        ]
        profile = data_profile(rows)
        missing = missing_value_report(rows)
        stats = descriptive_statistics(rows, ["value", "other"])
        outliers = detect_outliers(rows, "value")
        corr = correlation_analysis(rows, "month", "value")
        trend = trend_analysis(rows, "month", "value")
        cohort = cohort_group_comparison(rows, "group", "value")
        regression = simple_regression(rows, "month", "value")
        chart = chart_recommendation(len(rows), profile["fields"])
        spec = generate_chart_spec(chart["chart_type"], "month", "value", "Values by month")
        insights = insight_generation(profile, stats, outliers)
        self.assertEqual(profile["row_count"], 4)
        self.assertIn("value", missing["missing_by_field"])
        self.assertIn("value", stats)
        self.assertGreaterEqual(corr["sample_size"], 2)
        self.assertIn(trend["direction"], {"up", "down", "flat"})
        self.assertIn("A", cohort)
        self.assertGreaterEqual(regression["sample_size"], 2)
        self.assertIn("chart_type", chart)
        self.assertEqual(spec["x_field"], "month")
        self.assertTrue(insights)

    def test_storytelling_presentation_and_ui_build(self) -> None:
        contract = TaskContractEngine().extract("Build an executive analysis package with slides and a dashboard.")
        story = build_story_package(
            contract.to_dict(),
            {
                "insights": ["Median spend is rising.", "One cohort is an outlier."],
                "findings": ["Median spend is rising.", "One cohort is an outlier."],
                "recommended_actions": ["Validate the outlier cohort.", "Review pricing assumptions."],
                "caveats": ["Sample size is limited."],
            },
        )
        outline = build_presentation_outline(contract.to_dict(), story)
        ui = build_ui_delivery({"goal": "chat-first assistant"})
        self.assertTrue(StoryCritic().evaluate(story, contract.audience).passed)
        self.assertTrue(PresentationCritic().evaluate(outline).passed)
        self.assertTrue(UIUXCritic().evaluate(ui).passed)
        self.assertGreaterEqual(len(outline["slides"]), 5)

    def test_competitor_package_exports_through_executor(self) -> None:
        executors = default_executors()
        context = {
            "task_contract": {
                "user_goal": "Research the top competitors to Epic Systems and build stakeholder artifacts.",
                "domain": "competitor_analysis",
                "audience": "stakeholder",
                "requested_outputs": ["report", "presentation", "dashboard", "spreadsheet"],
            },
            "workspace_dir": str(self.root / "competitor_workspace"),
        }
        export = executors["artifact_export"].execute(
            context,
            {
                "structured_rows": [
                    {"rank": 1, "name": "Oracle Health (Cerner)", "segment": "Enterprise EHR", "why": "Large enterprise overlap.", "citations": "https://www.oracle.com/health/", "score": 0.95},
                    {"rank": 2, "name": "MEDITECH", "segment": "Enterprise EHR", "why": "Hospital presence.", "citations": "https://ehr.meditech.com/", "score": 0.88},
                ],
                "research_notes": {"target": "Epic Systems", "summary": "Prepared competitor landscape."},
                "story_package": {
                    "executive_summary": "Epic faces concentrated enterprise competition.",
                    "key_findings": ["Oracle Health and MEDITECH appear consistently."],
                    "recommended_actions": ["Validate shortlist with segment-specific criteria."],
                    "caveats": ["Use live market diligence before executive circulation."],
                    "so_what": "This narrows the diligence set for leadership.",
                },
                "report": {"markdown": "# Executive Summary\nEpic faces concentrated enterprise competition.\n"},
                "presentation": {"slides": [{"title": "Executive Summary", "bullets": ["Epic faces concentrated enterprise competition."]}]},
            },
        )
        self.assertIn("competitors_csv", export.artifacts)
        self.assertIn("executive_summary_md", export.artifacts)
        self.assertIn("powerpoint_pptx", export.artifacts)
        self.assertIn("dashboard_html", export.artifacts)
        self.assertIn("dashboard_html", export.artifact_metadata)

    def test_execution_graph_runtime_runs_and_revises(self) -> None:
        contract = TaskContractEngine().extract("Build an executive summary report and slides for stakeholders.")
        graph = CapabilityPlanner().plan(contract)
        runtime = ExecutionGraphRuntime(
            memory_store=MemoryStore(path=self.root / "runtime_memory.db"),
            artifact_factory=ArtifactFactory(manifests_root=self.root / "runtime_manifests"),
        )
        result = runtime.run(graph, {"task_contract": contract.to_dict(), "workspace_dir": str(self.root / "runtime_workspace")})
        self.assertTrue(result.ok)
        self.assertTrue(result.artifacts)
        self.assertTrue(result.verification_report.get("final_verification") == "passed")
        self.assertTrue(result.artifact_metadata)
        self.assertTrue(any(item.get("validation_history") for item in result.artifact_metadata.values() if isinstance(item, dict)))
        self.assertTrue(any(evt.get("event") == "graph_started" for evt in result.events))
        self.assertTrue(any(evt.get("event") == "revision_started" for evt in result.events))
        self.assertTrue(any(node.status in {"revised", "succeeded"} for node in result.graph.nodes))

    def test_build_platform_cards(self) -> None:
        payload = {
            "task_contract": TaskContractEngine().extract("Build a report for Fairfax, VA stakeholders.").to_dict(),
            "artifacts": {"summary_report_md": "C:\\temp\\summary_report.md"},
            "artifact_manifest": {"validation_status": "passed", "created_at": "2026-04-26T12:00:00", "source_data": ["user://instruction"]},
            "critics": {"platform": {"story": {"passed": True, "score": 0.9, "reason": "ok", "required_fix": ""}}},
            "capability_execution_graph": {"status": "succeeded", "nodes": [{"node_id": "n01", "capability": "report_build", "status": "succeeded", "attempts": 1}], "events": [{"event": "graph_started"}]},
            "memory_context": {"used": [{"type": "project_context", "content": {"note": "Use concise style"}}], "rejected": [], "retrieval_confidence": 0.9},
            "revisions_performed": [{"critic": "story", "required_fix": "Add so-what"}],
            "runtime_events": [
                {"event": "node_started", "node_id": "n01_report_build", "capability": "report_build", "status": "running"},
                {"event": "critic_failed", "node_id": "n01_report_build", "capability": "report_build", "critic": "story"},
                {"event": "revision_completed", "node_id": "n01_report_build", "capability": "report_build", "critic": "story", "status": "revised"},
            ],
        }
        cards = build_platform_cards(payload)
        self.assertIn("task_contract", cards)
        self.assertIn("artifact_manifest", cards)
        self.assertIn("items", cards["artifact_manifest"])
        self.assertIn("critic_results", cards)
        self.assertIn("execution_graph", cards)
        self.assertIn("runtime_timeline", cards)
        self.assertTrue(cards["runtime_timeline"]["groups"])
        self.assertIn("memory_context", cards)
        self.assertIn("validation", cards)

    def test_geography_validator_blocks_fairfax_report_with_nc_sources(self) -> None:
        contract = {
            "geography": "Fairfax, VA",
            "service_focus": "outpatient imaging",
            "invalid_geography_terms": ["Duke University Hospital", "WakeMed", "UNC Health", "Blue Cross NC", "North Carolina", "Durham", "Raleigh", "Cary"],
        }
        source_rows = [
            {"source_name": "Duke University Hospital standard charges", "geography": "Durham, NC", "source_url_or_path": "https://duke.example"},
            {"source_name": "WakeMed Cary Hospital standard charges", "geography": "Raleigh-Durham, NC", "source_url_or_path": "https://wakemed.example"},
        ]
        geography = GeographyValidator().validate(contract=contract, artifacts={}, source_rows=source_rows)
        gate = FinalOutputGate().evaluate(validation_results=[geography], completion_passed=False, required_artifacts_exist=False)
        self.assertFalse(geography.passed)
        self.assertFalse(gate.passed)
        self.assertIn("GeographyValidator", gate.blocking_failures)

    def test_service_scope_validator_filters_non_imaging_rows(self) -> None:
        contract = {"service_focus": "outpatient imaging"}
        candidates = [
            {"service": "MRI brain without contrast"},
            {"service": "Heart transplant"},
            {"service": "CABG video-assisted vein harvest"},
            {"service": "CT abdomen and pelvis with contrast"},
        ]
        validator = ServiceScopeValidator()
        filtered, removed = validator.filter_candidate_rows(contract=contract, candidate_rows=candidates)
        result = validator.validate(contract=contract, candidate_rows=candidates, artifact_paths={})
        self.assertFalse(result.passed)
        self.assertEqual(len(filtered), 2)
        self.assertEqual(len(removed), 2)
        self.assertTrue(all("mri" in row["service"].lower() or "ct" in row["service"].lower() for row in filtered))

    def test_artifact_contamination_validator_quarantines_conflicting_report(self) -> None:
        report = self.root / "fairfax_report.md"
        report.write_text("# Fairfax, VA Payer Pricing Review\n\nSources: Duke University Hospital, WakeMed, UNC Health.\n", encoding="utf-8")
        contract = {
            "geography": "Fairfax, VA",
            "service_focus": "outpatient imaging",
            "invalid_geography_terms": ["Duke University Hospital", "WakeMed", "UNC Health", "North Carolina", "Durham"],
        }
        geography = GeographyValidator().validate(contract=contract, artifacts={"summary_report_md": str(report)}, source_rows=[])
        service = ServiceScopeValidator().validate(contract=contract, candidate_rows=[], artifact_paths={"summary_report_md": str(report)})
        source = SourceRelevanceValidator().validate(contract=contract, source_rows=[])
        artifact = ArtifactContaminationValidator().validate(
            contract=contract,
            artifacts={"summary_report_md": str(report)},
            geography_result=geography,
            service_result=service,
            source_result=source,
        )
        self.assertFalse(artifact.passed)
        self.assertIn("summary_report_md", artifact.metadata.get("invalid_artifacts", []))


if __name__ == "__main__":
    unittest.main()
