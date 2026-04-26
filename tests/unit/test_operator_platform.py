import json
import shutil
import unittest
from pathlib import Path

from lam.operator_platform import (
    ArtifactFactory,
    CapabilityPlanner,
    CompletionCritic,
    DataQualityCritic,
    MemoryStore,
    PresentationCritic,
    SourceCritic,
    StoryCritic,
    TaskContractEngine,
    UIUXCritic,
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


if __name__ == "__main__":
    unittest.main()
