import unittest
from pathlib import Path

from lam.interface.human_operator_benchmark import (
    RUBRIC_CATEGORIES,
    benchmark_from_last_run,
    evaluate_run_result,
    load_scenarios,
    score_scenario,
    verdict_for_total,
)


class TestHumanOperatorBenchmark(unittest.TestCase):
    def test_verdict_bands(self) -> None:
        self.assertEqual(verdict_for_total(10), "brittle_script")
        self.assertEqual(verdict_for_total(22), "weak_agent")
        self.assertEqual(verdict_for_total(30), "usable_narrow_lanes")
        self.assertEqual(verdict_for_total(40), "promising_operator")
        self.assertEqual(verdict_for_total(45), "strong_operator")
        self.assertEqual(verdict_for_total(50), "human_like")

    def test_score_normalizes_range(self) -> None:
        s = score_scenario(
            scenario_id="S",
            scenario_name="N",
            scores={"environment_awareness": 9, "truthfulness": -3},
        )
        self.assertEqual(s.scores["environment_awareness"], 4)
        self.assertEqual(s.scores["truthfulness"], 0)
        self.assertEqual(len(s.scores), len(RUBRIC_CATEGORIES))

    def test_evaluate_run_result_has_expected_keys(self) -> None:
        result = {
            "ok": True,
            "mode": "autonomous_plan_execute",
            "trace": [{"step": 0, "action": "open_app", "ok": True}],
            "source_status": {"gmail_ui": "ok"},
            "plan_contract": {"validation_status": "valid"},
            "verification_report": {"final_verification": "passed", "verification_checks": [{"name": "x", "pass": True}]},
            "anti_drift": {"has_failures": False},
            "artifacts": {"task_list_csv": "C:\\tmp\\x.csv"},
            "decision_log": ["x"],
        }
        scores = evaluate_run_result(result)
        self.assertEqual(set(scores.keys()), set(RUBRIC_CATEGORIES))
        self.assertGreaterEqual(scores["planning_quality"], 3)

    def test_benchmark_from_last_run(self) -> None:
        bench = benchmark_from_last_run(result={"ok": False, "mode": "x"})
        self.assertTrue(bench["ok"])
        self.assertEqual(bench["mode"], "human_operator_benchmark")
        self.assertIn("scenario", bench)
        self.assertIn("weights", bench)

    def test_load_scenarios(self) -> None:
        scenarios = load_scenarios(Path("config/human_operator_scenarios.json"))
        self.assertGreaterEqual(len(scenarios), 20)
        self.assertIn("scenario_id", scenarios[0])


if __name__ == "__main__":
    unittest.main()

