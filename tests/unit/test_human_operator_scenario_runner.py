import tempfile
import unittest
from pathlib import Path

from lam.interface.human_operator_scenario_runner import (
    run_human_operator_20_suite,
    run_human_operator_killer_suite,
)


class TestHumanOperatorScenarioRunner(unittest.TestCase):
    def test_run_20_suite_passes_all(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = run_human_operator_20_suite(
                scenarios_path=Path("config/human_operator_scenarios.json"),
                artifacts_root=Path(td) / "suite",
                stop_on_fail=True,
            )
            self.assertTrue(Path(out["report_path"]).exists())
        self.assertTrue(out["ok"])
        self.assertEqual(out.get("suite"), "core20")
        self.assertEqual(out["summary"]["total_planned"], 20)
        self.assertEqual(out["summary"]["executed"], 20)
        self.assertEqual(out["summary"]["failed"], 0)

    def test_run_killer_suite_passes_all(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = run_human_operator_killer_suite(
                scenarios_path=Path("config/human_operator_scenarios.json"),
                artifacts_root=Path(td) / "suite",
                stop_on_fail=True,
            )
            self.assertTrue(Path(out["report_path"]).exists())
        self.assertTrue(out["ok"])
        self.assertEqual(out.get("suite"), "killer5")
        self.assertEqual(out["summary"]["total_planned"], 5)
        self.assertEqual(out["summary"]["executed"], 5)
        self.assertEqual(out["summary"]["failed"], 0)


if __name__ == "__main__":
    unittest.main()
