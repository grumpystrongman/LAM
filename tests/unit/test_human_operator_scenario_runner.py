import shutil
import unittest
from pathlib import Path

from lam.interface.human_operator_scenario_runner import (
    run_human_operator_20_suite,
    run_human_operator_killer_suite,
)


class TestHumanOperatorScenarioRunner(unittest.TestCase):
    def _case_dir(self, name: str) -> Path:
        root = Path("data") / "test_artifacts" / "human_operator_scenario_runner"
        root.mkdir(parents=True, exist_ok=True)
        path = root / name
        shutil.rmtree(path, ignore_errors=True)
        path.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(path, ignore_errors=True))
        return path

    def test_run_20_suite_passes_all(self) -> None:
        case_dir = self._case_dir("core20")
        out = run_human_operator_20_suite(
            scenarios_path=Path("config/human_operator_scenarios.json"),
            artifacts_root=case_dir / "suite",
            stop_on_fail=True,
        )
        self.assertTrue(Path(out["report_path"]).exists())
        self.assertTrue(out["ok"])
        self.assertEqual(out.get("suite"), "core20")
        self.assertEqual(out["summary"]["total_planned"], 20)
        self.assertEqual(out["summary"]["executed"], 20)
        self.assertEqual(out["summary"]["failed"], 0)

    def test_run_killer_suite_passes_all(self) -> None:
        case_dir = self._case_dir("killer5")
        out = run_human_operator_killer_suite(
            scenarios_path=Path("config/human_operator_scenarios.json"),
            artifacts_root=case_dir / "suite",
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
