import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from lam.interface import web_ui


class TestWebUiHumanSuite(unittest.TestCase):
    def test_start_human_suite_task_stores_result(self) -> None:
        state = web_ui.UiState()
        fake_result = {
            "ok": True,
            "mode": "human_operator_20_test_suite",
            "suite": "core20",
            "summary": {"total_planned": 20, "executed": 20, "passed": 20, "failed": 0},
            "results": [],
            "report_path": "C:\\temp\\suite_result.json",
        }
        with tempfile.TemporaryDirectory() as td:
            with patch("lam.interface.web_ui.run_human_operator_20_suite", return_value=fake_result):
                with patch("lam.interface.web_ui._history_path", return_value=Path(td) / "history.json"):
                    task_id = web_ui._start_human_operator_20_suite_task(state)
                    deadline = time.time() + 3
                    while time.time() < deadline:
                        with state.lock:
                            task = dict(state.tasks.get(task_id, {}))
                        if task.get("status") in {"done", "error"}:
                            break
                        time.sleep(0.05)

        self.assertEqual(task.get("status"), "done")
        self.assertEqual(task.get("result", {}).get("mode"), "human_operator_20_test_suite")
        with state.lock:
            self.assertEqual(state.human_suite_task_id, task_id)
            self.assertEqual(state.human_suite_result.get("mode"), "human_operator_20_test_suite")

    def test_start_killer_suite_task_stores_result(self) -> None:
        state = web_ui.UiState()
        fake_result = {
            "ok": True,
            "mode": "human_operator_killer_5_suite",
            "suite": "killer5",
            "summary": {"total_planned": 5, "executed": 5, "passed": 5, "failed": 0},
            "results": [],
            "report_path": "C:\\temp\\killer_suite_result.json",
        }
        with tempfile.TemporaryDirectory() as td:
            with patch("lam.interface.web_ui.run_human_operator_killer_suite", return_value=fake_result):
                with patch("lam.interface.web_ui._history_path", return_value=Path(td) / "history.json"):
                    task_id = web_ui._start_human_operator_killer_suite_task(state)
                    deadline = time.time() + 3
                    while time.time() < deadline:
                        with state.lock:
                            task = dict(state.tasks.get(task_id, {}))
                        if task.get("status") in {"done", "error"}:
                            break
                        time.sleep(0.05)

        self.assertEqual(task.get("status"), "done")
        self.assertEqual(task.get("result", {}).get("mode"), "human_operator_killer_5_suite")
        with state.lock:
            self.assertEqual(state.killer_suite_task_id, task_id)
            self.assertEqual(state.killer_suite_result.get("mode"), "human_operator_killer_5_suite")


if __name__ == "__main__":
    unittest.main()
