import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from lam.interface import web_ui


class TestWebUiReliability(unittest.TestCase):
    def test_start_reliability_suite_task_stores_result(self) -> None:
        state = web_ui.UiState()
        fake_result = {
            "ok": True,
            "mode": "reliability_suite",
            "summary": {"total": 1, "passed": 1, "failed": 0, "skipped": 0},
            "checks": [{"name": "x", "status": "pass", "ok": True, "details": "ok"}],
            "pytest": {"requested": False, "ran": False, "ok": True, "status": "skipped", "exit_code": 0, "args": [], "duration_ms": 0, "output_tail": []},
        }

        with tempfile.TemporaryDirectory() as td:
            with patch("lam.interface.web_ui.run_reliability_suite", return_value=fake_result):
                with patch("lam.interface.web_ui._history_path", return_value=Path(td) / "history.json"):
                    task_id = web_ui._start_reliability_suite_task(
                        state,
                        include_pytest=False,
                        include_desktop_smoke=False,
                        pytest_args=[],
                        pytest_timeout_seconds=60,
                    )
                    deadline = time.time() + 3
                    while time.time() < deadline:
                        with state.lock:
                            task = dict(state.tasks.get(task_id, {}))
                        if task.get("status") in {"done", "error"}:
                            break
                        time.sleep(0.05)

        self.assertEqual(task.get("status"), "done")
        self.assertEqual(task.get("result", {}).get("mode"), "reliability_suite")
        with state.lock:
            self.assertEqual(state.reliability_suite_task_id, task_id)
            self.assertEqual(state.reliability_suite_result.get("mode"), "reliability_suite")


if __name__ == "__main__":
    unittest.main()
