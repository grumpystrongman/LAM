import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from lam.interface import web_ui


class TestWebUiPreflightAndSmoke(unittest.TestCase):
    def test_preflight_gate_requires_green_suite(self) -> None:
        state = web_ui.UiState()
        with state.lock:
            err = web_ui._preflight_gate_error_locked(state)
        self.assertIn("Run Reliability Suite", err)

        with state.lock:
            state.reliability_suite_result = {"ok": True, "finished_at": time.time()}
            err2 = web_ui._preflight_gate_error_locked(state)
        self.assertEqual(err2, "")

    def test_start_notepad_smoke_task_stores_result(self) -> None:
        state = web_ui.UiState()
        fake_result = {
            "ok": True,
            "mode": "notepad_smoke",
            "message": "Opened Notepad and typed hello world.",
            "trace": [{"step": 0, "action": "open_app", "ok": True}],
            "artifacts": {"smoke_log": "C:\\temp\\smoke.json"},
            "canvas": {"title": "Notepad Smoke Passed", "subtitle": "ok", "cards": []},
        }
        with tempfile.TemporaryDirectory() as td:
            with patch("lam.interface.web_ui._run_notepad_smoke_once", return_value=fake_result):
                with patch("lam.interface.web_ui._history_path", return_value=Path(td) / "history.json"):
                    task_id = web_ui._start_notepad_smoke_task(state)
                    deadline = time.time() + 3
                    while time.time() < deadline:
                        with state.lock:
                            task = dict(state.tasks.get(task_id, {}))
                        if task.get("status") in {"done", "error"}:
                            break
                        time.sleep(0.05)

        self.assertEqual(task.get("status"), "done")
        self.assertEqual(task.get("result", {}).get("mode"), "notepad_smoke")


if __name__ == "__main__":
    unittest.main()

