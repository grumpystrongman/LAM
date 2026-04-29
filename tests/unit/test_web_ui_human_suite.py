import shutil
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from lam.interface import web_ui


class TestWebUiHumanSuite(unittest.TestCase):
    def _case_dir(self, name: str) -> Path:
        root = Path("data") / "test_artifacts" / "web_ui_human_suite"
        root.mkdir(parents=True, exist_ok=True)
        path = root / name
        shutil.rmtree(path, ignore_errors=True)
        path.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(path, ignore_errors=True))
        return path

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
        case_dir = self._case_dir("core20")
        with patch("lam.interface.web_ui.run_human_operator_20_suite", return_value=fake_result):
            with patch("lam.interface.web_ui._history_path", return_value=case_dir / "history.json"):
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

    def test_html_contains_platform_card_mounts(self) -> None:
        self.assertIn('id="platformCards"', web_ui.HTML_PAGE)
        self.assertIn('id="platformCardsCanvas"', web_ui.HTML_PAGE)
        self.assertIn('id="runtimeTimelineCanvas"', web_ui.HTML_PAGE)
        self.assertIn("function renderPlatformCards", web_ui.HTML_PAGE)
        self.assertIn("function renderRuntimeTimelineCanvas", web_ui.HTML_PAGE)
        self.assertIn("function setTimelineFilter", web_ui.HTML_PAGE)
        self.assertIn("lam_timeline_filter", web_ui.HTML_PAGE)
        self.assertIn("node-only", web_ui.HTML_PAGE)
        self.assertIn("critic-only", web_ui.HTML_PAGE)
        self.assertIn("revisions-only", web_ui.HTML_PAGE)
        self.assertIn("Runtime Timeline", web_ui.HTML_PAGE)
        self.assertIn("validation_history", web_ui.HTML_PAGE)
        self.assertIn("renderInlineArtifactChips", web_ui.HTML_PAGE)
        self.assertIn('class="artifact-open"', web_ui.HTML_PAGE)
        self.assertIn("Validation", web_ui.HTML_PAGE)
        self.assertIn("final_output_gate", web_ui.HTML_PAGE)
        self.assertIn("Capture Clipboard Image", web_ui.HTML_PAGE)
        self.assertIn("function captureClipboardImageUi", web_ui.HTML_PAGE)
        self.assertIn("/api/clipboard/capture", web_ui.HTML_PAGE)

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
        case_dir = self._case_dir("killer5")
        with patch("lam.interface.web_ui.run_human_operator_killer_suite", return_value=fake_result):
            with patch("lam.interface.web_ui._history_path", return_value=case_dir / "history.json"):
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
