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
        self.assertIn("Mission", web_ui.HTML_PAGE)
        self.assertIn("Research Strategy", web_ui.HTML_PAGE)
        self.assertIn("Evidence Map", web_ui.HTML_PAGE)
        self.assertIn("Artifact Plan", web_ui.HTML_PAGE)
        self.assertIn("Output Truth", web_ui.HTML_PAGE)
        self.assertIn("Final Package", web_ui.HTML_PAGE)
        self.assertIn("Capture Clipboard Image", web_ui.HTML_PAGE)
        self.assertIn("function captureClipboardImageUi", web_ui.HTML_PAGE)
        self.assertIn("/api/clipboard/capture", web_ui.HTML_PAGE)
        self.assertIn("Learning Summary", web_ui.HTML_PAGE)
        self.assertIn("Replay Studio", web_ui.HTML_PAGE)
        self.assertIn("/api/teach/replay_preview", web_ui.HTML_PAGE)
        self.assertIn("/api/teach/replay_run", web_ui.HTML_PAGE)
        self.assertIn("renderTeachStudio", web_ui.HTML_PAGE)
        self.assertIn("State snapshots", web_ui.HTML_PAGE)
        self.assertIn("Success rate", web_ui.HTML_PAGE)
        self.assertIn("Stale age hrs", web_ui.HTML_PAGE)
        self.assertIn("Re-teach suggested", web_ui.HTML_PAGE)
        self.assertIn("History:", web_ui.HTML_PAGE)
        self.assertIn("Branch Timeline", web_ui.HTML_PAGE)
        self.assertIn("Checkpoint Timeline", web_ui.HTML_PAGE)
        self.assertIn("Guided Re-teach Plan", web_ui.HTML_PAGE)
        self.assertIn("Re-teach this checkpoint", web_ui.HTML_PAGE)
        self.assertIn("Variant Diff View", web_ui.HTML_PAGE)
        self.assertIn("Suggested base context", web_ui.HTML_PAGE)
        self.assertIn("Learned Skill Studio", web_ui.HTML_PAGE)
        self.assertIn("One workspace for execution, research, teach mode, and reusable skills.", web_ui.HTML_PAGE)
        self.assertIn("Command Center", web_ui.HTML_PAGE)
        self.assertIn("Operator Conversation", web_ui.HTML_PAGE)
        self.assertIn("workspaceModeStat", web_ui.HTML_PAGE)
        self.assertIn("Save New Version", web_ui.HTML_PAGE)
        self.assertIn("Practice Preview", web_ui.HTML_PAGE)
        self.assertIn("Run Safe Practice", web_ui.HTML_PAGE)
        self.assertIn("Schedule Practice", web_ui.HTML_PAGE)
        self.assertIn("Refresh Topic", web_ui.HTML_PAGE)
        self.assertIn("learnedSkillNameField", web_ui.HTML_PAGE)
        self.assertIn("Workflow Preview", web_ui.HTML_PAGE)
        self.assertIn("/api/learn/skills", web_ui.HTML_PAGE)
        self.assertIn("/api/learn/skill/load", web_ui.HTML_PAGE)
        self.assertIn("/api/learn/skill/save", web_ui.HTML_PAGE)
        self.assertIn("/api/learn/skill/diff", web_ui.HTML_PAGE)
        self.assertIn("/api/learn/skill/feedback", web_ui.HTML_PAGE)
        self.assertIn("/api/learn/skill/practice_preview", web_ui.HTML_PAGE)
        self.assertIn("/api/learn/skill/practice_run", web_ui.HTML_PAGE)
        self.assertIn("/api/learn/skill/schedule_practice", web_ui.HTML_PAGE)
        self.assertIn("/api/learn/skill/refresh", web_ui.HTML_PAGE)
        self.assertIn("/api/learn/skill/selectors", web_ui.HTML_PAGE)

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

    def test_capture_live_replay_state_uses_uia_adapter(self) -> None:
        with patch("lam.interface.web_ui.UIAAdapter") as mock_adapter_cls:
            mock_adapter = mock_adapter_cls.return_value
            mock_adapter.capture_live_state.return_value = {
                "ok": True,
                "app_name": "gmail",
                "window_title": "Gmail",
                "visible_labels": ["Compose"],
                "selector_values": ["Compose"],
                "visible_roles": ["Button"],
                "tree_signature": "Button|Compose",
            }
            state = web_ui._capture_live_replay_state("gmail")
        self.assertTrue(state["ok"])
        self.assertEqual(state["app_name"], "gmail")
        self.assertIn("Compose", state["visible_labels"])
        self.assertIn("Button", state["visible_roles"])

    def test_build_reteach_guidance_flags_same_failed_segment(self) -> None:
        guidance = web_ui._build_reteach_guidance(
            [
                {"recipe_id": "a", "ok": False, "segment_index": 2, "checkpoint_id": "commit_send", "checkpoint_name": "3. Commit - Send"},
                {"recipe_id": "b", "ok": False, "segment_index": 2, "checkpoint_id": "commit_send", "checkpoint_name": "3. Commit - Send"},
            ]
        )
        self.assertTrue(guidance["suggested"])
        self.assertEqual(guidance["segment_index"], 2)
        self.assertEqual(guidance["checkpoint_id"], "commit_send")
        self.assertEqual(guidance["checkpoint_name"], "3. Commit - Send")
        self.assertTrue(guidance["steps"])

    def test_augment_reteach_guidance_with_family_adds_base_variant(self) -> None:
        guidance = {"suggested": True, "checkpoint_id": "commit_send", "checkpoint_name": "3. Commit - Send"}
        family = {
            "checkpoint_map": [
                {
                    "checkpoint_name": "3. Commit - Send",
                    "checkpoint_ids": ["commit_send"],
                    "suggested_base_variant": {"recipe_id": "v1", "variant_label": "variant_a"},
                    "variant_diffs": [],
                    "semantic_id": "commit_send_sem",
                }
            ]
        }
        out = web_ui._augment_reteach_guidance_with_family(guidance, family)
        self.assertEqual(out["base_variant_recipe_id"], "v1")
        self.assertEqual(out["base_variant_label"], "variant_a")
        self.assertEqual(out["checkpoint_detail"]["semantic_id"], "commit_send_sem")

    def test_needs_live_replay_state_requires_real_signals(self) -> None:
        self.assertTrue(web_ui._needs_live_replay_state({"app_name": "gmail", "visible_labels": [], "selector_values": []}))
        self.assertFalse(web_ui._needs_live_replay_state({"app_name": "gmail", "visible_labels": ["Compose"], "selector_values": [], "visible_roles": [], "tree_signature": ""}))


if __name__ == "__main__":
    unittest.main()
