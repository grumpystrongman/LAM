import unittest
from unittest.mock import patch

from lam.interface.search_agent import execute_instruction
from lam.interface.web_ui import UiState


class TestPhase2WorldModel(unittest.TestCase):
    @patch("lam.interface.search_agent._execute_native_plan")
    def test_execute_instruction_emits_playbook_narration_world_model(self, mock_exec) -> None:
        mock_exec.return_value = {
            "ok": True,
            "mode": "autonomous_plan_execute",
            "query": "data and ai jobs",
            "results_count": 5,
            "artifacts": {"jobs_csv": "C:\\temp\\jobs.csv"},
            "summary": {"total": 5, "artifact_reuse_mode": "reuse_if_recent", "reused_existing_outputs": True},
            "results": [],
            "source_status": {"job_market": "ok"},
            "opened_url": "file:///C:/temp/jobs.csv",
            "canvas": {"title": "Done", "subtitle": "ok", "cards": []},
            "plan": {
                "domain": "job_market",
                "steps": [{"kind": "research"}, {"kind": "produce"}],
            },
        }
        result = execute_instruction(
            "Search jobs across the US and Ireland, then build a spreadsheet and dashboard.",
            control_granted=True,
        )
        self.assertTrue(result.get("ok"))
        self.assertIn("playbook", result)
        self.assertIn("narration", result)
        self.assertIn("world_model", result)
        self.assertEqual(result.get("playbook", {}).get("id"), "job-market-v1")
        self.assertGreater(len(result.get("narration", [])), 0)
        self.assertEqual(result.get("world_model", {}).get("domain"), "job_market")
        self.assertIn("what_i_noticed", result.get("world_model", {}))
        self.assertIn("signals", result.get("world_model", {}))
        self.assertEqual(result.get("world_model", {}).get("signals", {}).get("artifact_reuse_mode"), "reuse_if_recent")

    def test_ui_state_snapshot_includes_live_world_model(self) -> None:
        state = UiState()
        with state.lock:
            state.control_granted = True
            state.paused_for_credentials = True
            state.pause_reason = "Sign in required."
            state.pending_auth_url = "https://mail.google.com/"
            state.tasks["t1"] = {"id": "t1", "status": "running", "progress": 42, "message": "Reading inbox"}
            state.current_task_id = "t1"
            state.history = [{"mode": "email_triage", "artifacts": {"email_tasks_csv": "C:\\temp\\tasks.csv"}}]
        snap = state.snapshot()
        self.assertIn("world_model", snap)
        wm = snap["world_model"]
        self.assertTrue(wm.get("workspace", {}).get("control_granted"))
        self.assertTrue(wm.get("workspace", {}).get("paused_for_credentials"))
        self.assertEqual(wm.get("task", {}).get("status"), "running")
        self.assertGreaterEqual(len(wm.get("narration", [])), 1)
        self.assertGreaterEqual(len(wm.get("what_i_noticed", [])), 1)
        self.assertIn("signals", wm)


if __name__ == "__main__":
    unittest.main()
