import unittest
import tempfile
from pathlib import Path
from unittest.mock import patch

from lam.interface.search_agent import EmailActionItem, execute_instruction, preview_instruction, resume_pending_plan


class TestSearchAgent(unittest.TestCase):
    def test_control_gate(self) -> None:
        result = execute_instruction("search amazon for abu garcia voltiq baitcasting reel", control_granted=False)
        self.assertFalse(result["ok"])
        self.assertIn("Control not granted", result["error"])

    @patch("lam.interface.search_agent.get_guidance")
    @patch("lam.interface.search_agent.execute_plan")
    def test_open_app_flow(self, mock_exec, mock_guidance) -> None:
        class R:
            ok = True
            trace = [{"step": 0, "action": "open_app", "ok": True}]
            done = False
            next_step_index = 1
            paused_for_credentials = True
            pause_reason = "Login checkpoint"
            error = ""

        mock_exec.return_value = R()
        mock_guidance.return_value = {"app_name": "chatgpt", "guidance": []}
        result = execute_instruction("open chatgpt app", control_granted=True)
        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "desktop_sequence")
        self.assertTrue(result["paused_for_credentials"])
        self.assertIsNotNone(result["pending_plan"])
        self.assertIn("task_envelope", result)
        self.assertIn("plan_contract", result)
        self.assertIn("execution_trace", result)
        self.assertIn("verification_report", result)
        self.assertIn("final_report", result)

    @patch("lam.interface.search_agent.execute_plan")
    def test_resume_pending_plan(self, mock_exec) -> None:
        class R:
            ok = True
            trace = [{"step": 1, "action": "click", "ok": True}]
            done = True
            next_step_index = 2
            paused_for_credentials = False
            pause_reason = ""
            error = ""

        mock_exec.return_value = R()
        result = resume_pending_plan(
            {
                "plan": {
                    "steps": [
                        {"action": "open_app", "app": "chatgpt"},
                        {"action": "click", "selector": {"value": "button:Run"}},
                    ]
                },
                "next_step_index": 1,
            },
            step_mode=False,
        )
        self.assertTrue(result["ok"])
        self.assertTrue(result["done"])

    def test_preview_instruction(self) -> None:
        result = preview_instruction("open chatgpt app then click submit")
        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "preview_desktop_sequence")
        self.assertIn("risk", result)
        self.assertIn("planned_steps", result)
        self.assertIn("undo_plan", result)

    def test_preview_native_plan(self) -> None:
        result = preview_instruction("Research top AI data leadership jobs in US and Ireland then build spreadsheet and dashboard")
        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "preview_native_plan")
        self.assertIn("plan", result)

    @patch("lam.interface.search_agent._run_job_market_research")
    def test_job_research_flow(self, mock_run) -> None:
        mock_run.return_value = {
            "ok": True,
            "mode": "job_market_research",
            "query": "VP Data and AI",
            "results_count": 3,
            "artifacts": {"dashboard_html": "C:\\temp\\dash.html", "jobs_csv": "C:\\temp\\jobs.csv"},
            "summary": {"total": 3},
            "results": [{"title": "VP Data and AI", "url": "https://example.com"}],
            "opened_url": "file:///C:/temp/dash.html",
            "canvas": {"title": "Job Market Dashboard Generated", "subtitle": "3 listings", "cards": []},
        }
        result = execute_instruction(
            "Search Indeed and LinkedIn for VP of Data and AI roles and build spreadsheet and dashboard",
            control_granted=True,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "autonomous_plan_execute")
        self.assertIn("plan", result)
        self.assertEqual(result["results_count"], 3)
        self.assertIn("verification", result)
        self.assertIn("report", result)
        self.assertIn("undo_plan", result)

    @patch("lam.interface.search_agent._run_competitor_analysis")
    def test_competitor_analysis_flow(self, mock_run) -> None:
        mock_run.return_value = {
            "ok": True,
            "mode": "competitor_analysis",
            "query": "Epic Systems EHR competitors",
            "results_count": 12,
            "artifacts": {
                "executive_summary_md": "C:\\temp\\executive_summary.md",
                "powerpoint_pptx": "C:\\temp\\executive_summary.pptx",
                "dashboard_html": "C:\\temp\\dashboard.html",
            },
            "summary": {
                "target": "Epic Systems",
                "top_competitors": ["Oracle Health (Cerner)"],
                "live_non_curated_citations": 4,
                "required_live_non_curated_citations": 3,
            },
            "results": [{"title": "Oracle Health vs Epic", "url": "https://example.com"}],
            "opened_url": "file:///C:/temp/dashboard.html",
            "canvas": {"title": "Epic Systems Competitor Analysis Ready", "subtitle": "Top 5", "cards": []},
        }
        result = execute_instruction(
            "Research the top 5 competitors to Epic Systems, build a 2-page executive summary, create a PowerPoint, and save everything to a folder called Epic Competitor Analysis.",
            control_granted=True,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "autonomous_plan_execute")
        self.assertIn("executive_summary_md", result["artifacts"])
        self.assertIn("powerpoint_pptx", result["artifacts"])
        self.assertIn("verification", result)

    @patch("lam.interface.search_agent._run_generic_research")
    @patch("lam.interface.search_agent._run_competitor_analysis")
    def test_competitor_analysis_strict_citation_gate(self, mock_comp, mock_generic) -> None:
        mock_comp.return_value = {
            "ok": False,
            "mode": "competitor_analysis",
            "query": "Epic Systems EHR competitors",
            "results_count": 2,
            "results": [],
            "artifacts": {},
            "summary": {
                "error": "insufficient_live_non_curated_citations",
                "live_non_curated_citations": 0,
                "required_live_non_curated_citations": 3,
            },
            "source_status": {},
            "opened_url": "",
            "canvas": {"title": "Run Blocked", "subtitle": "Strict citation gate", "cards": []},
        }
        mock_generic.return_value = {
            "ok": True,
            "query": "fallback generic query",
            "results_count": 20,
            "results": [{"title": "x", "url": "https://example.com", "source": "duckduckgo"}],
            "artifacts": {"report_md": "C:\\temp\\report.md"},
            "summary": {"total": 20},
            "source_status": {},
            "opened_url": "file:///C:/temp/report.md",
            "canvas": {"title": "Generic Result", "subtitle": "fallback", "cards": []},
        }
        result = execute_instruction(
            "Research the top 5 competitors to Epic Systems and build executive summary and PowerPoint",
            control_granted=True,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["summary"].get("error"), "strict_competitor_validation_failed")

    def test_destructive_instruction_requires_confirmation(self) -> None:
        result = execute_instruction(
            "delete all files in downloads",
            control_granted=True,
            confirm_risky=False,
        )
        self.assertFalse(result["ok"])
        self.assertTrue(result.get("requires_confirmation", False))
        self.assertIn("planned_steps", result)
        self.assertIn("undo_plan", result)

    @patch("lam.interface.search_agent._run_email_triage")
    def test_email_triage_flow(self, mock_email) -> None:
        mock_email.return_value = {
            "ok": True,
            "mode": "email_triage",
            "query": "newer_than:2d in:inbox",
            "results_count": 2,
            "results": [],
            "artifacts": {"email_tasks_csv": "C:\\temp\\task_list.csv", "email_triage_html": "C:\\temp\\dashboard.html"},
            "summary": {"messages_processed": 2, "action_required": 1, "drafts_created": 1},
            "source_status": {"gmail_ui": "ok"},
            "opened_url": "file:///C:/temp/dashboard.html",
            "paused_for_credentials": False,
            "pause_reason": "",
            "trace": [
                {"step": 0, "action": "list_recent_messages", "ok": True, "result_count": 2},
                {"step": 1, "action": "read_message", "ok": True, "result_count": 2},
                {"step": 2, "action": "create_draft", "ok": True, "result_count": 1},
                {"step": 3, "action": "save_csv", "ok": True, "artifact": "C:\\temp\\task_list.csv"},
                {"step": 4, "action": "present", "ok": True, "opened_url": "file:///C:/temp/dashboard.html"},
            ],
            "canvas": {"title": "Inbox Triage Completed", "subtitle": "1 action-needed emails", "cards": []},
        }
        result = execute_instruction(
            "Scan my inbox (cmajeff@gmail.com) for emails from the last 48 hours, identify anything requiring action, create a task list in a spreadsheet, and draft replies for each.",
            control_granted=True,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "autonomous_plan_execute")
        self.assertEqual(result.get("plan", {}).get("domain"), "email_triage")
        self.assertEqual(result.get("plan_contract", {}).get("primary_domain"), "email")
        self.assertEqual(result.get("plan_contract", {}).get("validation_status"), "valid")
        mock_email.assert_called_once()

    @patch("lam.interface.search_agent.focus_auth_session")
    @patch("lam.interface.search_agent._start_email_auth_session")
    def test_email_triage_manual_auth_phase_pauses(self, mock_start_auth, mock_focus) -> None:
        mock_start_auth.return_value = {"ok": True, "auth_session_id": "sid-auth"}
        mock_focus.return_value = {"ok": True, "opened_url": "https://mail.google.com/"}
        result = execute_instruction(
            "Scan my inbox (cmajeff@gmail.com) for emails from the last 48 hours, identify anything requiring action, create a task list in a spreadsheet, and draft replies for each.",
            control_granted=True,
            manual_auth_phase=True,
        )
        self.assertFalse(result["ok"])
        self.assertTrue(result.get("paused_for_credentials", False))
        self.assertEqual(result.get("error_code"), "credential_missing")
        self.assertEqual(result.get("summary", {}).get("error"), "manual_auth_phase")
        opened = result.get("opened_url", "")
        self.assertTrue(("accounts.google.com" in opened) or ("mail.google.com" in opened))
        self.assertEqual(result.get("auth_session_id"), "sid-auth")
        self.assertGreaterEqual(mock_start_auth.call_count, 1)

    @patch("lam.interface.search_agent._run_generic_research")
    @patch("lam.interface.search_agent._run_email_triage")
    def test_email_triage_credential_pause_does_not_fallback(self, mock_email, mock_generic) -> None:
        mock_email.return_value = {
            "ok": False,
            "mode": "email_triage",
            "query": "newer_than:2d in:inbox",
            "results_count": 0,
            "results": [],
            "artifacts": {},
            "summary": {"error": "credential_missing", "account": "cmajeff@gmail.com"},
            "source_status": {"gmail_ui": "ok"},
            "opened_url": "https://mail.google.com/",
            "paused_for_credentials": True,
            "pause_reason": "Google login required.",
            "error": "credential_missing",
            "error_code": "credential_missing",
            "trace": [{"step": 0, "action": "list_recent_messages", "ok": True, "opened_url": "https://mail.google.com/"}],
            "canvas": {"title": "Paused For Login", "subtitle": "Sign in", "cards": []},
        }
        mock_generic.return_value = {"ok": True, "query": "bad fallback", "results_count": 10, "results": [], "artifacts": {}}
        result = execute_instruction(
            "Scan my inbox (cmajeff@gmail.com) for emails from the last 48 hours, identify anything requiring action, create a task list in a spreadsheet, and draft replies for each.",
            control_granted=True,
        )
        self.assertFalse(result["ok"])
        self.assertTrue(result.get("paused_for_credentials", False))
        self.assertEqual(result.get("error_code"), "credential_missing")
        self.assertEqual(result.get("query"), "newer_than:2d in:inbox")
        mock_generic.assert_not_called()

    @patch("lam.interface.search_agent.webbrowser.open")
    def test_email_triage_active_browser_manual_auth_and_resume(self, mock_open) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            out_dir = root / "email_triage"
            out_dir.mkdir(parents=True, exist_ok=True)
            csv_path = out_dir / "task_list.csv"
            md_path = out_dir / "summary.md"
            html_path = out_dir / "dashboard.html"
            csv_path.write_text("message_id,sender\n", encoding="utf-8")
            md_path.write_text("# Inbox Triage Summary\n", encoding="utf-8")
            html_path.write_text("<html></html>", encoding="utf-8")

            with patch(
                "lam.interface.search_agent._write_email_triage_artifacts",
                return_value={
                    "directory": str(out_dir),
                    "email_tasks_csv": str(csv_path),
                    "summary_md": str(md_path),
                    "email_triage_html": str(html_path),
                    "primary_open_file": str(html_path),
                },
            ), patch(
                "lam.interface.search_agent._start_email_auth_session",
                return_value={"ok": True, "auth_session_id": "sid1"},
            ), patch(
                "lam.interface.search_agent.focus_auth_session",
                return_value={"ok": True, "opened_url": "https://mail.google.com/"},
            ):
                first = execute_instruction(
                    "Scan my inbox (cmajeff@gmail.com) for emails from the last 48 hours, identify anything requiring action, create a task list in a spreadsheet, and draft replies for each.",
                    control_granted=True,
                    manual_auth_phase=True,
                )
                self.assertFalse(first["ok"])
                self.assertTrue(first.get("paused_for_credentials", False))
                sid = str(first.get("auth_session_id", ""))
                self.assertTrue(bool(sid))

                class FakePage:
                    def __init__(self) -> None:
                        self.url = "https://mail.google.com/"

                    def bring_to_front(self) -> None:
                        return None

                with patch.dict(
                    "lam.interface.search_agent._EMAIL_AUTH_SESSIONS",
                    {"sid1": {"page": FakePage(), "url": "https://mail.google.com/"}},
                    clear=True,
                ), patch("lam.interface.search_agent._gmail_wait_ready_state", return_value="mail"), patch(
                    "lam.interface.search_agent._gmail_filter_last_48h", return_value=None
                ), patch(
                    "lam.interface.search_agent._gmail_collect_rows", return_value=[object(), object()]
                ), patch(
                    "lam.interface.search_agent._gmail_process_message",
                    side_effect=[
                        EmailActionItem(
                            message_id="m1",
                            sender="a@example.com",
                            subject="Action required: review",
                            received_at="now",
                            snippet="please review",
                            requires_action=True,
                            reason="matched:please review",
                            draft_created=False,
                        ),
                        EmailActionItem(
                            message_id="m2",
                            sender="b@example.com",
                            subject="FYI",
                            received_at="now",
                            snippet="for your info",
                            requires_action=False,
                            reason="no_action_keyword",
                            draft_created=False,
                        ),
                    ],
                ), patch("lam.interface.search_agent._gmail_create_draft_reply", return_value=True), patch(
                    "lam.interface.search_agent._gmail_back_to_inbox", return_value=None
                ):
                    second = execute_instruction(
                        "Scan my inbox (cmajeff@gmail.com) for emails from the last 48 hours, identify anything requiring action, create a task list in a spreadsheet, and draft replies for each.",
                        control_granted=True,
                        manual_auth_phase=False,
                        auth_session_id=sid,
                    )
                    self.assertTrue(second["ok"])
                    self.assertFalse(second.get("paused_for_credentials", False))
                    self.assertEqual(second.get("source_status", {}).get("gmail_ui"), "ok")
                    self.assertIn("email_tasks_csv", second.get("artifacts", {}))
                    self.assertGreaterEqual(mock_open.call_count, 1)

    @patch("lam.interface.search_agent.webbrowser.open")
    def test_email_triage_without_session_id_enters_manual_auth_phase(self, mock_open) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            out_dir = root / "email_triage"
            out_dir.mkdir(parents=True, exist_ok=True)
            csv_path = out_dir / "task_list.csv"
            md_path = out_dir / "summary.md"
            html_path = out_dir / "dashboard.html"
            csv_path.write_text("message_id,sender\n", encoding="utf-8")
            md_path.write_text("# Inbox Triage Summary\n", encoding="utf-8")
            html_path.write_text("<html></html>", encoding="utf-8")

            with patch(
                "lam.interface.search_agent._write_email_triage_artifacts",
                return_value={
                    "directory": str(out_dir),
                    "email_tasks_csv": str(csv_path),
                    "summary_md": str(md_path),
                    "email_triage_html": str(html_path),
                    "primary_open_file": str(html_path),
                },
            ), patch(
                "lam.interface.search_agent._start_email_auth_session",
                return_value={"ok": True, "auth_session_id": "sid2"},
            ), patch(
                "lam.interface.search_agent.focus_auth_session",
                return_value={"ok": True, "opened_url": "https://mail.google.com/"},
            ):
                result = execute_instruction(
                    "Scan my inbox (cmajeff@gmail.com) for emails from the last 48 hours, identify anything requiring action, create a task list in a spreadsheet, and draft replies for each.",
                    control_granted=True,
                    manual_auth_phase=False,
                    auth_session_id="",
                )
                self.assertFalse(result["ok"])
                self.assertTrue(result.get("paused_for_credentials", False))
                self.assertEqual(result.get("error_code"), "credential_missing")
                self.assertEqual(result.get("source_status", {}).get("gmail_ui"), "manual_auth_phase")
                self.assertEqual(result.get("auth_session_id"), "sid2")
                self.assertGreaterEqual(mock_open.call_count, 0)

    @patch("lam.interface.search_agent.get_guidance")
    @patch("lam.interface.search_agent.execute_plan")
    @patch("lam.interface.search_agent.assess_risk")
    @patch("lam.interface.search_agent.build_plan")
    def test_communication_social_routes_to_desktop_sequence(self, mock_build_plan, mock_risk, mock_exec, mock_guidance) -> None:
        class R:
            ok = True
            trace = [{"step": 0, "action": "type_text", "ok": True}]
            done = True
            next_step_index = 1
            paused_for_credentials = False
            pause_reason = ""
            error = ""

        mock_build_plan.return_value = {
            "app_name": "whatsapp",
            "steps": [
                {"action": "open_app", "app": "whatsapp"},
                {"action": "type_text", "text": "Confirmed for 5pm."},
                {"action": "click", "selector": {"value": "Send"}},
            ],
        }
        mock_risk.return_value = {"requires_confirmation": False, "risky_steps": []}
        mock_exec.return_value = R()
        mock_guidance.return_value = {"app_name": "whatsapp", "guidance": []}
        result = execute_instruction("Respond to the WhatsApp chat confirming I can join at 5pm.", control_granted=True)
        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "desktop_sequence")
        mock_build_plan.assert_called_once()

    @patch("lam.interface.search_agent._search_web")
    @patch("lam.interface.search_agent.webbrowser.open")
    def test_productivity_asks_route_to_artifact_generation(self, mock_open, mock_search) -> None:
        result = execute_instruction(
            "Write a launch document, build a PPT, and create visuals for the Q3 kickoff.",
            control_granted=True,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "artifact_generation")
        self.assertIn("document_md", result.get("artifacts", {}))
        self.assertIn("powerpoint_pptx", result.get("artifacts", {}))
        self.assertIn("visual_html", result.get("artifacts", {}))
        mock_search.assert_not_called()
        self.assertGreaterEqual(mock_open.call_count, 1)

    @patch("lam.interface.search_agent._execute_native_plan")
    @patch("lam.interface.search_agent._build_native_plan")
    @patch("lam.interface.search_agent._classify_explicit_route")
    def test_fail_fast_when_requested_outputs_not_in_native_plan(self, mock_route, mock_build_native, mock_exec_native) -> None:
        mock_route.return_value = ""
        mock_build_native.return_value = {
            "planner": "native-v1",
            "domain": "web_research",
            "objective": "Research AI infra trends and build a dashboard and PowerPoint",
            "deliverables": ["report"],
            "sources": ["web_search"],
            "constraints": {"prefer_public_pages": True},
            "steps": [
                {"kind": "research", "name": "Collect sources", "target": {"query": "ai infra"}},
                {"kind": "produce", "name": "Generate report", "target": {"path": "data/reports"}},
            ],
        }
        result = execute_instruction(
            "Research AI infra trends and build a dashboard and PowerPoint",
            control_granted=True,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["mode"], "native_plan_invalid")
        self.assertIn("powerpoint", result.get("missing_outputs", []))
        mock_exec_native.assert_not_called()


if __name__ == "__main__":
    unittest.main()
