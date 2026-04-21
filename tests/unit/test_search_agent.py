import unittest
from unittest.mock import patch

from lam.interface.search_agent import execute_instruction, preview_instruction, resume_pending_plan


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
        result = resume_pending_plan({"plan": {"steps": [{}, {}]}, "next_step_index": 1}, step_mode=False)
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


if __name__ == "__main__":
    unittest.main()
