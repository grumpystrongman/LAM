import tempfile
import unittest
from pathlib import Path

from lam.interface.session_manager import SessionManager


class TestSessionManager(unittest.TestCase):
    def test_retry_decision_blocks_after_failed_attempts_without_reusable_tab(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mgr = SessionManager(path=str(Path(td) / "session_state.json"))
            mgr.record_auth_attempt(domain="mail.google.com", status="failed", detail="a")
            mgr.record_auth_attempt(domain="mail.google.com", status="blocked", detail="b")
            decision = mgr.auth_retry_decision(domain="mail.google.com", max_failed_attempts=2)
            self.assertFalse(decision.allow_retry)
            self.assertEqual(decision.reason, "auth_retry_budget_exhausted")

    def test_retry_decision_allows_when_authenticated_tab_exists(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mgr = SessionManager(path=str(Path(td) / "session_state.json"))
            mgr.record_auth_attempt(domain="mail.google.com", status="failed", detail="a")
            mgr.record_auth_attempt(domain="mail.google.com", status="failed", detail="b")
            mgr.remember_tab(url="https://mail.google.com/mail/u/0/#inbox", title="Inbox", authenticated=True)
            decision = mgr.auth_retry_decision(domain="mail.google.com", max_failed_attempts=2)
            self.assertTrue(decision.allow_retry)
            self.assertEqual(decision.reason, "reusable_authenticated_tab")
            self.assertIn("mail.google.com", decision.reusable_authenticated_tab)

    def test_find_reusable_url_exact(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            mgr = SessionManager(path=str(Path(td) / "session_state.json"))
            mgr.remember_tab(url="file:///C:/temp/dashboard.html", title="Dashboard", authenticated=False)
            found = mgr.find_reusable_url("file:///C:/temp/dashboard.html")
            self.assertEqual(found, "file:///C:/temp/dashboard.html")


if __name__ == "__main__":
    unittest.main()
