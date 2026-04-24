import unittest

from lam.interface import web_ui


class TestWebUiAuthRecovery(unittest.TestCase):
    def test_recommend_mode_prefers_local_for_docker_auth_failures(self) -> None:
        mode, confidence, reason = web_ui._recommend_mode_for_auth_error(
            "docker_worker_auth_required",
            "docker",
        )
        self.assertEqual(mode, "local")
        self.assertEqual(confidence, "high")
        self.assertIn("interactive login", reason.lower())

    def test_recommend_mode_prefers_docker_for_locked_local_profile(self) -> None:
        mode, confidence, reason = web_ui._recommend_mode_for_auth_error(
            "auth_profile_locked",
            "local",
        )
        self.assertEqual(mode, "docker")
        self.assertEqual(confidence, "medium")
        self.assertIn("cleaner browser context", reason.lower())

    def test_extract_auth_error_code_prefers_imap_summary_code(self) -> None:
        code = web_ui._extract_auth_error_code(
            {
                "summary": {
                    "error": "imap_fallback_failed",
                    "imap_error_code": "imap_app_password_required",
                }
            }
        )
        self.assertEqual(code, "imap_app_password_required")

    def test_build_auth_recovery_marks_auth_loop_detected(self) -> None:
        state = web_ui.UiState()
        with state.lock:
            state.browser_worker_mode = "local"
            state.paused_for_credentials = True
            state.pause_reason = "Sign in to Gmail, then click Resume."
            state.auth_loop_blocked = True
            out = web_ui._build_auth_recovery_recommendation_locked(
                state=state,
                current_task={},
            )
        self.assertTrue(out.get("show"))
        self.assertEqual(out.get("error_code"), "auth_loop_detected")
        self.assertEqual(out.get("recommended_mode"), "local")


if __name__ == "__main__":
    unittest.main()
