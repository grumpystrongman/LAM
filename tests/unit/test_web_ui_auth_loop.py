import unittest

from lam.interface import web_ui


class TestWebUiAuthLoop(unittest.TestCase):
    def test_auth_loop_tracking_blocks_after_three_identical_pauses(self) -> None:
        state = web_ui.UiState()
        paused_result = {
            "ok": False,
            "mode": "email_triage",
            "paused_for_credentials": True,
            "error_code": "credential_missing",
            "pause_reason": "Complete Gmail sign-in in the opened tab, then click Resume.",
            "summary": {"error": "imap_fallback_failed"},
            "source_status": {"gmail_imap": "error:RuntimeError"},
        }
        with state.lock:
            web_ui._apply_auth_loop_tracking_locked(state, paused_result)
            web_ui._apply_auth_loop_tracking_locked(state, paused_result)
            web_ui._apply_auth_loop_tracking_locked(state, paused_result)
            self.assertTrue(state.auth_loop_blocked)
            self.assertEqual(state.auth_loop_count, 3)

    def test_auth_loop_tracking_resets_on_non_paused_result(self) -> None:
        state = web_ui.UiState()
        paused_result = {
            "ok": False,
            "mode": "email_triage",
            "paused_for_credentials": True,
            "error_code": "credential_missing",
            "pause_reason": "Sign in required.",
            "summary": {"error": "manual_auth_phase"},
            "source_status": {"gmail_ui": "manual_auth_phase"},
        }
        done_result = {"ok": True, "mode": "email_triage", "paused_for_credentials": False}
        with state.lock:
            web_ui._apply_auth_loop_tracking_locked(state, paused_result)
            self.assertEqual(state.auth_loop_count, 1)
            web_ui._apply_auth_loop_tracking_locked(state, done_result)
            self.assertEqual(state.auth_loop_count, 0)
            self.assertFalse(state.auth_loop_blocked)
            self.assertEqual(state.auth_loop_signature, "")

    def test_auth_loop_block_response_contains_recovery_guidance(self) -> None:
        state = web_ui.UiState()
        with state.lock:
            state.auth_loop_count = 4
            state.auth_loop_blocked = True
            out = web_ui._auth_loop_block_response(state)
        self.assertFalse(out.get("ok"))
        self.assertEqual(out.get("error_code"), "auth_loop_detected")
        self.assertTrue(out.get("paused_for_credentials"))
        self.assertIn("Focus auth", str(out.get("pause_reason", "")))


if __name__ == "__main__":
    unittest.main()

