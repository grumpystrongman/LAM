import unittest

from lam.interface import web_ui


class TestWebUiAuthSanitize(unittest.TestCase):
    def test_sanitize_focus_auth_url_blocks_myaccount_redirect(self) -> None:
        out = web_ui._sanitize_focus_auth_url(
            "https://myaccount.google.com/find-your-phone",
            instruction="",
        )
        self.assertEqual(out, "https://mail.google.com/")

    def test_sanitize_focus_auth_url_for_gmail_instruction(self) -> None:
        out = web_ui._sanitize_focus_auth_url(
            "https://example.com/auth",
            instruction="Scan my inbox (cmajeff@gmail.com) for emails from the last 48 hours",
        )
        self.assertEqual(out, "https://mail.google.com/")

    def test_sanitize_focus_auth_url_keeps_non_google_generic(self) -> None:
        out = web_ui._sanitize_focus_auth_url(
            "https://login.microsoftonline.com/",
            instruction="Continue Outlook auth",
        )
        self.assertEqual(out, "https://login.microsoftonline.com/")


if __name__ == "__main__":
    unittest.main()

