import unittest
from unittest.mock import patch

from lam.interface import web_ui


class TestWebUiBrowserWorkerPolicy(unittest.TestCase):
    def test_load_policy_browser_worker_defaults(self) -> None:
        with patch(
            "lam.interface.web_ui._load_policy_yaml",
            return_value={"policies": {"browser_worker": {"mode": "docker", "human_like_interaction": True}}},
        ):
            out = web_ui._load_policy_browser_worker_defaults()
        self.assertEqual(out.get("browser_worker_mode"), "docker")
        self.assertTrue(out.get("human_like_interaction"))

    def test_apply_user_defaults_uses_policy_when_user_default_missing(self) -> None:
        state = web_ui.UiState()
        with patch("lam.interface.web_ui.load_defaults", return_value={}), patch(
            "lam.interface.web_ui._load_policy_browser_worker_defaults",
            return_value={"browser_worker_mode": "docker", "human_like_interaction": True},
        ):
            web_ui._apply_user_defaults(state)
        self.assertEqual(state.browser_worker_mode, "docker")
        self.assertTrue(state.human_like_interaction)

    def test_load_policy_browser_worker_defaults_falls_back_to_human_like_true(self) -> None:
        with patch(
            "lam.interface.web_ui._load_policy_yaml",
            return_value={"policies": {"browser_worker": {"mode": "local"}}},
        ):
            out = web_ui._load_policy_browser_worker_defaults()
        self.assertEqual(out.get("browser_worker_mode"), "local")
        self.assertTrue(out.get("human_like_interaction"))


if __name__ == "__main__":
    unittest.main()
