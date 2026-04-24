import unittest
from unittest.mock import patch

from lam.interface import browser_worker


class TestBrowserWorker(unittest.TestCase):
    def test_normalize_mode_defaults_to_local(self) -> None:
        self.assertEqual(browser_worker.normalize_browser_worker_mode(""), "local")
        self.assertEqual(browser_worker.normalize_browser_worker_mode("weird"), "local")
        self.assertEqual(browser_worker.normalize_browser_worker_mode("docker"), "docker")

    def test_ensure_local_mode_returns_local_session(self) -> None:
        out = browser_worker.ensure_browser_worker(mode="local")
        self.assertTrue(out.get("ok"))
        self.assertEqual(out.get("mode"), "local")
        self.assertEqual(out.get("debug_port"), 9222)

    def test_ensure_docker_mode_fails_when_docker_unavailable(self) -> None:
        with patch("lam.interface.browser_worker._docker_available", return_value={"ok": False, "detail": "missing"}):
            out = browser_worker.ensure_browser_worker(mode="docker")
        self.assertFalse(out.get("ok"))
        self.assertEqual(out.get("error"), "docker_unavailable")

    def test_ensure_docker_mode_ready(self) -> None:
        with patch("lam.interface.browser_worker._docker_available", return_value={"ok": True}), patch(
            "lam.interface.browser_worker._docker_container_running", return_value=True
        ), patch(
            "lam.interface.browser_worker._wait_for_cdp",
            return_value={"ok": True, "version": "Chrome/125", "websocket": "ws://127.0.0.1:9223/devtools/browser/x"},
        ):
            out = browser_worker.ensure_browser_worker(mode="docker")
        self.assertTrue(out.get("ok"))
        self.assertEqual(out.get("mode"), "docker")
        self.assertEqual(out.get("status"), "ready")


if __name__ == "__main__":
    unittest.main()
