import unittest
from unittest.mock import patch
from pathlib import Path

from lam.interface.desktop_sequence import assess_risk, build_plan, execute_plan


class TestDesktopSequence(unittest.TestCase):
    def test_build_plan(self) -> None:
        plan = build_plan('open chatgpt app then click New chat then type "hello" then press enter')
        self.assertGreaterEqual(len(plan["steps"]), 4)
        self.assertEqual(plan["steps"][0]["action"], "open_app")

    def test_build_plan_scroll_and_visual(self) -> None:
        plan = build_plan('open chatgpt app then scroll down 3 then find text "Send" then click found')
        actions = [s["action"] for s in plan["steps"]]
        self.assertIn("scroll", actions)
        self.assertIn("visual_search", actions)
        self.assertIn("click_found", actions)
    
    def test_assess_risk(self) -> None:
        plan = {"steps": [{"action": "click", "selector": {"value": "Submit"}}]}
        risk = assess_risk(plan)
        self.assertTrue(risk["requires_confirmation"])

    def test_build_plan_use_credentials(self) -> None:
        plan = build_plan("open chatgpt app then login with linkedin")
        actions = [s["action"] for s in plan["steps"]]
        self.assertIn("use_credentials", actions)

    def test_build_plan_capture_clipboard_image(self) -> None:
        plan = build_plan("open paint then capture clipboard image")
        actions = [s["action"] for s in plan["steps"]]
        self.assertIn("capture_clipboard_image", actions)

    @patch("lam.interface.desktop_sequence.open_installed_app")
    def test_execute_plan_open_pause(self, mock_open) -> None:
        mock_open.return_value = (True, "chatgpt.exe")
        plan = {"steps": [{"action": "open_app", "app": "chatgpt"}], "checkpoint_after_open": True}
        result = execute_plan(plan, start_index=0, step_mode=False)
        self.assertTrue(result.ok)
        self.assertTrue(result.paused_for_credentials)
        self.assertFalse(result.done)

    @patch("lam.interface.desktop_sequence.LocalPasswordVault")
    @patch("lam.interface.desktop_sequence.UIAAdapter")
    def test_execute_plan_use_credentials(self, mock_adapter_cls, mock_vault_cls) -> None:
        mock_adapter = mock_adapter_cls.return_value
        mock_vault = mock_vault_cls.return_value
        mock_vault.find_entry_by_service.return_value = {
            "ok": True,
            "entry": {"id": "1", "service": "linkedin", "username": "user1", "password": "pass1"},
        }
        plan = {"steps": [{"action": "use_credentials", "service": "linkedin", "submit": True}], "checkpoint_after_open": False}
        result = execute_plan(plan, start_index=0, step_mode=False)
        self.assertTrue(result.ok)
        mock_adapter.type.assert_any_call({}, "user1")
        mock_adapter.type.assert_any_call({}, "pass1")
        mock_adapter.hotkey.assert_any_call("TAB")
        mock_adapter.hotkey.assert_any_call("ENTER")

    @patch("lam.interface.desktop_sequence.capture_clipboard_image")
    @patch("lam.interface.desktop_sequence.LocalPasswordVault")
    @patch("lam.interface.desktop_sequence.UIAAdapter")
    def test_execute_plan_capture_clipboard_image(self, mock_adapter_cls, mock_vault_cls, mock_capture) -> None:
        del mock_adapter_cls, mock_vault_cls
        out = Path("data/reports/desktop_sequence/test_clipboard/clipboard_capture.png").resolve()
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(b"fake-image")
        mock_capture.return_value = str(out)
        plan = {"steps": [{"action": "capture_clipboard_image", "output_path": ""}], "checkpoint_after_open": False}
        result = execute_plan(plan, start_index=0, step_mode=False)
        self.assertTrue(result.ok)
        self.assertEqual(result.artifacts.get("clipboard_image_png"), str(out))
        self.assertEqual(result.artifacts.get("primary_open_file"), str(out))


if __name__ == "__main__":
    unittest.main()
