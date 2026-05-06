import unittest
from unittest.mock import Mock, patch
from pathlib import Path

from lam.interface.desktop_sequence import assess_risk, build_plan, build_plan_from_recipe, execute_plan
from lam.interface.learned_recipe import build_learned_recipe


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

    def test_build_plan_from_recipe(self) -> None:
        recipe = build_learned_recipe(
            "gmail",
            [
                {"ts": 1.0, "action": "click", "payload": {"selector": {"value": "Compose", "metadata": {"name": "Compose"}}}},
                {"ts": 2.0, "action": "type_text", "payload": {"text": "person@example.com"}},
                {"ts": 3.0, "action": "hotkey", "payload": {"keys": "enter"}},
            ],
        )
        built = build_plan_from_recipe(recipe.to_dict(), input_bindings={"email_input": "leader@example.com"})
        self.assertTrue(built["ok"])
        self.assertTrue(built["preview"]["can_autorun"])
        type_step = next(step for step in built["plan"]["steps"] if step["action"] == "type_text")
        self.assertEqual(type_step["text"], "leader@example.com")
        self.assertTrue(built["plan"]["state_checks"])

    def test_build_plan_from_recipe_missing_input_blocks_autorun(self) -> None:
        recipe = build_learned_recipe(
            "chatgpt",
            [
                {"ts": 1.0, "action": "type_text", "payload": {"text": ""}},
            ],
        )
        built = build_plan_from_recipe(recipe.to_dict(), input_bindings={})
        self.assertTrue(built["ok"])
        self.assertFalse(built["preview"]["can_autorun"])
        self.assertTrue(built["preview"]["missing_inputs"])

    def test_build_plan_from_family_chooses_matching_variant(self) -> None:
        recipe_a = build_learned_recipe(
            "gmail",
            [{"ts": 1.0, "action": "click", "payload": {"selector": {"value": "Compose", "metadata": {"name": "Compose"}}}}],
        ).to_dict()
        recipe_b = build_learned_recipe(
            "gmail",
            [{"ts": 1.0, "action": "click", "payload": {"selector": {"value": "Drafts", "metadata": {"name": "Drafts"}}}}],
        ).to_dict()
        family = {"family_id": "gmail_family", "variants": [recipe_b, recipe_a]}
        built = build_plan_from_recipe(family, input_bindings={}, current_state={"app_name": "gmail", "visible_labels": ["Compose"]}, is_family=True)
        self.assertTrue(built["ok"])
        self.assertEqual(built["preview"]["selected_variant"]["recipe_id"], recipe_a["recipe_id"])

    def test_build_plan_from_recipe_resume_from_source_index_skips_prior_steps(self) -> None:
        recipe = build_learned_recipe(
            "gmail",
            [
                {"ts": 1.0, "action": "click", "payload": {"selector": {"value": "Compose", "metadata": {"name": "Compose"}}}},
                {"ts": 2.0, "action": "type_text", "payload": {"text": "person@example.com"}},
            ],
        )
        built = build_plan_from_recipe(recipe.to_dict(), input_bindings={"email_input": "leader@example.com"}, resume_from_source_index=1)
        actions = [step["action"] for step in built["plan"]["steps"]]
        self.assertNotIn("open_app", actions)
        self.assertIn("type_text", actions)

    def test_build_plan_from_recipe_resume_from_segment_index_skips_prior_segment(self) -> None:
        recipe = build_learned_recipe(
            "gmail",
            [
                {"ts": 1.0, "action": "click", "payload": {"selector": {"value": "Compose", "metadata": {"name": "Compose"}}}},
                {"ts": 2.0, "action": "type_text", "payload": {"text": "person@example.com"}},
            ],
            observation_frames=[
                {"index": 0, "action": "click", "app_name": "gmail", "target_label": "Compose", "target_role": "Button", "selector": {"value": "Compose"}, "selector_candidates": [{"strategy": "text", "value": "Compose"}], "typed_text": "", "hotkey": "", "expected_state": "compose visible", "note": ""},
                {"index": 1, "action": "type_text", "app_name": "gmail", "target_label": "", "target_role": "Edit", "selector": {}, "selector_candidates": [], "typed_text": "person@example.com", "hotkey": "", "expected_state": "input accepted", "note": ""},
            ],
            observation_segments=[
                {"segment_type": "navigation", "purpose": "Move", "start_index": 0, "end_index": 0, "actions": ["click"]},
                {"segment_type": "data_entry", "purpose": "Type", "start_index": 1, "end_index": 1, "actions": ["type_text"]},
            ],
        )
        built = build_plan_from_recipe(recipe.to_dict(), input_bindings={"email_input": "leader@example.com"}, resume_from_segment_index=1)
        actions = [step["action"] for step in built["plan"]["steps"]]
        self.assertNotIn("open_app", actions)
        self.assertIn("type_text", actions)
        checkpoint_ids = [step.get("checkpoint_id", "") for step in built["plan"]["steps"] if step.get("action") in {"type_text", "assert_state"}]
        self.assertTrue(any(checkpoint_ids))

    def test_build_plan_from_recipe_resume_from_checkpoint_id_skips_prior_checkpoint(self) -> None:
        recipe = build_learned_recipe(
            "gmail",
            [
                {"ts": 1.0, "action": "click", "payload": {"selector": {"value": "Compose", "metadata": {"name": "Compose"}}}},
                {"ts": 2.0, "action": "type_text", "payload": {"text": "person@example.com"}},
            ],
            observation_frames=[
                {"index": 0, "action": "click", "app_name": "gmail", "target_label": "Compose", "target_role": "Button", "selector": {"value": "Compose"}, "selector_candidates": [{"strategy": "text", "value": "Compose"}], "typed_text": "", "hotkey": "", "expected_state": "compose visible", "note": ""},
                {"index": 1, "action": "type_text", "app_name": "gmail", "target_label": "", "target_role": "Edit", "selector": {}, "selector_candidates": [], "typed_text": "person@example.com", "hotkey": "", "expected_state": "input accepted", "note": ""},
            ],
            observation_segments=[
                {"segment_type": "navigation", "purpose": "Move", "start_index": 0, "end_index": 0, "actions": ["click"]},
                {"segment_type": "data_entry", "purpose": "Type", "start_index": 1, "end_index": 1, "actions": ["type_text"]},
            ],
        )
        checkpoint_id = recipe.state_snapshots[1]["checkpoint_id"]
        built = build_plan_from_recipe(recipe.to_dict(), input_bindings={"email_input": "leader@example.com"}, resume_from_checkpoint_id=checkpoint_id)
        actions = [step["action"] for step in built["plan"]["steps"]]
        self.assertNotIn("open_app", actions)
        self.assertIn("type_text", actions)

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

    @patch("lam.interface.desktop_sequence.LocalPasswordVault")
    @patch("lam.interface.desktop_sequence.UIAAdapter")
    def test_execute_plan_click_uses_fallback_selector(self, mock_adapter_cls, mock_vault_cls) -> None:
        del mock_vault_cls
        adapter = mock_adapter_cls.return_value
        attempts = {"count": 0}

        def click_side_effect(selector):
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise RuntimeError("primary selector failed")
            return None

        adapter.click.side_effect = click_side_effect
        plan = {
            "steps": [
                {
                    "action": "click",
                    "selector": {"strategy": "text", "value": "Primary"},
                    "fallback_selectors": [{"strategy": "text", "value": "Fallback"}],
                    "recovery_hint": "retry with fallback",
                }
            ],
            "checkpoint_after_open": False,
        }
        result = execute_plan(plan, start_index=0, step_mode=False)
        self.assertTrue(result.ok)
        self.assertTrue(result.done)
        self.assertEqual(adapter.click.call_count, 2)

    @patch("lam.interface.desktop_sequence.LocalPasswordVault")
    @patch("lam.interface.desktop_sequence.UIAAdapter")
    def test_execute_plan_assert_state_uses_candidates(self, mock_adapter_cls, mock_vault_cls) -> None:
        del mock_vault_cls
        adapter = mock_adapter_cls.return_value
        calls = {"count": 0}

        def assert_side_effect(selector, timeout_ms=None):
            del timeout_ms
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("not visible")
            return None

        setattr(adapter, "assert_visible", Mock(side_effect=assert_side_effect))
        plan = {
            "steps": [
                {
                    "action": "assert_state",
                    "candidate_selectors": [{"strategy": "text", "value": "Primary"}, {"strategy": "text", "value": "Fallback"}],
                    "description": "Expected compose surface",
                    "optional": False,
                    "recovery_hint": "return to compose",
                }
            ],
            "checkpoint_after_open": False,
        }
        result = execute_plan(plan, start_index=0, step_mode=False)
        self.assertTrue(result.ok)
        self.assertEqual(adapter.assert_visible.call_count, 2)

    @patch("lam.interface.desktop_sequence.LocalPasswordVault")
    @patch("lam.interface.desktop_sequence.UIAAdapter")
    def test_execute_plan_assert_state_captures_live_state(self, mock_adapter_cls, mock_vault_cls) -> None:
        del mock_vault_cls
        adapter = mock_adapter_cls.return_value
        setattr(adapter, "assert_visible", Mock(return_value=None))
        setattr(
            adapter,
            "capture_live_state",
            Mock(side_effect=[
                {"ok": True, "app_name": "gmail", "visible_labels": ["Compose"], "selector_values": ["Compose"]},
                {"ok": True, "app_name": "gmail", "visible_labels": ["Draft"], "selector_values": ["Draft"]},
            ]),
        )
        plan = {
            "app_name": "gmail",
            "steps": [
                {
                    "action": "assert_state",
                    "candidate_selectors": [{"strategy": "text", "value": "Compose"}],
                    "description": "Expected compose surface",
                    "phase": "post",
                    "source_index": 2,
                    "optional": False,
                }
            ],
            "checkpoint_after_open": False,
        }
        result = execute_plan(plan, start_index=0, step_mode=False)
        self.assertTrue(result.ok)
        self.assertIn("live_state_before", result.trace[0])
        self.assertIn("live_state_after", result.trace[0])


if __name__ == "__main__":
    unittest.main()
