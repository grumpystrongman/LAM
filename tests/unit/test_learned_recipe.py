import json
import shutil
import time
import unittest
from pathlib import Path

from lam.interface.learned_recipe import RecipeCritic, RecipeMemory, build_learned_recipe, recipe_to_instruction


class TestLearnedRecipe(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path("data") / "test_artifacts" / "learned_recipe_case"
        shutil.rmtree(self.root, ignore_errors=True)
        self.root.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def test_build_recipe_from_demonstration(self) -> None:
        recipe = build_learned_recipe(
            "gmail",
            [
                {"ts": 1.0, "action": "click", "payload": {"selector": {"value": "Compose", "metadata": {"name": "Compose"}}}},
                {"ts": 2.0, "action": "type_text", "payload": {"text": "person@example.com"}},
                {"ts": 3.0, "action": "hotkey", "payload": {"keys": "enter"}},
            ],
        )
        payload = recipe.to_dict()
        self.assertEqual(payload["app_name"], "gmail")
        self.assertTrue(payload["steps"])
        self.assertTrue(payload["required_inputs"])
        self.assertTrue(payload["success_signals"])
        self.assertGreater(payload["confidence"], 0.5)

    def test_recipe_instruction_uses_placeholders(self) -> None:
        recipe = build_learned_recipe(
            "chatgpt",
            [
                {"ts": 1.0, "action": "type_text", "payload": {"text": "hello world"}},
                {"ts": 2.0, "action": "hotkey", "payload": {"keys": "enter"}},
            ],
        )
        instruction = recipe_to_instruction(recipe)
        self.assertIn("type <text_input_1>", instruction)
        self.assertIn("press enter", instruction)

    def test_recipe_memory_round_trip(self) -> None:
        recipe = build_learned_recipe(
            "notepad",
            [{"ts": 1.0, "action": "type_text", "payload": {"text": "draft note"}}],
        )
        memory = RecipeMemory(self.root / "recipes")
        path = memory.save(recipe)
        self.assertTrue(Path(path).exists())
        items = memory.list_for_app("notepad")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["app_name"], "notepad")
        families = memory.list_families_for_app("notepad")
        self.assertEqual(len(families), 1)
        self.assertEqual(families[0]["variant_count"], 1)

    def test_recipe_memory_groups_variants_into_family(self) -> None:
        memory = RecipeMemory(self.root / "recipes")
        recipe_a = build_learned_recipe(
            "gmail",
            [{"ts": 1.0, "action": "click", "payload": {"selector": {"value": "Compose", "metadata": {"name": "Compose"}}}}],
            observation_frames=[
                {"index": 0, "action": "click", "app_name": "gmail", "target_label": "Compose", "target_role": "Button", "selector": {"value": "Compose"}, "selector_candidates": [{"strategy": "text", "value": "Compose"}], "typed_text": "", "hotkey": "", "expected_state": "compose visible", "note": ""},
            ],
            observation_segments=[
                {"segment_type": "navigation", "purpose": "Open compose", "start_index": 0, "end_index": 0, "actions": ["click"]},
            ],
        )
        recipe_b = build_learned_recipe(
            "gmail",
            [{"ts": 1.0, "action": "click", "payload": {"selector": {"value": "Compose", "metadata": {"name": "Compose"}}}}],
            observation_frames=[
                {"index": 0, "action": "click", "app_name": "gmail", "target_label": "Compose", "target_role": "Button", "selector": {"value": "Compose"}, "selector_candidates": [{"strategy": "text", "value": "Compose"}], "typed_text": "", "hotkey": "", "expected_state": "compose visible", "note": ""},
            ],
            observation_segments=[
                {"segment_type": "navigation", "purpose": "Open compose", "start_index": 0, "end_index": 0, "actions": ["click"]},
            ],
        )
        memory.save(recipe_a)
        memory.save(recipe_b)
        families = memory.list_families_for_app("gmail")
        self.assertEqual(len(families), 1)
        self.assertEqual(families[0]["variant_count"], 2)
        checkpoint_map = families[0].get("checkpoint_map", [])
        self.assertEqual(len(checkpoint_map), 1)
        self.assertEqual(checkpoint_map[0]["variant_count"], 2)

    def test_record_variant_outcome_updates_family_history(self) -> None:
        memory = RecipeMemory(self.root / "recipes")
        recipe = build_learned_recipe(
            "gmail",
            [{"ts": 1.0, "action": "click", "payload": {"selector": {"value": "Compose", "metadata": {"name": "Compose"}}}}],
            observation_frames=[
                {"index": 0, "action": "click", "app_name": "gmail", "target_label": "Compose", "target_role": "Button", "selector": {"value": "Compose"}, "selector_candidates": [{"strategy": "text", "value": "Compose"}], "typed_text": "", "hotkey": "", "expected_state": "compose visible", "note": ""},
            ],
            observation_segments=[
                {"segment_type": "navigation", "purpose": "Open compose", "start_index": 0, "end_index": 0, "actions": ["click"]},
            ],
        )
        memory.save(recipe)
        result = memory.record_variant_outcome(
            family_id=recipe.family_id,
            recipe_id=recipe.recipe_id,
            ok=True,
            reason="passed",
            current_state={"app_name": "gmail", "visible_labels": ["Compose"]},
            checkpoint_id=recipe.state_snapshots[0]["checkpoint_id"],
            checkpoint_name=recipe.state_snapshots[0]["checkpoint_name"],
        )
        self.assertTrue(result["ok"])
        family = memory.load_family(recipe.family_id)
        history = family["variants"][0].get("replay_history", {})
        self.assertEqual(history.get("success_count"), 1)
        self.assertEqual(history.get("last_outcome"), "success")
        self.assertEqual(history.get("recent_runs", [])[-1].get("checkpoint_id"), recipe.state_snapshots[0]["checkpoint_id"])
        checkpoint_map = family.get("checkpoint_map", [])
        self.assertTrue(checkpoint_map)
        self.assertTrue(checkpoint_map[0]["trend_points"])

    def test_branch_health_demotes_or_prunes_failing_variant(self) -> None:
        memory = RecipeMemory(self.root / "recipes")
        recipe = build_learned_recipe(
            "gmail",
            [{"ts": 1.0, "action": "click", "payload": {"selector": {"value": "Compose", "metadata": {"name": "Compose"}}}}],
        )
        memory.save(recipe)
        for _ in range(4):
            memory.record_variant_outcome(
                family_id=recipe.family_id,
                recipe_id=recipe.recipe_id,
                ok=False,
                reason="failed",
                current_state={"app_name": "gmail"},
            )
        family = memory.load_family(recipe.family_id)
        health = family["variants"][0].get("branch_health", {})
        self.assertIn(health.get("status"), {"demoted", "pruned"})

    def test_branch_health_can_retire_stale_variant(self) -> None:
        memory = RecipeMemory(self.root / "recipes")
        recipe = build_learned_recipe(
            "gmail",
            [{"ts": 1.0, "action": "click", "payload": {"selector": {"value": "Compose", "metadata": {"name": "Compose"}}}}],
        )
        memory.save(recipe)
        family = memory.load_family(recipe.family_id)
        family["variants"][0]["replay_history"] = {
            "success_count": 1,
            "failure_count": 0,
            "last_used_at": time.time() - (24 * 140 * 3600),
            "recent_runs": [],
        }
        family = memory._refresh_family_health(family)  # type: ignore[attr-defined]
        self.assertEqual(family["variants"][0]["branch_health"]["status"], "retired")

    def test_recipe_critic_flags_missing_success_signal(self) -> None:
        recipe = build_learned_recipe(
            "browser",
            [{"ts": 1.0, "action": "click", "payload": {"selector": {"value": "", "metadata": {}}}}],
        )
        critic = RecipeCritic().evaluate(recipe).to_dict()
        self.assertIn("low_confidence_selectors", critic["issues"])
        self.assertFalse(critic["passed"])

    def test_checkpoint_map_suggests_base_variant_and_diffs(self) -> None:
        memory = RecipeMemory(self.root / "recipes")
        recipe_a = build_learned_recipe(
            "gmail",
            [{"ts": 1.0, "action": "click", "payload": {"selector": {"value": "Compose", "metadata": {"name": "Compose"}}}}],
            observation_frames=[
                {"index": 0, "action": "click", "app_name": "gmail", "target_label": "Compose", "target_role": "Button", "selector": {"value": "Compose"}, "selector_candidates": [{"strategy": "text", "value": "Compose"}], "typed_text": "", "hotkey": "", "expected_state": "compose visible", "note": ""},
            ],
            observation_segments=[
                {"segment_type": "navigation", "purpose": "Open compose", "start_index": 0, "end_index": 0, "actions": ["click"]},
            ],
        )
        recipe_b = build_learned_recipe(
            "gmail",
            [{"ts": 1.0, "action": "click", "payload": {"selector": {"value": "Compose", "metadata": {"name": "Compose"}}}}],
            observation_frames=[
                {"index": 0, "action": "click", "app_name": "gmail", "target_label": "Compose", "target_role": "Button", "selector": {"value": "Compose"}, "selector_candidates": [{"strategy": "text", "value": "Compose"}], "typed_text": "", "hotkey": "", "expected_state": "compose visible", "note": ""},
            ],
            observation_segments=[
                {"segment_type": "navigation", "purpose": "Open compose", "start_index": 0, "end_index": 0, "actions": ["click"]},
            ],
        )
        memory.save(recipe_a)
        memory.save(recipe_b)
        memory.record_variant_outcome(
            family_id=recipe_a.family_id,
            recipe_id=recipe_a.recipe_id,
            ok=True,
            reason="passed",
            checkpoint_id=recipe_a.state_snapshots[0]["checkpoint_id"],
            checkpoint_name=recipe_a.state_snapshots[0]["checkpoint_name"],
        )
        memory.record_variant_outcome(
            family_id=recipe_a.family_id,
            recipe_id=recipe_b.recipe_id,
            ok=False,
            reason="failed",
            checkpoint_id=recipe_b.state_snapshots[0]["checkpoint_id"],
            checkpoint_name=recipe_b.state_snapshots[0]["checkpoint_name"],
        )
        family = memory.load_family(recipe_a.family_id)
        checkpoint = family["checkpoint_map"][0]
        self.assertTrue(checkpoint["suggested_base_variant"])
        self.assertTrue(checkpoint["variant_diffs"])


if __name__ == "__main__":
    unittest.main()
