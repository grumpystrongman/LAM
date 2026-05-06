import time
import unittest

from lam.interface.learned_recipe import build_learned_recipe
from lam.interface.teach_runtime import DemonstrationSegmenter, ScreenObservationStream, TeachReplayRuntime


class TestTeachRuntime(unittest.TestCase):
    def test_observation_stream_builds_frames(self) -> None:
        frames = ScreenObservationStream().build(
            app_name="gmail",
            compressed_events=[
                {"ts": 1.0, "action": "click", "payload": {"selector": {"value": "Compose", "metadata": {"name": "Compose"}}}},
                {"ts": 2.0, "action": "type_text", "payload": {"text": "person@example.com"}},
            ],
        )
        self.assertEqual(len(frames), 2)
        self.assertEqual(frames[0]["target_label"], "Compose")
        self.assertEqual(frames[1]["typed_text"], "person@example.com")

    def test_segmenter_groups_semantic_steps(self) -> None:
        frames = [
            {"action": "click", "target_label": "Compose", "hotkey": ""},
            {"action": "type_text", "target_label": "", "hotkey": ""},
            {"action": "hotkey", "target_label": "", "hotkey": "enter"},
        ]
        segments = DemonstrationSegmenter().segment(frames)
        self.assertEqual(len(segments), 3)
        self.assertEqual(segments[0]["segment_type"], "navigation")
        self.assertEqual(segments[1]["segment_type"], "data_entry")
        self.assertEqual(segments[2]["segment_type"], "commit")

    def test_replay_runtime_builds_adaptive_plan(self) -> None:
        recipe = build_learned_recipe(
            "gmail",
            [
                {"ts": 1.0, "action": "click", "payload": {"selector": {"value": "Compose", "metadata": {"name": "Compose"}}}},
                {"ts": 2.0, "action": "type_text", "payload": {"text": "person@example.com"}},
                {"ts": 3.0, "action": "hotkey", "payload": {"keys": "enter"}},
            ],
        )
        preview = TeachReplayRuntime().build_plan(recipe=recipe, input_bindings={"email_input": "leader@example.com"})
        self.assertTrue(preview["ok"])
        self.assertTrue(preview["can_autorun"])
        actions = [step["action"] for step in preview["steps"]]
        self.assertIn("open_app", actions)
        self.assertIn("assert_visible", actions)
        self.assertIn("click", actions)
        self.assertIn("type_text", actions)
        type_step = next(step for step in preview["steps"] if step["action"] == "type_text")
        self.assertEqual(type_step["text"], "leader@example.com")
        self.assertTrue(preview["state_checks"])

    def test_replay_runtime_includes_segment_state_snapshots(self) -> None:
        frames = ScreenObservationStream().build(
            app_name="gmail",
            compressed_events=[
                {"ts": 1.0, "action": "click", "payload": {"selector": {"value": "Compose", "metadata": {"name": "Compose"}}}},
                {"ts": 2.0, "action": "type_text", "payload": {"text": "person@example.com"}},
            ],
        )
        segments = DemonstrationSegmenter().segment(frames)
        recipe = build_learned_recipe(
            "gmail",
            [
                {"ts": 1.0, "action": "click", "payload": {"selector": {"value": "Compose", "metadata": {"name": "Compose"}}}},
                {"ts": 2.0, "action": "type_text", "payload": {"text": "person@example.com"}},
            ],
            observation_frames=frames,
            observation_segments=segments,
        )
        preview = TeachReplayRuntime().build_plan(recipe=recipe, input_bindings={"email_input": "leader@example.com"})
        self.assertTrue(preview["state_snapshots"])
        self.assertIn("assert_state", [step["action"] for step in preview["steps"]])
        self.assertTrue(preview["state_snapshots"][0]["checkpoint_name"])

    def test_choose_variant_prefers_matching_family_branch(self) -> None:
        runtime = TeachReplayRuntime()
        family = {
            "family_id": "gmail_compose",
            "variants": [
                {
                    "recipe_id": "variant_a",
                    "app_name": "gmail",
                    "learned_goal": "Open gmail and complete a typed workflow.",
                    "confidence": 0.7,
                    "state_snapshots": [{"precondition_selectors": [{"strategy": "text", "value": "Compose"}]}],
                },
                {
                    "recipe_id": "variant_b",
                    "app_name": "gmail",
                    "learned_goal": "Open gmail and complete a typed workflow.",
                    "confidence": 0.9,
                    "state_snapshots": [{"precondition_selectors": [{"strategy": "text", "value": "Drafts"}]}],
                },
            ],
        }
        choice = runtime.choose_variant(family=family, current_state={"app_name": "gmail", "visible_labels": ["Compose"]})
        self.assertTrue(choice["ok"])
        self.assertEqual(choice["selected_variant"]["recipe_id"], "variant_a")

    def test_build_plan_from_family_returns_ranked_variants(self) -> None:
        frames = ScreenObservationStream().build(
            app_name="gmail",
            compressed_events=[
                {"ts": 1.0, "action": "click", "payload": {"selector": {"value": "Compose", "metadata": {"name": "Compose"}}}},
                {"ts": 2.0, "action": "type_text", "payload": {"text": "person@example.com"}},
            ],
        )
        segments = DemonstrationSegmenter().segment(frames)
        recipe = build_learned_recipe(
            "gmail",
            [
                {"ts": 1.0, "action": "click", "payload": {"selector": {"value": "Compose", "metadata": {"name": "Compose"}}}},
                {"ts": 2.0, "action": "type_text", "payload": {"text": "person@example.com"}},
            ],
            observation_frames=frames,
            observation_segments=segments,
        ).to_dict()
        alt = dict(recipe)
        alt["recipe_id"] = f"alt_{int(time.time())}"
        alt["confidence"] = 0.4
        alt["state_snapshots"] = [{"precondition_selectors": [{"strategy": "text", "value": "Drafts"}]}]
        family = {"family_id": "gmail_family", "variants": [alt, recipe]}
        preview = TeachReplayRuntime().build_plan(
            family=family,
            input_bindings={"email_input": "leader@example.com"},
            current_state={"app_name": "gmail", "visible_labels": ["Compose"]},
        )
        self.assertTrue(preview["ranked_variants"])
        self.assertEqual(preview["selected_variant"]["recipe_id"], recipe["recipe_id"])

    def test_choose_variant_uses_success_history(self) -> None:
        runtime = TeachReplayRuntime()
        family = {
            "family_id": "gmail_compose",
            "variants": [
                {
                    "recipe_id": "variant_a",
                    "app_name": "gmail",
                    "learned_goal": "Open gmail and complete a typed workflow.",
                    "confidence": 0.7,
                    "replay_history": {"success_count": 4, "failure_count": 0},
                    "steps": [{"action": "click", "target": "Compose", "selector": {"value": "Compose"}}],
                },
                {
                    "recipe_id": "variant_b",
                    "app_name": "gmail",
                    "learned_goal": "Open gmail and complete a typed workflow.",
                    "confidence": 0.9,
                    "replay_history": {"success_count": 0, "failure_count": 4},
                    "steps": [{"action": "click", "target": "Compose", "selector": {"value": "Compose"}}],
                },
            ],
        }
        choice = runtime.choose_variant(family=family, current_state={"app_name": "gmail", "visible_labels": ["Compose"]})
        self.assertEqual(choice["selected_variant"]["recipe_id"], "variant_a")

    def test_should_reassign_branch_only_on_post_state_failure(self) -> None:
        runtime = TeachReplayRuntime()
        self.assertTrue(
            runtime.should_reassign_branch(
                [
                    {"action": "click", "ok": True},
                    {"action": "assert_state", "phase": "post", "ok": False, "error": "missing compose"},
                ]
            )
        )

    def test_reassignment_checkpoint_returns_failed_post_source_index(self) -> None:
        runtime = TeachReplayRuntime()
        checkpoint = runtime.reassignment_checkpoint(
            [
                {"action": "click", "ok": True},
                {"action": "assert_state", "phase": "post", "ok": False, "source_index": 4, "error": "missing compose"},
            ]
        )
        self.assertEqual(checkpoint, 4)

    def test_reassignment_segment_returns_failed_post_segment_index(self) -> None:
        runtime = TeachReplayRuntime()
        segment = runtime.reassignment_segment(
            [
                {"action": "assert_state", "phase": "post", "ok": False, "segment_index": 2, "source_index": 4, "checkpoint_id": "commit_send"},
            ]
        )
        self.assertEqual(segment, 2)
        self.assertEqual(
            runtime.reassignment_checkpoint_id(
                [
                    {"action": "assert_state", "phase": "post", "ok": False, "segment_index": 2, "source_index": 4, "checkpoint_id": "commit_send"},
                ]
            ),
            "commit_send",
        )
        self.assertFalse(
            runtime.should_reassign_branch(
                [
                    {"action": "assert_state", "phase": "pre", "ok": False, "error": "not visible"},
                ]
            )
        )


if __name__ == "__main__":
    unittest.main()
