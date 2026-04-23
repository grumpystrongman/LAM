import tempfile
import unittest
from pathlib import Path

from lam.interface.human_judgment import (
    ActionCritic,
    EleganceBudget,
    NegativeMemory,
    QualityCritic,
    action_critic,
    assess_result_quality,
)


class TestHumanJudgment(unittest.TestCase):
    def test_action_critic_blocks_missing_target(self) -> None:
        ok, reason = action_critic("open_tab", "", already_open=False)
        self.assertFalse(ok)
        self.assertEqual(reason, "missing_target")

    def test_result_quality_penalizes_generic_search_page(self) -> None:
        q = assess_result_quality(
            title="Whiskey",
            url="https://example.com/search?q=whiskey",
            snippet="Category results",
            query="best whiskey",
            locality_terms=[],
        )
        self.assertIn(q.level, {"low", "medium"})
        self.assertLessEqual(q.score, 1)

    def test_negative_memory_marks_and_reads_bad_url(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "mem.json"
            mem = NegativeMemory(path=p)
            self.assertFalse(mem.is_bad_url("https://example.com/a"))
            mem.mark_bad_url("https://example.com/a", "low_quality")
            self.assertTrue(mem.is_bad_url("https://example.com/a"))

    def test_action_critic_blocks_shortest_path_when_reusable_target_exists(self) -> None:
        ok, reason = action_critic(
            "open_tab",
            "https://mail.google.com/",
            already_open=False,
            context={"reusable_target": "https://mail.google.com/mail/u/0/#inbox"},
        )
        self.assertFalse(ok)
        self.assertEqual(reason, "shortest_path_reuse_existing_state")

    def test_action_critic_blocks_repeated_loop(self) -> None:
        ok, reason = action_critic(
            "open_tab",
            "https://mail.google.com/",
            context={
                "recent_actions": [
                    "open_tab:https://mail.google.com/",
                    "open_tab:https://mail.google.com/",
                ]
            },
        )
        self.assertFalse(ok)
        self.assertEqual(reason, "loop_detected_repeated_action_target")

    def test_action_critic_class_produces_weighted_decision(self) -> None:
        decision = ActionCritic().evaluate(
            next_action="open_tab",
            target="https://mail.google.com/",
            context={"reusable_target": "https://mail.google.com/mail/u/0/#inbox"},
        )
        self.assertFalse(decision.allow)
        self.assertLess(decision.score, 100.0)
        self.assertGreater(decision.elegance_cost, 0)

    def test_quality_critic_scores_with_evidence(self) -> None:
        quality = QualityCritic().evaluate(
            title="Durham NC local product listing",
            url="https://shop.example.com/item/whiskey-durham-nc",
            snippet="In stock in Durham NC with price details.",
            query="most expensive whiskey in durham nc",
            locality_terms=["durham nc"],
            evidence_count=2,
        )
        self.assertIn(quality.level, {"medium", "high"})
        self.assertGreater(quality.score, 0.0)

    def test_elegance_budget_tracks_consumption(self) -> None:
        budget = EleganceBudget(total=20)
        budget.consume(3, "query_refinement")
        budget.consume(2, "retry")
        snap = budget.snapshot()
        self.assertEqual(snap["remaining"], 15)
        self.assertEqual(snap["consumed"], 5)


if __name__ == "__main__":
    unittest.main()
