import unittest
from unittest.mock import patch

from lam.interface.desktop_sequence import SequenceResult
from lam.learn.skill_runtime import SkillPracticeRuntime


class TestSkillPracticeRuntime(unittest.TestCase):
    def setUp(self) -> None:
        self.skill = {
            "skill_id": "skill_power_bi_kpi_dashboard",
            "skill_name": "Topic Mastery - Power BI KPI dashboard",
            "topic": "Power BI KPI dashboard",
            "app_context": {"app_name": "power bi desktop"},
            "workflow": [
                {
                    "step": 1,
                    "description": "Open the KPI visual pane",
                    "action_type": "click",
                    "risk_level": "low",
                    "approval_required": False,
                    "selector_suggestions": [{"kind": "label", "value": "Visualizations"}],
                    "ui_grounding": {"expected_state": {"labels": ["Visualizations"]}},
                    "checkpoint_id": "open_visual_pane",
                    "checkpoint_name": "1. Click - Open the KPI visual pane",
                    "confidence": 0.84,
                },
                {
                    "step": 2,
                    "description": "Validate KPI output",
                    "action_type": "validate",
                    "risk_level": "low",
                    "approval_required": False,
                    "selector_suggestions": [{"kind": "label", "value": "KPI"}],
                    "ui_grounding": {"expected_state": {"labels": ["KPI", "result"]}},
                    "checkpoint_id": "validate_kpi",
                    "checkpoint_name": "2. Validate - Validate KPI output",
                    "confidence": 0.86,
                },
            ],
        }

    def test_build_preview_creates_checkpoint_policy(self) -> None:
        preview = SkillPracticeRuntime().build_preview(self.skill)
        self.assertTrue(preview["checkpoints"])
        self.assertEqual(preview["checkpoint_policy"]["policy"], "checkpoint_by_checkpoint")
        self.assertGreaterEqual(len(preview["plan"]["steps"]), 4)

    def test_execute_practice_records_checkpoint_runs(self) -> None:
        with patch(
            "lam.learn.skill_runtime.execute_plan",
            return_value=SequenceResult(True, [], [{"step": 0, "action": "assert_state", "ok": True}], 0, True, False, "", {}),
        ):
            result = SkillPracticeRuntime().execute_practice(self.skill)
        self.assertTrue(result["ok"])
        self.assertEqual(len(result["checkpoint_runs"]), 3)


if __name__ == "__main__":
    unittest.main()
