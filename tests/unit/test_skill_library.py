import shutil
import unittest
from pathlib import Path

from lam.learn.skill_library import SkillLibrary
from lam.learn.source_adapters import adapt_source, adapter_summary


class TestSkillLibrary(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path("data") / "test_artifacts" / "skill_library_case"
        shutil.rmtree(self.root, ignore_errors=True)
        self.root.mkdir(parents=True, exist_ok=True)
        self.library = SkillLibrary(self.root / "skills")
        self.skill = {
            "skill_id": "skill_power_bi_kpi_dashboard",
            "skill_name": "Topic Mastery - Power BI KPI dashboard",
            "topic": "Power BI KPI dashboard",
            "workflow": [{"step": 1, "description": "Open Power BI Desktop", "selector_suggestions": [{"kind": "label", "value": "Open"}]}],
            "confidence_score": 0.82,
            "source_urls": ["https://youtube.example/powerbi-seed"],
        }

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def test_adapt_source_and_summary(self) -> None:
        adapted = adapt_source({"source_url": "https://www.youtube.com/watch?v=abc", "source_type": "video"})
        self.assertEqual(adapted["platform"], "youtube")
        summary = adapter_summary([adapted, {"source_url": "https://docs.example/x", "source_type": "docs"}])
        self.assertIn("youtube", summary["platforms"])
        self.assertGreaterEqual(summary["version_sensitive_count"], 1)

    def test_save_version_and_diff(self) -> None:
        first = self.library.save_skill(dict(self.skill), editor_note="initial")
        changed = dict(self.skill)
        changed["workflow"] = [{"step": 1, "description": "Open Power BI Desktop and validate model", "selector_suggestions": []}]
        second = self.library.save_skill(changed, editor_note="refined")
        self.assertEqual(first["version"], "1.0")
        self.assertEqual(second["version"], "1.1")
        diff = self.library.diff_versions(self.skill["skill_id"], "1.0", "1.1")
        self.assertIn("validate model", diff["unified_diff"])

    def test_feedback_and_refresh_plan(self) -> None:
        saved = self.library.save_skill(dict(self.skill), editor_note="initial")
        summary = self.library.record_feedback(self.skill["skill_id"], saved["version"], rating=5, comment="Worked well", signal="useful")
        self.assertEqual(summary["count"], 1)
        self.assertEqual(summary["average_rating"], 5.0)
        refresh = self.library.build_refresh_plan(self.skill["skill_id"], saved["version"], reason="stale_docs")
        self.assertEqual(refresh["skill_id"], self.skill["skill_id"])
        self.assertIn("Refresh the learned skill", refresh["recommended_instruction"])
        self.assertIn("refresh_window_days", refresh)

    def test_practice_schedule_payload(self) -> None:
        saved = self.library.save_skill(dict(self.skill), editor_note="initial")
        payload = self.library.practice_schedule_payload(self.skill["skill_id"], saved["version"])
        self.assertIn("automation_name", payload)
        self.assertIn("safely", payload["instruction"])
        self.assertIn("checkpoint_policy", payload)

    def test_editor_schema_and_history_tracking(self) -> None:
        saved = self.library.save_skill(dict(self.skill), editor_note="initial")
        schema = self.library.editor_schema(self.library.load_skill(self.skill["skill_id"], saved["version"]))
        self.assertTrue(schema["fields"])
        practice = self.library.record_practice_run(self.skill["skill_id"], saved["version"], {"ok": True, "checkpoint_count": 1})
        refresh = self.library.record_refresh_run(self.skill["skill_id"], saved["version"], {"status": "real_complete", "selected_sources": 4, "runtime_quality": "strong_live_mix"})
        self.assertEqual(practice["count"], 1)
        self.assertEqual(refresh["count"], 1)


if __name__ == "__main__":
    unittest.main()
