import json
import shutil
import unittest
from pathlib import Path
from unittest.mock import patch

from lam.interface.search_agent import execute_instruction
from lam.learn.audio_transcriber import transcribe_audio_fallback
from lam.learn.contradiction_detector import ContradictionDetector
from lam.learn.learn_memory import LearnMemory
from lam.learn.mastery_guide_builder import build_mastery_guide
from lam.learn.models import LearnedSkill
from lam.learn.multi_source_synthesizer import MultiSourceSynthesizer
from lam.learn.skill_library import SkillLibrary
from lam.learn.skill_builder import build_skill
from lam.learn.skill_validator import validate_skill
from lam.learn.source_ranker import SourceRanker
from lam.learn.topic_mastery_runtime import TopicMasteryRuntime
from lam.learn.transcript_extractor import extract_transcript
from lam.operator_platform.memory_store import MemoryStore
from lam.operator_platform.ui_cards import build_platform_cards


class TestTopicMasteryLearnMode(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path("data") / "test_artifacts" / "topic_mastery_case"
        shutil.rmtree(self.root, ignore_errors=True)
        self.root.mkdir(parents=True, exist_ok=True)
        self.sources = [
            {
                "source_url": "https://youtube.example/powerbi-seed",
                "title": "Power BI KPI dashboard tutorial",
                "source_type": "video",
                "channel": "BI Coach",
                "captions": {"official": "Open Power BI Desktop. Create KPI measures. Build the dashboard. Validate the visuals."},
                "snippet": "Step-by-step Power BI KPI dashboard tutorial.",
                "upload_date": "2026-05-01",
            },
            {
                "source_url": "https://youtube.example/powerbi-related-1",
                "title": "How to build a Power BI KPI dashboard walkthrough",
                "source_type": "video",
                "channel": "Analytics Lab",
                "captions": {"auto": "Create the report page. Add KPI cards. Configure filters. Review the dashboard output."},
                "snippet": "Recent walkthrough for KPI dashboards.",
                "upload_date": "2026-04-25",
            },
            {
                "source_url": "https://youtube.example/powerbi-related-2",
                "title": "Advanced KPI dashboard best practices",
                "source_type": "video",
                "channel": "Official Fabric",
                "captions": {"official": "Always use a date table. Validate relationships. Review performance before sharing."},
                "snippet": "Advanced dashboard guidance.",
                "upload_date": "2026-04-20",
            },
            {
                "source_url": "https://docs.example/powerbi-kpi",
                "title": "Official docs for Power BI KPI dashboard design",
                "source_type": "docs",
                "snippet": "Official documentation for building KPI dashboards in Power BI.",
                "upload_date": "2026-04-28",
            },
            {
                "source_url": "https://blog.example/powerbi-kpi-mistakes",
                "title": "Power BI KPI dashboard mistakes to avoid",
                "source_type": "blog",
                "snippet": "Best practices and common mistakes for KPI dashboards.",
            },
            {
                "source_url": "https://forum.example/powerbi-kpi-troubleshooting",
                "title": "Troubleshooting Power BI KPI dashboard filters",
                "source_type": "forum",
                "snippet": "Troubleshooting filter context and KPI card behavior.",
            },
        ]

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def test_seed_video_creates_learn_mission(self) -> None:
        runtime = TopicMasteryRuntime(memory_store=MemoryStore(path=self.root / "seed.db"))
        result = runtime.run(
            "Learn how to build a Power BI KPI dashboard from this YouTube tutorial and get well versed in the topic: https://youtube.example/powerbi-seed",
            context={"workspace_dir": str(self.root / "seed_run"), "mock_sources": self.sources},
        )
        mission = result["learn_mission"]
        self.assertEqual(mission["input_mode"], "seed_video")
        self.assertEqual(mission["seed_url"], "https://youtube.example/powerbi-seed")
        self.assertIn("Power BI KPI dashboard", mission["topic"])

    def test_topic_only_input_creates_source_discovery_plan(self) -> None:
        runtime = TopicMasteryRuntime(memory_store=MemoryStore(path=self.root / "topic.db"))
        result = runtime.run(
            "Learn how to create a grant proposal budget narrative and build me a reusable playbook.",
            context={"workspace_dir": str(self.root / "topic_run")},
        )
        self.assertEqual(result["learn_mission"]["input_mode"], "topic_only")
        self.assertGreaterEqual(result["source_discovery"]["found"], 4)
        self.assertGreaterEqual(result["source_discovery"]["selected"], 3)

    def test_source_ranker_ranks_sources(self) -> None:
        ranked = SourceRanker().rank("Power BI KPI dashboard", self.sources)
        self.assertTrue(ranked)
        self.assertEqual(ranked[0]["rank"], 1)
        self.assertGreaterEqual(ranked[0]["score"], ranked[-1]["score"])
        self.assertIn("expected_use", ranked[0])

    def test_transcript_extraction_fallback_is_handled(self) -> None:
        source = {"source_url": "https://video.example/fallback", "audio_text": "Open the tool and validate the output."}
        transcript = extract_transcript(source)
        self.assertEqual(transcript["method"], "audio_fallback")
        self.assertGreater(transcript["coverage"], 0.0)
        self.assertIn("validate", transcript["text"].lower())
        self.assertIn("method", transcribe_audio_fallback(source))

    def test_multi_source_synthesizer_merges_source_notes(self) -> None:
        analyses = [
            {
                "source_url": "a",
                "title": "Source A",
                "highlights": ["Always use a date table.", "Validate relationships before building visuals."],
                "procedure_steps": [{"description": "Open Power BI Desktop", "action_type": "open", "support_count": 1}],
                "concepts": ["Power BI", "KPI"],
                "tools": ["Power BI"],
                "prerequisites": ["Desktop installed"],
                "variations": ["Alternative visuals"],
            },
            {
                "source_url": "b",
                "title": "Source B",
                "highlights": ["Always use a date table.", "Review filters before sharing."],
                "procedure_steps": [{"description": "Open Power BI Desktop", "action_type": "open", "support_count": 1}],
                "concepts": ["Dashboard", "KPI"],
                "tools": ["Power BI"],
                "prerequisites": ["Data model ready"],
                "variations": [],
            },
        ]
        synthesis = MultiSourceSynthesizer().synthesize("Power BI KPI dashboard", analyses)
        self.assertIn("topic_model", synthesis)
        self.assertTrue(synthesis["topic_model"]["core_concepts"])
        self.assertTrue(synthesis["consensus_workflow"])

    def test_contradiction_detector_identifies_conflicts(self) -> None:
        contradictions = ContradictionDetector().detect(
            [
                {"title": "Source A", "highlights": ["Always use bookmarks for navigation."]},
                {"title": "Source B", "highlights": ["Do not use bookmarks for navigation in this workflow."]},
            ]
        )
        self.assertTrue(contradictions)
        self.assertEqual(contradictions[0]["topic"], "bookmarks")

    def test_skill_builder_creates_structured_skill(self) -> None:
        synthesis = {
            "topic_model": {"required_tools": ["Power BI"], "prerequisites": ["Desktop installed"], "variations": ["Alternative layout"]},
            "consensus_workflow": [
                {"description": "Open Power BI Desktop", "action_type": "open", "target": "Power BI Desktop", "supporting_sources": ["a"], "timestamp_refs": ["00:10"], "confidence": 0.9, "risk_level": "low", "approval_required": False}
            ],
            "best_practices": ["Validate measures before publishing."],
            "confidence": 0.82,
        }
        skill = build_skill("Power BI KPI dashboard", synthesis, self.sources[:3])
        self.assertTrue(skill.skill_id)
        self.assertEqual(skill.domain, "topic_learning")
        self.assertTrue(skill.workflow)
        self.assertTrue(skill.source_urls)

    def test_mastery_guide_builder_creates_required_sections(self) -> None:
        skill = LearnedSkill(skill_id="skill_x", skill_name="Topic Mastery - Power BI KPI dashboard", topic="Power BI KPI dashboard", purpose="Playbook", domain="topic_learning")
        guide = build_mastery_guide(
            "Power BI KPI dashboard",
            {
                "topic_model": {"core_concepts": ["KPIs"], "required_tools": ["Power BI"]},
                "consensus_workflow": [{"description": "Open Power BI Desktop"}],
                "contradictions": [],
                "best_practices": ["Validate each visual."],
                "source_notes": [{"title": "Doc", "highlights": ["Use a date table."]}],
            },
            skill,
        )
        self.assertIn("## Executive summary", guide)
        self.assertIn("## Beginner workflow", guide)
        self.assertIn("## Reusable OpenLAMb skill", guide)

    def test_skill_validator_flags_low_confidence_executable_steps(self) -> None:
        skill = LearnedSkill(
            skill_id="skill_bad",
            skill_name="Low confidence",
            topic="Power BI KPI dashboard",
            purpose="Playbook",
            domain="topic_learning",
            workflow=[{"description": "Publish dashboard", "confidence": 0.4, "risk_level": "high", "approval_required": True}],
        )
        validation = validate_skill(skill)
        self.assertFalse(validation["passed"])
        self.assertIn("low_confidence_steps", validation["issues"])
        self.assertEqual(validation["executable_status"], "guided_only")

    def test_memory_saves_and_retrieves_learned_topic(self) -> None:
        memory = LearnMemory(MemoryStore(path=self.root / "memory.db"))
        payload = {"topic": "Power BI KPI dashboard", "confidence": 0.88, "skill": {"skill_name": "Topic Mastery - Power BI KPI dashboard"}}
        key = memory.save_topic(payload)
        self.assertEqual(key, "learned_topic:power bi kpi dashboard")
        stored = memory.get_topic("Power BI KPI dashboard")
        self.assertEqual(stored["topic"], "Power BI KPI dashboard")
        retrieved = memory.retrieve("Power BI KPI dashboard")
        self.assertIn("used", retrieved)

    def test_ui_card_payloads_include_source_discovery_and_learned_skill(self) -> None:
        runtime = TopicMasteryRuntime(memory_store=MemoryStore(path=self.root / "cards.db"))
        result = runtime.run(
            "Learn how to build a Power BI KPI dashboard from this YouTube tutorial and get well versed in the topic: https://youtube.example/powerbi-seed",
            context={"workspace_dir": str(self.root / "cards_run"), "mock_sources": self.sources},
        )
        cards = build_platform_cards(result)
        self.assertIn("learn_mission", cards)
        self.assertIn("source_discovery", cards)
        self.assertIn("video_analysis", cards)
        self.assertIn("learned_skill_card", cards)
        self.assertIn("mastery_guide_card", cards)
        self.assertIn("adapter_summary", cards["source_discovery"])

    def test_runtime_adds_ui_grounding_and_versioned_skill_metadata(self) -> None:
        runtime = TopicMasteryRuntime(memory_store=MemoryStore(path=self.root / "grounding.db"))
        runtime.skill_library = SkillLibrary(self.root / "grounding_library")
        result = runtime.run(
            "Learn how to build a Power BI KPI dashboard from this YouTube tutorial and get well versed in the topic: https://youtube.example/powerbi-seed",
            context={"workspace_dir": str(self.root / "grounding_run"), "mock_sources": self.sources},
        )
        skill = result["learned_skill"]
        self.assertTrue(skill["workflow"])
        self.assertIn("ui_grounding", skill["workflow"][0])
        self.assertIn("selector_suggestions", skill["workflow"][0])
        self.assertEqual(skill["version"], "1.0")
        self.assertIn("learned_skill_library", result)
        self.assertTrue(result["learned_skill_library"]["path"])
        self.assertIn("refresh_plan", result)
        self.assertIn("practice_preview", result)
        self.assertTrue(result["learned_skill"]["checkpoints"])

    def test_live_source_collector_improves_discovery_mode(self) -> None:
        runtime = TopicMasteryRuntime(memory_store=MemoryStore(path=self.root / "live.db"))

        def collector(**kwargs):
            return {
                "sources": [
                    {
                        "source_url": "https://www.youtube.com/watch?v=powerbi-live",
                        "title": "Live Power BI KPI dashboard tutorial",
                        "source_type": "video",
                        "snippet": "Step by step Power BI KPI tutorial with captions.",
                        "captions": {"official": "Open Power BI Desktop. Build KPI cards. Validate the dashboard."},
                        "upload_date": "2026-05-02",
                    },
                    {
                        "source_url": "https://learn.microsoft.com/powerbi/kpi",
                        "title": "Official Power BI KPI docs",
                        "source_type": "docs",
                        "snippet": "Official guidance for KPI dashboards.",
                        "upload_date": "2026-05-01",
                    },
                ]
            }

        result = runtime.run(
            "Learn how to build a Power BI KPI dashboard from this YouTube tutorial and get well versed in the topic: https://youtube.example/powerbi-seed",
            context={"workspace_dir": str(self.root / "live_run"), "source_collector": collector},
        )
        self.assertEqual(result["source_discovery"]["discovery_mode"], "live")
        self.assertGreaterEqual(result["source_discovery"]["adapter_summary"]["live_source_count"], 1)

    def test_critic_failure_triggers_revision_and_additional_source_behavior(self) -> None:
        runtime = TopicMasteryRuntime(memory_store=MemoryStore(path=self.root / "revise.db"))
        call_counter = {"count": 0}
        original = runtime._run_critics

        def flaky(selected, analyses, synthesis):
            call_counter["count"] += 1
            if call_counter["count"] == 1:
                return {name: {"passed": False, "score": 0.2, "reason": "force revision"} for name in ["source_quality", "transcript_coverage", "visual_grounding", "synthesis_quality", "procedure_completeness", "skill_executability", "safety", "mastery_guide"]}
            return original(selected, analyses, synthesis)

        with patch.object(runtime, "_run_critics", side_effect=flaky):
            result = runtime.run(
                "Learn how to build a Power BI KPI dashboard from this YouTube tutorial and get well versed in the topic: https://youtube.example/powerbi-seed light",
                context={"workspace_dir": str(self.root / "revise_run"), "mock_sources": self.sources},
            )
        self.assertGreaterEqual(call_counter["count"], 2)
        self.assertGreaterEqual(result["source_discovery"]["selected"], 4)

    def test_execute_instruction_routes_topic_mastery_mode(self) -> None:
        with patch("lam.learn.topic_mastery_runtime.discover_related_sources", return_value=self.sources):
            result = execute_instruction(
                "Learn how to build a Power BI KPI dashboard from this YouTube tutorial and get well versed in the topic: https://youtube.example/powerbi-seed",
                control_granted=True,
            )
        self.assertEqual(result["mode"], "topic_mastery_learn_mode")
        self.assertEqual(result["runtime_mode"], "execution_graph_runtime")
        self.assertIn("learned_skill", result)

    def test_acceptance_scenarios_include_topic_mastery_benchmark(self) -> None:
        payload = json.loads(Path("config/human_operator_scenarios.json").read_text(encoding="utf-8"))
        scenario_ids = {item.get("scenario_id") for item in payload.get("scenarios", [])}
        self.assertIn("L1", scenario_ids)


if __name__ == "__main__":
    unittest.main()
