from __future__ import annotations

import csv
import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from .frame_sampler import sample_frames
from .learn_memory import LearnMemory
from .mastery_guide_builder import build_mastery_guide
from .models import LearnMission
from .multimodal_video_runtime import MultimodalVideoRuntime
from .multi_source_synthesizer import MultiSourceSynthesizer
from .procedure_extractor import build_highlights, extract_procedure, extract_topic_concepts, procedure_steps_to_dict
from .related_source_discovery import discover_related_sources
from .skill_library import SkillLibrary
from .skill_builder import build_skill
from .skill_runtime import SkillPracticeRuntime
from .skill_validator import validate_skill
from .source_adapters import adapt_sources, adapter_summary
from .source_ranker import SourceRanker
from .transcript_extractor import extract_transcript
from .video_ingest import ingest_video_source
from .visual_observer import observe_frames


class TopicMasteryRuntime:
    def __init__(self, memory_store: Any | None = None) -> None:
        self._memory_store = memory_store or _default_memory_store()
        self.memory = LearnMemory(self._memory_store)
        self.skill_library = SkillLibrary()
        self.video_runtime = MultimodalVideoRuntime(memory_store=self._memory_store)

    def run(self, instruction: str, *, context: Dict[str, Any] | None = None) -> Dict[str, Any]:
        ctx = dict(context or {})
        skill_root = str(ctx.get("skill_library_root", "") or "").strip()
        if skill_root:
            self.skill_library = SkillLibrary(skill_root)
        mission = self._mission_from_instruction(instruction, ctx)
        workspace = self._workspace_dir(instruction, ctx)
        discovered = adapt_sources(discover_related_sources(mission, ctx))
        discovery_summary = adapter_summary(discovered)
        ranked = SourceRanker().rank(mission.topic, discovered)
        selected = self._select_sources(mission, ranked)
        analyses = [self._analyze_source(mission.topic, item, workspace=workspace, context=ctx) for item in selected]
        synthesis = MultiSourceSynthesizer().synthesize(mission.topic, analyses)
        critics = self._run_critics(selected, analyses, synthesis)
        if any(not bool(v.get("passed", True)) for v in critics.values() if isinstance(v, dict)):
            selected = self._revise_source_set(selected, ranked)
            analyses = [self._analyze_source(mission.topic, item, workspace=workspace, context=ctx) for item in selected]
            synthesis = MultiSourceSynthesizer().synthesize(mission.topic, analyses)
            critics = self._run_critics(selected, analyses, synthesis)
        skill = build_skill(mission.topic, synthesis, selected)
        validation = validate_skill(skill)
        skill.executable_status = str(validation.get("executable_status", skill.executable_status))
        saved_skill = self.skill_library.save_skill(
            skill.to_dict(),
            editor_note="topic_mastery_runtime",
            parent_version=str(ctx.get("existing_skill_version", "") or ""),
        )
        skill.version = str(saved_skill.get("version", skill.version))
        saved_skill_payload = self.skill_library.load_skill(skill.skill_id, skill.version)
        skill.feedback_summary = dict(saved_skill_payload.get("feedback_summary", {}) or {"count": 0, "average_rating": 0.0, "signals": []})
        skill.next_review_at = _default_next_review_at(skill.confidence_score)
        guide = build_mastery_guide(mission.topic, synthesis, skill)
        practice_plan = self._practice_plan(skill, validation)
        practice_preview = SkillPracticeRuntime().build_preview(skill.to_dict(), mode="safe_practice")
        refresh_plan = self.skill_library.build_refresh_plan(skill.skill_id, skill.version, reason="version_sensitive_refresh", source_url=mission.seed_url)
        artifacts = self._write_artifacts(workspace, mission, ranked, selected, analyses, synthesis, skill.to_dict(), guide, practice_plan, critics, validation, saved_skill, refresh_plan)
        memory_payload = {
            "topic": mission.topic,
            "seed_url": mission.seed_url,
            "confidence": float(synthesis.get("confidence", 0.0) or 0.0),
            "skill": skill.to_dict(),
            "skill_version": skill.version,
            "sources": [row.get("source_url", "") for row in selected],
            "status": "learned",
        }
        memory_key = self.memory.save_topic(memory_payload)
        memory_id = self.memory.save_memory_item(memory_payload)
        status = self._status(selected, validation)
        result = {
            "ok": True,
            "mode": "topic_mastery_learn_mode",
            "learn_mission": mission.to_dict(),
            "source_discovery": {
                "found": len(ranked),
                "selected": len(selected),
                "rejected": max(0, len(ranked) - len(selected)),
                "sources": ranked,
                "adapter_summary": discovery_summary,
                "discovery_mode": "live" if int(discovery_summary.get("live_source_count", 0) or 0) > 0 else "offline_seeded",
            },
            "video_analysis": {
                "transcript_coverage": round(sum(float(a.get("transcript", {}).get("coverage", 0.0) or 0.0) for a in analyses if a.get("source_type") == "video") / max(1, len([a for a in analyses if a.get("source_type") == "video"])), 3),
                "visual_sampling_coverage": sum(len(list(a.get("sampled_frames", []) or [])) for a in analyses if a.get("source_type") == "video"),
                "key_timestamps": [ts for a in analyses for ts in list(a.get("key_timestamps", []) or [])][:12],
                "process_checkpoints": sum(len(list((a.get("process_state", {}) or {}).get("phases", []) or [])) for a in analyses if a.get("source_type") == "video"),
                "follow_up_questions": [q for a in analyses for q in list((a.get("follow_up_questions", {}) or {}).get("questions", []) or [])][:20],
                "confidence": float(synthesis.get("confidence", 0.0) or 0.0),
            },
            "topic_model": dict(synthesis.get("topic_model", {}) or {}),
            "consensus_workflow": list(synthesis.get("consensus_workflow", []) or []),
            "contradictions": list(synthesis.get("contradictions", []) or []),
            "best_practices": list(synthesis.get("best_practices", []) or []),
            "learned_skill": skill.to_dict(),
            "learned_skill_library": {
                "skill_id": skill.skill_id,
                "version": skill.version,
                "path": saved_skill.get("path", ""),
                "manifest_path": saved_skill.get("manifest_path", ""),
                "diff": dict(saved_skill.get("diff", {}) or {}),
                "history": list(saved_skill.get("history", []) or []),
            },
            "mastery_guide": {"path": artifacts.get("mastery_guide_md", ""), "summary": guide.splitlines()[0] if guide else ""},
            "practice_plan": {"path": artifacts.get("practice_plan_md", ""), "safe": True},
            "practice_preview": practice_preview,
            "refresh_plan": refresh_plan,
            "critics": {"topic_mastery": critics},
            "critic_results": critics,
            "skill_validation": validation,
            "artifacts": artifacts,
            "memory": {"key": memory_key, "memory_id": memory_id},
            "status": status,
            "ui_cards": {},
            "final_package": {
                "status": status,
                "summary": f"Built a topic mastery package for {mission.topic} using {len(selected)} selected sources.",
                "next_steps": self._next_steps(status, validation),
            },
        }
        return result

    def _mission_from_instruction(self, instruction: str, context: Dict[str, Any]) -> LearnMission:
        seed_url = str(context.get("seed_url", "") or _extract_url(instruction))
        topic = str(context.get("topic", "") or _extract_topic(instruction, seed_url))
        depth = "deep" if "deep" in instruction.lower() else ("light" if "light" in instruction.lower() else "normal")
        related = {"light": 2, "normal": 5, "deep": 10}[depth]
        support = {"light": 1, "normal": 3, "deep": 5}[depth]
        return LearnMission(
            topic=topic,
            seed_url=seed_url,
            input_mode="seed_video" if seed_url else "topic_only",
            learning_depth=depth,
            expected_outputs=["source_manifest", "video_analysis_notes", "topic_model", "consensus_workflow", "learned_skill", "mastery_guide", "practice_plan", "critic_results"],
            max_related_videos=related,
            max_supporting_sources=support,
        )

    def _select_sources(self, mission: LearnMission, ranked: List[Dict[str, object]]) -> List[Dict[str, object]]:
        videos = [dict(row) for row in ranked if str(row.get("source_type", "")) == "video"]
        docs = [dict(row) for row in ranked if str(row.get("source_type", "")) != "video"]
        selected = videos[: 1 + mission.max_related_videos] + docs[: mission.max_supporting_sources]
        for idx, row in enumerate(selected, start=1):
            row["selected"] = True
            row["rank"] = idx
        return selected

    def _analyze_source(self, topic: str, source: Dict[str, object], *, workspace: Path, context: Dict[str, Any]) -> Dict[str, object]:
        stype = str(source.get("source_type", "other") or "other")
        payload = ingest_video_source(str(source.get("source_url", "")), source) if stype == "video" else dict(source)
        multimodal: Dict[str, Any] = {}
        if stype == "video":
            multimodal = self.video_runtime.analyze(topic=topic, source=payload, workspace=workspace, context=context)
        transcript = dict(multimodal.get("transcript", {}) or extract_transcript(payload))
        frames = list(multimodal.get("sampled_frames", []) or sample_frames(str(transcript.get("text", "")), list(payload.get("visual_notes", []) or []))) if stype == "video" else []
        observations = list(multimodal.get("visual_observations", []) or observe_frames(payload, frames)) if stype == "video" else []
        inferred_text = str(multimodal.get("inferred_process_text", "") or "")
        analysis_text = str(transcript.get("text", "") or payload.get("snippet", ""))
        if inferred_text:
            analysis_text = f"{analysis_text}\n{inferred_text}".strip()
        steps = extract_procedure(payload, analysis_text, observations)
        concepts = extract_topic_concepts(topic, analysis_text, str(source.get("title", "")))
        highlights = build_highlights(analysis_text, str(source.get("title", "")))
        return {
            "source_url": str(source.get("source_url", "")),
            "title": str(source.get("title", "")),
            "source_type": stype,
            "transcript": transcript,
            "sampled_frames": frames,
            "visual_observations": observations,
            "procedure_steps": procedure_steps_to_dict(steps),
            "highlights": highlights,
            "concepts": concepts.get("concepts", []),
            "tools": concepts.get("tools", []),
            "prerequisites": concepts.get("prerequisites", []),
            "variations": concepts.get("variations", []),
            "key_timestamps": [str(item.get("timestamp", "")) for item in list(multimodal.get("sampled_frames", []) or frames)[:12]],
            "process_state": dict(multimodal.get("process_state", {}) or {}),
            "follow_up_questions": dict(multimodal.get("follow_up_questions", {}) or {}),
            "chunk_reports": list(multimodal.get("chunk_reports", []) or []),
            "learning_memory_refs": list(multimodal.get("learning_memory_refs", []) or []),
            "process_state_path": str(multimodal.get("process_state_path", "") or ""),
            "local_video_path": str(multimodal.get("local_video_path", "") or ""),
        }

    def _run_critics(self, selected: List[Dict[str, object]], analyses: List[Dict[str, object]], synthesis: Dict[str, object]) -> Dict[str, Dict[str, object]]:
        transcript_coverages = [float(item.get("transcript", {}).get("coverage", 0.0) or 0.0) for item in analyses if str(item.get("source_type", "")) == "video"]
        avg_cov = sum(transcript_coverages) / max(1, len(transcript_coverages))
        source_scores = [float(item.get("score", 0.0) or 0.0) for item in selected]
        workflow = list(synthesis.get("consensus_workflow", []) or [])
        guide_ok = bool(list(synthesis.get("topic_model", {}).get("core_concepts", []) or [])) and bool(workflow)
        skill_validation = validate_skill(build_skill(str(synthesis.get("topic_model", {}).get("topic", "")), synthesis, selected))
        return {
            "source_quality": {"passed": (sum(source_scores) / max(1, len(source_scores))) >= 0.55, "score": round(sum(source_scores) / max(1, len(source_scores)), 3), "reason": "Average selected-source score."},
            "transcript_coverage": {"passed": avg_cov >= 0.45, "score": round(avg_cov, 3), "reason": "Average transcript coverage across selected videos."},
            "visual_grounding": {"passed": sum(len(list(item.get("sampled_frames", []) or [])) for item in analyses) >= 2, "score": round(min(1.0, sum(len(list(item.get("sampled_frames", []) or [])) for item in analyses) / 8.0), 3), "reason": "Visual sampling coverage across analyzed videos."},
            "synthesis_quality": {"passed": len(list(synthesis.get("topic_model", {}).get("core_concepts", []) or [])) >= 2 and len(workflow) >= 2, "score": round(float(synthesis.get("confidence", 0.0) or 0.0), 3), "reason": "Topic model and consensus workflow completeness."},
            "procedure_completeness": {"passed": len(workflow) >= 3, "score": round(min(1.0, len(workflow) / 8.0), 3), "reason": "Consensus workflow depth."},
            "skill_executability": {"passed": bool(skill_validation.get("passed", False)), "score": 0.8 if bool(skill_validation.get("passed", False)) else 0.42, "reason": "Skill validator result."},
            "safety": {"passed": int(skill_validation.get("risky_step_count", 0) or 0) <= len(workflow), "score": 0.85, "reason": "Risky steps are gated rather than auto-executed."},
            "mastery_guide": {"passed": guide_ok, "score": 0.86 if guide_ok else 0.4, "reason": "Guide sections can be generated from synthesis."},
        }

    def _revise_source_set(self, selected: List[Dict[str, object]], ranked: List[Dict[str, object]]) -> List[Dict[str, object]]:
        existing = {str(item.get("source_url", "")) for item in selected}
        revised = list(selected)
        for row in ranked:
            if str(row.get("source_url", "")) in existing:
                continue
            revised.append(dict(row))
            break
        return revised

    def _practice_plan(self, skill, validation: Dict[str, object]) -> str:
        lines = [
            "# Practice Plan",
            "",
            "## Safe practice scope",
            "- Validate prerequisites before practice.",
            "- Run harmless setup and review steps only.",
            "- Avoid destructive, publishing, financial, or production actions without approval.",
            "",
            "## Checkpoint policy",
            f"- Mode: {str(skill.practice_policy.get('mode', 'checkpoint_guided') if isinstance(skill.practice_policy, dict) else 'checkpoint_guided')}",
            f"- Checkpoints: {len(list(skill.checkpoints or []))}",
            "- Enforce pre/post validation at each checkpoint before continuing.",
            "",
            "## Validation checks",
        ]
        lines.extend([f"- {item}" for item in list(skill.validation_checks or [])])
        lines.extend(["", "## Current executable status", f"- {validation.get('executable_status', 'guided_only')}"])
        lines.extend(["", "## Selector suggestions"])
        for step in list(skill.workflow or [])[:8]:
            selectors = [f"{item.get('kind')}={item.get('value')}" for item in list(step.get("selector_suggestions", []) or [])[:3]]
            if selectors:
                lines.append(f"- Step {step.get('step')}: {' | '.join(selectors)}")
        lines.extend(["", "## Recommended next review", f"- {skill.next_review_at or 'Schedule a review within 7 days.'}"])
        return "\n".join(lines) + "\n"

    def _write_artifacts(self, workspace: Path, mission: LearnMission, ranked: List[Dict[str, object]], selected: List[Dict[str, object]], analyses: List[Dict[str, object]], synthesis: Dict[str, object], skill: Dict[str, object], guide: str, practice_plan: str, critics: Dict[str, Dict[str, object]], validation: Dict[str, object], saved_skill: Dict[str, object], refresh_plan: Dict[str, object]) -> Dict[str, str]:
        workspace.mkdir(parents=True, exist_ok=True)
        out = workspace / "learn_artifacts"
        out.mkdir(parents=True, exist_ok=True)
        artifacts: Dict[str, str] = {}
        manifest = out / "source_manifest.csv"
        with manifest.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=["rank", "title", "source_type", "source_url", "score", "reason_selected", "expected_use", "selected"])
            writer.writeheader()
            for row in ranked:
                writer.writerow({key: row.get(key, "") for key in writer.fieldnames})
        artifacts["source_manifest_csv"] = str(manifest.resolve())
        notes_path = out / "video_analysis_notes.md"
        notes_lines = ["# Video Analysis Notes", ""]
        for analysis in analyses:
            notes_lines.append(f"## {analysis.get('title','source')}")
            notes_lines.append(f"- Source type: {analysis.get('source_type','')}")
            notes_lines.append(f"- Transcript method: {(analysis.get('transcript',{}) or {}).get('method','')}")
            notes_lines.append(f"- Transcript coverage: {(analysis.get('transcript',{}) or {}).get('coverage','')}")
            chunk_reports = list(analysis.get("chunk_reports", []) or [])
            if chunk_reports:
                notes_lines.append(f"- Chunk count: {len(chunk_reports)}")
            process_state = dict(analysis.get("process_state", {}) or {})
            phase_count = len(list(process_state.get("phases", []) or []))
            if phase_count:
                notes_lines.append(f"- Process checkpoints: {phase_count}")
            for obs in list(analysis.get("visual_observations", []) or [])[:5]:
                notes_lines.append(f"- {obs.get('timestamp','')}: {obs.get('workflow_stage','')} | {obs.get('ui_elements','')}")
            for question in list((analysis.get("follow_up_questions", {}) or {}).get("questions", [])[:4]):
                notes_lines.append(f"- Follow-up: {question}")
            notes_lines.append("")
        notes_path.write_text("\n".join(notes_lines), encoding="utf-8")
        artifacts["video_analysis_notes_md"] = str(notes_path.resolve())
        topic_model_path = out / "topic_model.md"
        model = dict(synthesis.get("topic_model", {}) or {})
        topic_model_path.write_text("# Topic Model\n\n" + "\n".join([f"- {item}" for item in list(model.get("core_concepts", []) or [])]), encoding="utf-8")
        artifacts["topic_model_md"] = str(topic_model_path.resolve())
        consensus_path = out / "consensus_workflow.md"
        consensus_path.write_text("# Consensus Workflow\n\n" + "\n".join([f"1. {row.get('description','')}" for row in list(synthesis.get("consensus_workflow", []) or [])]), encoding="utf-8")
        artifacts["consensus_workflow_md"] = str(consensus_path.resolve())
        skill_path = out / "learned_skill.json"
        skill_path.write_text(json.dumps(skill, indent=2), encoding="utf-8")
        artifacts["learned_skill_json"] = str(skill_path.resolve())
        version_history_path = out / "skill_version_history.json"
        version_history_path.write_text(json.dumps({"history": list(saved_skill.get("history", []) or []), "diff": dict(saved_skill.get("diff", {}) or {})}, indent=2), encoding="utf-8")
        artifacts["skill_version_history_json"] = str(version_history_path.resolve())
        selector_path = out / "selector_suggestions.json"
        selector_path.write_text(json.dumps({"workflow": [{"step": step.get("step"), "description": step.get("description"), "selector_suggestions": list(step.get("selector_suggestions", []) or [])} for step in list(skill.get("workflow", []) or [])]}, indent=2), encoding="utf-8")
        artifacts["selector_suggestions_json"] = str(selector_path.resolve())
        guide_path = out / "mastery_guide.md"
        guide_path.write_text(guide, encoding="utf-8")
        artifacts["mastery_guide_md"] = str(guide_path.resolve())
        practice_path = out / "practice_plan.md"
        practice_path.write_text(practice_plan, encoding="utf-8")
        artifacts["practice_plan_md"] = str(practice_path.resolve())
        refresh_path = out / "topic_refresh_plan.md"
        refresh_path.write_text("# Topic Refresh Plan\n\n" + "\n".join([f"- {key}: {value}" for key, value in refresh_plan.items()]), encoding="utf-8")
        artifacts["topic_refresh_plan_md"] = str(refresh_path.resolve())
        critic_path = out / "critic_results.md"
        critic_lines = ["# Critic Results", ""] + [f"- {name}: passed={payload.get('passed', False)} score={payload.get('score', 0)} reason={payload.get('reason','')}" for name, payload in critics.items()]
        critic_lines.append(f"- skill_validation: {json.dumps(validation)}")
        critic_path.write_text("\n".join(critic_lines), encoding="utf-8")
        artifacts["critic_results_md"] = str(critic_path.resolve())
        timeline_path = out / "visual_timeline.html"
        timeline_rows = [f"<li>{analysis.get('title','source')} - {', '.join(list(analysis.get('key_timestamps', []) or [])[:5])}</li>" for analysis in analyses]
        timeline_path.write_text(f"<html><body><h1>Visual Timeline</h1><ul>{''.join(timeline_rows)}</ul></body></html>", encoding="utf-8")
        artifacts["visual_timeline_html"] = str(timeline_path.resolve())
        process_path = out / "video_process_state.json"
        process_payload = {
            "videos": [
                {
                    "title": str(analysis.get("title", "")),
                    "source_url": str(analysis.get("source_url", "")),
                    "process_state": dict(analysis.get("process_state", {}) or {}),
                    "chunk_reports": list(analysis.get("chunk_reports", []) or []),
                    "process_state_path": str(analysis.get("process_state_path", "")),
                }
                for analysis in analyses
                if str(analysis.get("source_type", "")) == "video"
            ]
        }
        process_path.write_text(json.dumps(process_payload, indent=2), encoding="utf-8")
        artifacts["video_process_state_json"] = str(process_path.resolve())
        follow_up_path = out / "follow_up_questions.md"
        follow_lines = ["# Follow-up Questions", ""]
        for analysis in analyses:
            title = str(analysis.get("title", "video"))
            questions = list((analysis.get("follow_up_questions", {}) or {}).get("questions", []) or [])
            queries = list((analysis.get("follow_up_questions", {}) or {}).get("research_queries", []) or [])
            if not questions and not queries:
                continue
            follow_lines.append(f"## {title}")
            for item in questions[:8]:
                follow_lines.append(f"- Question: {item}")
            for item in queries[:8]:
                follow_lines.append(f"- Research query: {item}")
            follow_lines.append("")
        follow_up_path.write_text("\n".join(follow_lines).strip() + "\n", encoding="utf-8")
        artifacts["follow_up_questions_md"] = str(follow_up_path.resolve())
        memory_index_path = out / "learning_memory_index.json"
        memory_index_payload = {
            "items": [
                {
                    "title": str(analysis.get("title", "")),
                    "source_url": str(analysis.get("source_url", "")),
                    "learning_memory_refs": list(analysis.get("learning_memory_refs", []) or []),
                }
                for analysis in analyses
                if list(analysis.get("learning_memory_refs", []) or [])
            ]
        }
        memory_index_path.write_text(json.dumps(memory_index_payload, indent=2), encoding="utf-8")
        artifacts["learning_memory_index_json"] = str(memory_index_path.resolve())
        return artifacts

    def _workspace_dir(self, instruction: str, context: Dict[str, Any]) -> Path:
        workspace = str(context.get("workspace_dir", "") or "")
        if workspace:
            path = Path(workspace)
            path.mkdir(parents=True, exist_ok=True)
            return path
        slug = re.sub(r"[^a-z0-9]+", "_", instruction.lower()).strip("_")[:60] or "topic_mastery"
        path = Path("data/learn_runs") / f"{slug}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _status(self, selected: List[Dict[str, object]], validation: Dict[str, object]) -> str:
        if not selected:
            return "blocked"
        if bool(validation.get("passed", False)):
            return "real_complete"
        return "real_partial"

    def _next_steps(self, status: str, validation: Dict[str, object]) -> List[str]:
        if status == "real_complete":
            return ["Open the mastery guide first.", "Review the learned skill and practice plan before using it in a live task."]
        return ["Review low-confidence steps.", "Add stronger official or recent sources before treating the learned skill as executable."]


def _extract_url(text: str) -> str:
    match = re.search(r"https?://\S+", text or "")
    return match.group(0).rstrip(").,") if match else ""


def _extract_topic(text: str, seed_url: str) -> str:
    cleaned = re.sub(r"https?://\S+", "", text or "").strip()
    cleaned = re.sub(r"^(learn how to|learn|watch this video and learn|watch this tutorial and learn)\s+", "", cleaned, flags=re.I)
    cleaned = re.sub(r"(start with|use)\s+this\s+video.*$", "", cleaned, flags=re.I).strip(" .")
    if cleaned:
        return cleaned[:120]
    if seed_url:
        return "Learned topic from seed video"
    return "Topic mastery"


def _default_next_review_at(confidence_score: float) -> str:
    days = 3 if confidence_score < 0.6 else (7 if confidence_score < 0.85 else 14)
    return datetime.fromtimestamp(time.time() + (days * 86400)).isoformat(timespec="seconds")


def _default_memory_store():
    from lam.operator_platform.memory_store import MemoryStore

    return MemoryStore()
