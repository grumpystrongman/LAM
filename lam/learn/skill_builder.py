from __future__ import annotations

import re
from typing import Dict, List

from .models import LearnedSkill
from .source_adapters import adapter_summary
from .ui_grounding import build_step_grounding, infer_app_context


def build_skill(topic: str, synthesis: Dict[str, object], selected_sources: List[Dict[str, object]]) -> LearnedSkill:
    model = dict(synthesis.get("topic_model", {}) or {})
    app_context = infer_app_context(topic, list(model.get("required_tools", []) or []), selected_sources)
    workflow = []
    for idx, step in enumerate(list(synthesis.get("consensus_workflow", []) or []), start=1):
        step_payload = dict(step)
        grounding = build_step_grounding(step_payload, app_context, list(step_payload.get("observations", []) or []))
        checkpoint_name = f"{idx}. {str(step_payload.get('action_type', 'workflow')).title()} - {str(step_payload.get('description', ''))[:50]}".strip()
        workflow.append(
            {
                "step": idx,
                "description": str(step_payload.get("description", "")),
                "action_type": str(step_payload.get("action_type", "workflow")),
                "target": str(step_payload.get("target", "")),
                "supporting_sources": list(step_payload.get("supporting_sources", []) or []),
                "timestamp_refs": list(step_payload.get("timestamp_refs", []) or []),
                "confidence": float(step_payload.get("confidence", 0.0) or 0.0),
                "risk_level": str(step_payload.get("risk_level", "low") or "low"),
                "approval_required": bool(step_payload.get("approval_required", False)),
                "ui_grounding": grounding,
                "selector_suggestions": list(grounding.get("control_hints", []) or []),
                "checkpoint_id": re.sub(r"[^a-z0-9]+", "_", checkpoint_name.lower()).strip("_")[:60] or f"step_{idx}",
                "checkpoint_name": checkpoint_name,
                "practice_mode": "safe_simulation" if str(step_payload.get("risk_level", "low")) != "low" else "guided_step",
            }
        )
    confidence = float(synthesis.get("confidence", 0.0) or 0.0)
    skill_id = re.sub(r"[^a-z0-9]+", "_", topic.lower()).strip("_")[:60] or "learned_skill"
    checkpoints = [
        {
            "checkpoint_id": str(step.get("checkpoint_id", "")),
            "checkpoint_name": str(step.get("checkpoint_name", "")),
            "step_index": int(step.get("step", 1) or 1) - 1,
            "approval_required": bool(step.get("approval_required", False)),
            "risk_level": str(step.get("risk_level", "low")),
            "step_payload": dict(step),
        }
        for step in workflow
    ]
    return LearnedSkill(
        skill_id=f"skill_{skill_id}",
        skill_name=f"Topic Mastery - {topic}",
        topic=topic,
        purpose=f"Reusable playbook for {topic}",
        domain="topic_learning",
        source_urls=[str(item.get("source_url", "")) for item in selected_sources if str(item.get("source_url", ""))],
        source_summary=[f"{item.get('title','')} ({item.get('source_type','')})" for item in selected_sources[:8]],
        prerequisites=list(model.get("prerequisites", []) or []),
        required_tools=list(model.get("required_tools", []) or []),
        workflow=workflow,
        decision_points=["Pause and verify expected output before moving to the next stage.", "Escalate to human review if the workflow diverges from the supported sources."],
        safety_gates=["Do not run destructive or publishing actions without approval.", "Treat low-confidence steps as guided only."],
        validation_checks=["Confirm prerequisites are met.", "Validate the intermediate output after each major stage.", "Cross-check against at least one supporting source when confidence is low."],
        troubleshooting=["Re-check version differences and official docs.", "Return to the last validated checkpoint if the workflow diverges."],
        common_mistakes=list(synthesis.get("best_practices", []) or [])[:5],
        variations=list(model.get("variations", []) or []),
        confidence_score=round(confidence, 3),
        limitations=["Built from summaries, transcript coverage, and sampled observations rather than full copyrighted content.", "Executable status depends on source quality and safety review."],
        executable_status="guided_only",
        app_context=app_context,
        checkpoints=checkpoints,
        source_adapter_summary=adapter_summary(selected_sources),
        refresh_policy={
            "version_sensitive": any(bool(item.get("version_sensitive", False)) for item in selected_sources),
            "preferred_refresh_window_days": min([int(item.get("freshness_window_days", 60) or 60) for item in selected_sources] or [60]),
            "refresh_when": "new official docs, major product changes, or repeated practice failures",
        },
        practice_policy={
            "mode": "checkpoint_guided",
            "safe_only": True,
            "enforce_checkpoint_validation": True,
            "allow_autorun_only_for_low_risk": True,
        },
    )
