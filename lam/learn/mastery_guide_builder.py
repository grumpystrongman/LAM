from __future__ import annotations

from typing import Dict, List

from .models import LearnedSkill


def build_mastery_guide(topic: str, synthesis: Dict[str, object], skill: LearnedSkill) -> str:
    model = dict(synthesis.get("topic_model", {}) or {})
    consensus = list(synthesis.get("consensus_workflow", []) or [])
    contradictions = list(synthesis.get("contradictions", []) or [])
    practices = list(synthesis.get("best_practices", []) or [])
    source_notes = list(synthesis.get("source_notes", []) or [])
    lines: List[str] = [
        f"# Mastery Guide: {topic}",
        "",
        "## Executive summary",
        f"This guide synthesizes multiple sources to build a reusable playbook for {topic}.",
        "",
        "## What the topic is",
        f"{topic} as a practical workflow area with reusable patterns and validation checkpoints.",
        "",
        "## Why it matters",
        f"A structured approach to {topic} reduces trial-and-error and improves repeatability.",
        "",
        "## Key concepts",
    ]
    lines.extend([f"- {item}" for item in list(model.get("core_concepts", []) or [])] or ["- Concepts were inferred from the selected sources."])
    lines.extend(["", "## Tools required"])
    lines.extend([f"- {item}" for item in list(model.get("required_tools", []) or [])] or ["- Tool requirements vary by source."])
    lines.extend(["", "## Beginner workflow"])
    lines.extend([f"1. {str(step.get('description', ''))}" for step in consensus[:6]] or ["1. Review the selected sources and define the workflow goal."])
    lines.extend(["", "## Advanced workflow"])
    lines.extend([f"1. {str(step.get('description', ''))}" for step in consensus[6:12]] or ["1. Apply variations only after the base workflow is validated."])
    lines.extend(["", "## Best practices"])
    lines.extend([f"- {item}" for item in practices] or ["- Validate output at each stage."])
    lines.extend(["", "## Common mistakes"])
    lines.extend([f"- {item}" for item in skill.common_mistakes] or ["- Skipping validation between steps."])
    lines.extend(["", "## Troubleshooting"])
    lines.extend([f"- {item}" for item in skill.troubleshooting] or ["- Re-check the source version and prerequisites."])
    lines.extend(["", "## Source comparison"])
    for note in source_notes[:8]:
        lines.append(f"- {note.get('title','source')}: {', '.join(list(note.get('highlights', []) or [])[:2])}")
    lines.extend(["", "## Contradictions and how to resolve them"])
    if contradictions:
        for item in contradictions:
            lines.append(f"- {item.get('topic','difference')}: {item.get('resolution','Review official guidance.')}")
    else:
        lines.append("- No major contradictions detected across the selected sources.")
    lines.extend(["", "## Practice exercises", "- Reproduce the workflow in a safe, non-production environment.", "- Validate each major checkpoint before proceeding."])
    lines.extend(["", "## Reusable OpenLAMb skill", f"- Skill name: {skill.skill_name}", f"- Executable status: {skill.executable_status}"])
    lines.extend(["", "## Next learning recommendations", "- Add a fresh official or version-specific source before production use.", "- Practice the workflow and capture user edits into the learned skill."])
    return "\n".join(lines).strip() + "\n"
