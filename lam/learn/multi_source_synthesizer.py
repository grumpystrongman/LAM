from __future__ import annotations

from typing import Dict, List

from .best_practice_extractor import extract_best_practices
from .consensus_builder import build_consensus_workflow
from .contradiction_detector import ContradictionDetector
from .models import TopicModel


class MultiSourceSynthesizer:
    def synthesize(self, topic: str, analyses: List[Dict[str, object]]) -> Dict[str, object]:
        source_notes: List[Dict[str, object]] = []
        all_steps: List[Dict[str, object]] = []
        concepts: List[str] = []
        tools: List[str] = []
        prerequisites: List[str] = []
        variations: List[str] = []
        for analysis in analyses:
            source_notes.append({
                "source_url": str(analysis.get("source_url", "")),
                "title": str(analysis.get("title", "")),
                "highlights": list(analysis.get("highlights", []) or []),
            })
            all_steps.extend(list(analysis.get("procedure_steps", []) or []))
            concepts.extend([str(x) for x in list(analysis.get("concepts", []) or [])])
            tools.extend([str(x) for x in list(analysis.get("tools", []) or [])])
            prerequisites.extend([str(x) for x in list(analysis.get("prerequisites", []) or [])])
            variations.extend([str(x) for x in list(analysis.get("variations", []) or [])])
        topic_model = TopicModel(
            topic=topic,
            core_concepts=_dedupe(concepts)[:10],
            terminology=_dedupe(concepts)[:12],
            required_tools=_dedupe(tools)[:8],
            prerequisites=_dedupe(prerequisites)[:8],
            mental_model=[f"Use a repeatable workflow to complete {topic} with validation at each stage."],
            common_use_cases=[f"Create a reliable output for {topic}.", f"Teach a reusable process for {topic}."],
            variations=_dedupe(variations)[:8],
        )
        consensus = build_consensus_workflow(all_steps)
        contradictions = ContradictionDetector().detect(source_notes)
        best_practices = extract_best_practices(source_notes)
        support_counts = [int(item.get("support_count", 0) or 0) for item in consensus]
        confidence = round(min(0.97, 0.45 + (0.04 * len(analyses)) + (0.03 * len(best_practices)) + (0.02 * sum(1 for x in support_counts if x >= 2))), 3)
        return {
            "topic_model": topic_model.to_dict(),
            "consensus_workflow": consensus,
            "contradictions": contradictions,
            "best_practices": best_practices,
            "confidence": confidence,
            "source_notes": source_notes,
        }


def _dedupe(items: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in items:
        text = str(item or "").strip()
        if not text:
            continue
        low = text.lower()
        if low in seen:
            continue
        seen.add(low)
        out.append(text)
    return out
