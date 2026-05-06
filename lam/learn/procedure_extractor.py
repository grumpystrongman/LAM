from __future__ import annotations

import re
from typing import Dict, List

from .models import ProcedureStep

_VERBS = ["open", "click", "select", "type", "enter", "configure", "build", "create", "filter", "validate", "review", "save", "publish", "run"]


def extract_procedure(source: Dict[str, object], transcript_text: str, observations: List[Dict[str, str]]) -> List[ProcedureStep]:
    sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", transcript_text or "") if s.strip()]
    steps: List[ProcedureStep] = []
    for sentence in sentences:
        low = sentence.lower()
        verb = next((v for v in _VERBS if low.startswith(v) or f" {v} " in low), "")
        if not verb:
            continue
        risk = "medium" if any(t in low for t in ["publish", "deploy", "send", "submit", "install"]) else "low"
        steps.append(
            ProcedureStep(
                description=sentence[:220],
                action_type=verb,
                target=_target(sentence),
                supporting_sources=[str(source.get("source_url", ""))],
                timestamp_refs=[str(observations[min(len(steps), max(0, len(observations)-1))].get("timestamp", ""))] if observations else [],
                confidence=0.84 if risk == "low" else 0.62,
                risk_level=risk,
                approval_required=risk != "low",
            )
        )
        if len(steps) >= 14:
            break
    return steps[:14]


def extract_topic_concepts(topic: str, transcript_text: str, title: str) -> Dict[str, List[str]]:
    low = f"{topic} {title} {transcript_text}".lower()
    tools: List[str] = []
    prerequisites: List[str] = []
    variations: List[str] = []
    concepts: List[str] = []
    for phrase in ["power bi", "dashboard", "kpi", "react", "canvas", "grant proposal", "budget narrative", "azure", "fabric", "documentation", "validation"]:
        if phrase in low:
            concepts.append(phrase.title())
    for phrase in ["power bi", "react", "excel", "github", "browser", "editor"]:
        if phrase in low:
            tools.append(phrase.title())
    if any(t in low for t in ["install", "setup", "workspace"]):
        prerequisites.append("Required tool access and environment setup")
    if any(t in low for t in ["advanced", "optional", "variation", "alternative"]):
        variations.append("Alternative workflow variant described")
    return {
        "concepts": _dedupe(concepts)[:10],
        "tools": _dedupe(tools)[:8],
        "prerequisites": _dedupe(prerequisites)[:6],
        "variations": _dedupe(variations)[:6],
    }


def procedure_steps_to_dict(steps: List[ProcedureStep]) -> List[Dict[str, object]]:
    return [step.to_dict() for step in steps]


def build_highlights(transcript_text: str, title: str) -> List[str]:
    sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", transcript_text or "") if s.strip()]
    highlights = [title]
    highlights.extend(sentences[:5])
    return highlights[:6]


def _target(sentence: str) -> str:
    words = re.findall(r"[A-Za-z][A-Za-z0-9_./-]+", sentence)
    return " ".join(words[:5])


def _dedupe(items: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in items:
        low = item.lower().strip()
        if low and low not in seen:
            seen.add(low)
            out.append(item)
    return out
