from __future__ import annotations

from datetime import datetime
from typing import Dict, List


class SourceRanker:
    def rank(self, topic: str, sources: List[Dict[str, object]]) -> List[Dict[str, object]]:
        ranked: List[Dict[str, object]] = []
        for row in sources:
            item = dict(row)
            score = self._score(topic, item)
            item["score"] = round(score, 3)
            item["reason_selected"] = self._reason(item)
            item["expected_use"] = self._expected_use(item)
            ranked.append(item)
        ranked.sort(key=lambda item: float(item.get("score", 0.0)), reverse=True)
        for idx, row in enumerate(ranked, start=1):
            row["rank"] = idx
        return ranked

    def _score(self, topic: str, source: Dict[str, object]) -> float:
        title = str(source.get("title", "") or "").lower()
        snippet = str(source.get("snippet", "") or "").lower()
        stype = str(source.get("source_type", "other") or "other").lower()
        adapter = dict(source.get("adapter", {}) or {})
        capabilities = dict(source.get("adapter_capabilities", {}) or {})
        topic_terms = [t for t in topic.lower().split() if len(t) > 2]
        overlap = sum(1 for term in topic_terms if term in title or term in snippet)
        base = min(0.32, overlap * 0.055)
        type_bonus = {"video": 0.21, "docs": 0.24, "blog": 0.14, "github": 0.16, "forum": 0.08}.get(stype, 0.07)
        recency_bonus = _recency_bonus(str(source.get("upload_date", "") or ""), bool(source.get("version_sensitive", False)))
        transcript_bonus = 0.11 if (source.get("captions") or source.get("transcript") or capabilities.get("transcript")) else 0.0
        tutorial_bonus = 0.12 if any(t in title for t in ["tutorial", "walkthrough", "guide", "how to"]) else 0.05
        trust_bonus = float(adapter.get("trust_tier", 0.5) or 0.5) * 0.14
        official_bonus = 0.08 if str(source.get("authority_level", "")).startswith("official") else 0.0
        live_bonus = 0.06 if str(source.get("discovery_mode", "") or "") == "live" else 0.0
        procedural_bonus = 0.05 if any(t in snippet for t in ["step", "click", "open", "build", "configure", "validate"]) else 0.0
        visual_bonus = 0.05 if bool(capabilities.get("visual_sampling", False)) else 0.0
        return min(0.99, 0.12 + base + type_bonus + recency_bonus + transcript_bonus + tutorial_bonus + trust_bonus + official_bonus + live_bonus + procedural_bonus + visual_bonus)

    def _reason(self, source: Dict[str, object]) -> str:
        stype = str(source.get("source_type", "other") or "other")
        live = str(source.get("discovery_mode", "") or "") == "live"
        authority = str(source.get("authority_level", "") or "")
        if stype == "video":
            return "Selected as a procedural tutorial source with demonstration value and transcript/visual coverage." if live else "Selected as a procedural tutorial source with demonstration value."
        if stype == "docs":
            return "Selected as an authoritative source for official guidance and version-sensitive details."
        if stype == "github":
            return "Selected as an implementation reference and example source."
        if authority.startswith("official"):
            return "Selected as an official source for authoritative workflow and policy details."
        return "Selected as supporting context and practitioner guidance."

    def _expected_use(self, source: Dict[str, object]) -> str:
        stype = str(source.get("source_type", "other") or "other")
        return {
            "video": "primary procedural source",
            "docs": "authoritative reference",
            "blog": "supporting best-practice source",
            "github": "reference example",
            "forum": "troubleshooting source",
        }.get(stype, "supporting source")


def _recency_bonus(upload_date: str, version_sensitive: bool) -> float:
    if not upload_date:
        return 0.02 if version_sensitive else 0.04
    try:
        parsed = datetime.fromisoformat(upload_date.replace("Z", "+00:00"))
        age_days = max(0, (datetime.now(parsed.tzinfo) - parsed).days)
    except Exception:
        return 0.05
    if age_days <= 30:
        return 0.1 if version_sensitive else 0.08
    if age_days <= 120:
        return 0.07 if version_sensitive else 0.06
    if age_days <= 365:
        return 0.04
    return 0.01 if version_sensitive else 0.03
