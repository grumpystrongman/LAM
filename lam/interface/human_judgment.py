from __future__ import annotations

import json
import re
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


LOW_VALUE_HOSTS = {
    "google.com",
    "www.google.com",
    "bing.com",
    "www.bing.com",
}


def _memory_path() -> Path:
    path = Path("data/interface/negative_memory.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


@dataclass(slots=True)
class ResultQuality:
    score: float
    level: str
    reasons: List[str]


@dataclass(slots=True)
class CriticDecision:
    allow: bool
    score: float
    reasons: List[str]
    elegance_cost: int


@dataclass(slots=True)
class EleganceBudget:
    total: int = 100
    consumed: int = 0
    events: List[Dict[str, Any]] | None = None

    def __post_init__(self) -> None:
        if self.events is None:
            self.events = []

    @property
    def remaining(self) -> int:
        return max(0, int(self.total) - int(self.consumed))

    def consume(self, points: int, reason: str) -> None:
        p = max(0, int(points))
        self.consumed += p
        assert self.events is not None
        self.events.append({"cost": p, "reason": str(reason)[:120]})

    def snapshot(self) -> Dict[str, Any]:
        return {
            "total": int(self.total),
            "consumed": int(self.consumed),
            "remaining": int(self.remaining),
            "events": list(self.events or [])[-20:],
        }


class ActionCritic:
    def __init__(
        self,
        *,
        weights: Optional[Dict[str, float]] = None,
        base_score: float = 100.0,
    ) -> None:
        self.weights = weights or {
            "missing_action": 100.0,
            "missing_target": 100.0,
            "redundant_open": 25.0,
            "shortest_path_reuse_existing_state": 35.0,
            "loop_detected_repeated_action_target": 45.0,
        }
        self.base_score = float(base_score)

    def evaluate(
        self,
        *,
        next_action: str,
        target: str,
        already_open: bool = False,
        context: Optional[Dict[str, Any]] = None,
    ) -> CriticDecision:
        action = str(next_action or "").strip().lower()
        tgt = str(target or "").strip().lower()
        ctx = context or {}
        reasons: List[str] = []
        score = self.base_score
        elegance_cost = 0
        if not action:
            reasons.append("missing_action")
        if not tgt:
            reasons.append("missing_target")
        if already_open and action in {"open_tab", "open_app", "launch"}:
            reasons.append("redundant_open")
        reusable_target = str(ctx.get("reusable_target", "")).strip().lower()
        if reusable_target and action in {"open_tab", "open_app", "launch"}:
            if reusable_target == tgt or reusable_target in tgt or tgt in reusable_target:
                reasons.append("shortest_path_reuse_existing_state")
        recent_actions = ctx.get("recent_actions", [])
        if isinstance(recent_actions, list) and len(recent_actions) >= 2:
            tail = [str(x).strip().lower() for x in recent_actions[-2:]]
            probe = f"{action}:{tgt}"
            if all(x == probe for x in tail):
                reasons.append("loop_detected_repeated_action_target")
        for r in reasons:
            score -= float(self.weights.get(r, 10.0))
            if r in {"redundant_open", "shortest_path_reuse_existing_state", "loop_detected_repeated_action_target"}:
                elegance_cost += 8 if r == "redundant_open" else 12
        allow = len(reasons) == 0
        return CriticDecision(allow=allow, score=max(0.0, score), reasons=reasons or ["ok"], elegance_cost=elegance_cost)


class QualityCritic:
    def __init__(
        self,
        *,
        weights: Optional[Dict[str, float]] = None,
    ) -> None:
        self.weights = weights or {
            "generic_search_host": -3.0,
            "homepage_only": -2.0,
            "search_or_category_page": -2.0,
            "specific_path": 1.2,
            "concrete_item_page": 2.2,
            "low_query_overlap": -1.5,
            "query_overlap": 0.8,
            "locality_match": 2.0,
            "locality_miss": -1.0,
            "evidence_count": 0.5,
        }

    def evaluate(
        self,
        *,
        title: str,
        url: str,
        snippet: str,
        query: str,
        locality_terms: Iterable[str],
        evidence_count: int = 0,
    ) -> ResultQuality:
        low_title = str(title or "").lower()
        low_snippet = str(snippet or "").lower()
        low_url = str(url or "").lower()
        parsed = urllib.parse.urlparse(low_url)
        host = parsed.netloc.lower()
        path = parsed.path.lower()
        reasons: List[str] = []
        score = 0.0
        if host in LOW_VALUE_HOSTS:
            score += self.weights["generic_search_host"]
            reasons.append("generic_search_host")
        if path in {"", "/"}:
            score += self.weights["homepage_only"]
            reasons.append("homepage_only")
        if "/search" in path or "q=" in parsed.query or "query=" in parsed.query:
            score += self.weights["search_or_category_page"]
            reasons.append("search_or_category_page")
        if len(path.strip("/").split("/")) >= 2:
            score += self.weights["specific_path"]
            reasons.append("specific_path")
        if any(x in path for x in ["/dp/", "/product", "/item", "/listing", "/jobs/", "/job/"]):
            score += self.weights["concrete_item_page"]
            reasons.append("concrete_item_page")
        qtokens = [t for t in re.findall(r"[a-z0-9]+", query.lower()) if len(t) >= 3][:8]
        hay = f"{low_title} {low_snippet} {low_url}"
        overlap = sum(1 for t in qtokens if t in hay)
        score += min(3.0, overlap * self.weights["query_overlap"])
        if overlap == 0:
            score += self.weights["low_query_overlap"]
            reasons.append("low_query_overlap")
        loc_terms = [str(x).strip().lower() for x in locality_terms if str(x).strip()]
        if loc_terms:
            if any(loc in hay for loc in loc_terms):
                score += self.weights["locality_match"]
                reasons.append("locality_match")
            else:
                score += self.weights["locality_miss"]
                reasons.append("locality_miss")
        if evidence_count > 0:
            score += min(2.0, float(evidence_count) * self.weights["evidence_count"])
            reasons.append("evidence_count")
        level = "high" if score >= 3.0 else ("medium" if score >= 1.0 else "low")
        return ResultQuality(score=float(score), level=level, reasons=reasons)


class NegativeMemory:
    def __init__(self, path: Optional[Path] = None) -> None:
        self.path = path or _memory_path()
        self._data = self._load()

    def _load(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {"bad_urls": {}, "bad_hosts": {}}
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                raw.setdefault("bad_urls", {})
                raw.setdefault("bad_hosts", {})
                return raw
        except Exception:
            pass
        return {"bad_urls": {}, "bad_hosts": {}}

    def _save(self) -> None:
        self.path.write_text(json.dumps(self._data, indent=2), encoding="utf-8")

    def is_bad_url(self, url: str) -> bool:
        key = str(url or "").strip().lower()
        if not key:
            return False
        if key in self._data.get("bad_urls", {}):
            return True
        host = urllib.parse.urlparse(key).netloc.lower()
        return host in self._data.get("bad_hosts", {})

    def mark_bad_url(self, url: str, reason: str) -> None:
        key = str(url or "").strip().lower()
        if not key:
            return
        host = urllib.parse.urlparse(key).netloc.lower()
        self._data.setdefault("bad_urls", {})[key] = reason[:200]
        if host:
            self._data.setdefault("bad_hosts", {}).setdefault(host, reason[:200])
        self._save()


def assess_result_quality(
    *,
    title: str,
    url: str,
    snippet: str,
    query: str,
    locality_terms: Iterable[str],
) -> ResultQuality:
    critic = QualityCritic()
    return critic.evaluate(
        title=title,
        url=url,
        snippet=snippet,
        query=query,
        locality_terms=locality_terms,
        evidence_count=0,
    )


def action_critic(
    next_action: str,
    target: str,
    already_open: bool = False,
    context: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, str]:
    decision = ActionCritic().evaluate(
        next_action=next_action,
        target=target,
        already_open=already_open,
        context=context,
    )
    if decision.allow:
        return True, "ok"
    primary = next((x for x in decision.reasons if x != "ok"), "blocked")
    return False, primary
