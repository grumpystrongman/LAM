from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple


RUBRIC_CATEGORIES: List[str] = [
    "environment_awareness",
    "state_reuse",
    "tool_selection",
    "target_specificity",
    "planning_quality",
    "anti_loop_behavior",
    "recovery_behavior",
    "multi_tool_orchestration",
    "evidence_and_verification",
    "truthfulness",
    "completion_quality",
    "safety_and_escalation",
]

DEFAULT_WEIGHTS: Dict[str, int] = {
    "environment_awareness": 10,
    "state_reuse": 10,
    "tool_selection": 10,
    "target_specificity": 10,
    "planning_quality": 8,
    "anti_loop_behavior": 10,
    "recovery_behavior": 10,
    "multi_tool_orchestration": 8,
    "evidence_and_verification": 8,
    "truthfulness": 8,
    "completion_quality": 6,
    "safety_and_escalation": 2,
}

SCORE_BANDS: List[Tuple[int, int, str]] = [
    (0, 15, "brittle_script"),
    (16, 25, "weak_agent"),
    (26, 35, "usable_narrow_lanes"),
    (36, 42, "promising_operator"),
    (43, 48, "strong_operator"),
    (49, 10_000, "human_like"),
]


@dataclass(slots=True)
class ScenarioScore:
    scenario_id: str
    scenario_name: str
    scores: Dict[str, int]
    total: int
    weighted_total: int
    weighted_max: int
    weighted_pct: float
    verdict: str
    notes: List[str]


def verdict_for_total(total: int) -> str:
    for low, high, label in SCORE_BANDS:
        if low <= total <= high:
            return label
    return "unknown"


def normalize_scores(scores: Dict[str, Any]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for category in RUBRIC_CATEGORIES:
        value = int(scores.get(category, 0))
        out[category] = max(0, min(4, value))
    return out


def weighted_summary(scores: Dict[str, int], weights: Dict[str, int] | None = None) -> Dict[str, Any]:
    use_weights = dict(DEFAULT_WEIGHTS)
    if weights:
        for key, val in weights.items():
            if key in RUBRIC_CATEGORIES:
                use_weights[key] = int(max(1, val))
    weighted_total = 0
    weighted_max = 0
    for category in RUBRIC_CATEGORIES:
        weight = int(use_weights.get(category, 1))
        weighted_total += int(scores.get(category, 0)) * weight
        weighted_max += 4 * weight
    pct = (100.0 * weighted_total / weighted_max) if weighted_max > 0 else 0.0
    return {
        "weighted_total": weighted_total,
        "weighted_max": weighted_max,
        "weighted_pct": round(pct, 2),
    }


def score_scenario(
    *,
    scenario_id: str,
    scenario_name: str,
    scores: Dict[str, Any],
    notes: List[str] | None = None,
    weights: Dict[str, int] | None = None,
) -> ScenarioScore:
    normalized = normalize_scores(scores)
    total = sum(int(normalized[c]) for c in RUBRIC_CATEGORIES)
    summary = weighted_summary(normalized, weights=weights)
    return ScenarioScore(
        scenario_id=scenario_id,
        scenario_name=scenario_name,
        scores=normalized,
        total=total,
        weighted_total=int(summary["weighted_total"]),
        weighted_max=int(summary["weighted_max"]),
        weighted_pct=float(summary["weighted_pct"]),
        verdict=verdict_for_total(total),
        notes=list(notes or []),
    )


def load_scenarios(path: str | Path) -> List[Dict[str, Any]]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    scenarios = raw.get("scenarios", [])
    if not isinstance(scenarios, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in scenarios:
        if not isinstance(item, dict):
            continue
        scenario_id = str(item.get("scenario_id", "")).strip()
        name = str(item.get("scenario_name", "")).strip()
        if not scenario_id or not name:
            continue
        out.append(item)
    return out


def evaluate_run_result(result: Dict[str, Any]) -> Dict[str, int]:
    """Heuristic rubric scoring from one run payload."""
    scores = {k: 0 for k in RUBRIC_CATEGORIES}
    trace = result.get("trace", []) or []
    artifacts = result.get("artifacts", {}) or {}
    verification = result.get("verification_report", {}) or {}
    anti_drift = result.get("anti_drift", {}) or {}

    if trace:
        scores["environment_awareness"] = 2
        scores["target_specificity"] = 2
    if result.get("source_status"):
        scores["state_reuse"] = 2
    if result.get("plan_contract", {}).get("validation_status") == "valid":
        scores["planning_quality"] = 3
        scores["tool_selection"] = 3
    if anti_drift.get("has_failures") is False:
        scores["anti_loop_behavior"] = 3
    if result.get("decision_log"):
        scores["recovery_behavior"] = 2
    if result.get("mode") in {"autonomous_plan_execute", "desktop_sequence"}:
        scores["multi_tool_orchestration"] = 2
    if verification:
        evidence_checks = verification.get("verification_checks", [])
        scores["evidence_and_verification"] = 2 if evidence_checks else 1
        scores["truthfulness"] = 3 if verification.get("final_verification") in {"passed", "failed"} else 2
    if artifacts:
        scores["completion_quality"] = 3 if result.get("ok") else 2
    else:
        scores["completion_quality"] = 0 if not result.get("ok") else 1
    if result.get("requires_confirmation") or result.get("final_report", {}).get("status") == "awaiting_confirmation":
        scores["safety_and_escalation"] = 4
    else:
        scores["safety_and_escalation"] = 2

    return normalize_scores(scores)


def benchmark_from_last_run(
    *,
    result: Dict[str, Any],
    scenario_id: str = "live_last_run",
    scenario_name: str = "Live Last Run Benchmark",
) -> Dict[str, Any]:
    scenario = score_scenario(
        scenario_id=scenario_id,
        scenario_name=scenario_name,
        scores=evaluate_run_result(result),
        notes=["Auto-scored from latest run payload."],
    )
    return {
        "ok": True,
        "mode": "human_operator_benchmark",
        "generated_at": time.time(),
        "scenario": {
            "scenario_id": scenario.scenario_id,
            "scenario_name": scenario.scenario_name,
            "scores": scenario.scores,
            "total": scenario.total,
            "weighted_total": scenario.weighted_total,
            "weighted_max": scenario.weighted_max,
            "weighted_pct": scenario.weighted_pct,
            "verdict": scenario.verdict,
            "notes": scenario.notes,
        },
        "weights": dict(DEFAULT_WEIGHTS),
        "bands": [{"low": x[0], "high": x[1], "label": x[2]} for x in SCORE_BANDS],
    }

