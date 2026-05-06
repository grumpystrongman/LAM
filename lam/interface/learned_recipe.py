from __future__ import annotations

import json
import re
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List


@dataclass(slots=True)
class RecipeStep:
    action: str
    purpose: str
    source_index: int = -1
    target: str = ""
    selector: Dict[str, Any] = field(default_factory=dict)
    fallback_selectors: List[Dict[str, Any]] = field(default_factory=list)
    variable_name: str = ""
    default_value: str = ""
    expected_state: str = ""
    recovery_hint: str = ""
    confidence: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class LearnedMissionRecipe:
    recipe_id: str
    app_name: str
    learned_goal: str
    family_id: str = ""
    variant_label: str = ""
    source: str = "teach_mode"
    created_at: float = field(default_factory=time.time)
    steps: List[RecipeStep] = field(default_factory=list)
    required_inputs: List[Dict[str, Any]] = field(default_factory=list)
    decision_points: List[str] = field(default_factory=list)
    preconditions: List[str] = field(default_factory=list)
    success_signals: List[str] = field(default_factory=list)
    state_snapshots: List[Dict[str, Any]] = field(default_factory=list)
    robustness_notes: List[str] = field(default_factory=list)
    confidence: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["steps"] = [step.to_dict() for step in self.steps]
        return payload


@dataclass(slots=True)
class RecipeCriticResult:
    passed: bool
    score: float
    issues: List[str] = field(default_factory=list)
    suggested_repairs: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class RecipeCritic:
    def evaluate(self, recipe: LearnedMissionRecipe) -> RecipeCriticResult:
        issues: List[str] = []
        if not recipe.steps:
            issues.append("no_steps")
        click_steps = [step for step in recipe.steps if step.action == "click"]
        low_conf_clicks = [step for step in click_steps if step.confidence < 0.55]
        if low_conf_clicks:
            issues.append("low_confidence_selectors")
        if recipe.required_inputs and not any(step.variable_name for step in recipe.steps if step.action == "type_text"):
            issues.append("missing_variable_binding")
        if not recipe.success_signals:
            issues.append("missing_success_signal")
        passed = not issues
        score = 0.92 if passed else max(0.35, 0.92 - (0.18 * len(issues)))
        repairs: List[str] = []
        if "low_confidence_selectors" in issues:
            repairs.append("Capture named controls instead of point-only clicks where possible.")
        if "missing_success_signal" in issues:
            repairs.append("Demonstrate the completion state so replay can verify success.")
        if "missing_variable_binding" in issues:
            repairs.append("Enter representative text so the recorder can identify reusable text fields.")
        if "no_steps" in issues:
            repairs.append("Record at least one concrete action.")
        return RecipeCriticResult(passed=passed, score=round(score, 2), issues=issues, suggested_repairs=repairs)


def build_learned_recipe(
    app_name: str,
    compressed_events: List[Dict[str, Any]],
    observation_frames: List[Dict[str, Any]] | None = None,
    observation_segments: List[Dict[str, Any]] | None = None,
) -> LearnedMissionRecipe:
    app = (app_name or "").strip().lower()
    timestamp_ms = int(time.time() * 1000)
    recipe_id = _slug(f"{app}_{timestamp_ms}_{uuid.uuid4().hex[:8]}")
    required_inputs: List[Dict[str, Any]] = []
    decision_points: List[str] = []
    success_signals: List[str] = []
    robustness_notes: List[str] = []
    steps: List[RecipeStep] = []
    typed_index = 0
    click_labels: List[str] = []

    if app:
        steps.append(
            RecipeStep(
                action="open_app",
                purpose=f"Open {app}",
                source_index=-1,
                target=app,
                confidence=0.95,
            )
        )

    for source_index, event in enumerate(compressed_events):
        action = str(event.get("action", "") or "")
        payload = dict(event.get("payload", {}) or {})
        if action == "click":
            selector = dict(payload.get("selector", {}) or {})
            label = _selector_label(selector)
            click_labels.append(label)
            confidence = _selector_confidence(selector)
            steps.append(
                RecipeStep(
                    action="click",
                    purpose=_click_purpose(label),
                    source_index=source_index,
                    target=label,
                    selector=selector,
                    fallback_selectors=_fallback_selectors(selector, label),
                    expected_state=_expected_state_for_click(label),
                    recovery_hint=_recovery_hint_for_click(label),
                    confidence=confidence,
                )
            )
        elif action == "type_text":
            text = str(payload.get("text", "") or "")
            typed_index += 1
            var_name = _variable_name(text, typed_index)
            required_inputs.append(
                {
                    "name": var_name,
                    "kind": "text",
                    "example": text[:120],
                }
            )
            steps.append(
                RecipeStep(
                    action="type_text",
                    purpose="Provide task-specific text input",
                    source_index=source_index,
                    variable_name=var_name,
                    default_value=text,
                    expected_state="Target input accepts the provided text.",
                    recovery_hint="Refocus the intended input field before retrying text entry.",
                    confidence=0.9,
                )
            )
        elif action == "hotkey":
            keys = str(payload.get("keys", "") or "")
            if keys.lower() in {"enter", "return"}:
                success_signals.append("submission_or_next_step_triggered")
            steps.append(
                RecipeStep(
                    action="hotkey",
                    purpose=f"Press {keys}",
                    source_index=source_index,
                    target=keys,
                    expected_state="The workflow advances or commits the current form state.",
                    recovery_hint="Confirm the focused control is correct before retrying the hotkey.",
                    confidence=0.88,
                )
            )
        elif action == "wait":
            seconds = int(payload.get("seconds", 1) or 1)
            decision_points.append(f"Wait for UI update or async work for about {seconds} second(s).")
            steps.append(
                RecipeStep(
                    action="wait",
                    purpose="Allow the interface to update before the next decision",
                    source_index=source_index,
                    target=str(seconds),
                    expected_state="The next UI state becomes available after the wait.",
                    recovery_hint="Increase wait time or assert the next control is visible before proceeding.",
                    confidence=0.7,
                )
            )

    if not success_signals and click_labels:
        success_signals.append(f"Target screen element reached after '{click_labels[-1]}'")
    if any("password" in label.lower() for label in click_labels):
        robustness_notes.append("Credential handling was suppressed; replay must rely on stored credentials or user approval.")
    if any(step.confidence < 0.55 for step in steps if step.action == "click"):
        robustness_notes.append("Some clicks were captured with weak selectors and may require re-teaching for stronger adaptation.")

    goal = infer_goal(app, steps)
    recipe = LearnedMissionRecipe(
        recipe_id=recipe_id,
        app_name=app,
        learned_goal=goal,
        family_id=_family_id(app, goal),
        variant_label=f"variant_{timestamp_ms}",
        steps=steps,
        required_inputs=required_inputs,
        decision_points=list(dict.fromkeys(decision_points)),
        preconditions=_preconditions(app, steps),
        success_signals=list(dict.fromkeys(success_signals)),
        state_snapshots=_build_state_snapshots(observation_frames or [], observation_segments or []),
        robustness_notes=robustness_notes,
    )
    recipe.confidence = round(_recipe_confidence(recipe), 2)
    return recipe


def recipe_to_instruction(recipe: LearnedMissionRecipe) -> str:
    parts: List[str] = []
    for step in recipe.steps:
        if step.action == "open_app":
            parts.append(f"open {step.target} app")
        elif step.action == "click":
            parts.append(f"click {step.target}")
        elif step.action == "type_text":
            var = step.variable_name or "input_text"
            parts.append(f'type <{var}>')
        elif step.action == "hotkey":
            parts.append(f"press {step.target}")
        elif step.action == "wait":
            parts.append(f"wait {step.target} seconds")
    return " then ".join(parts)


class RecipeMemory:
    def __init__(self, root: str | Path | None = None) -> None:
        self.root = Path(root or Path("data") / "teach_recipes")
        self.root.mkdir(parents=True, exist_ok=True)
        self.family_root = self.root / "families"
        self.family_root.mkdir(parents=True, exist_ok=True)

    def save(self, recipe: LearnedMissionRecipe) -> str:
        path = self.root / f"{recipe.recipe_id}.json"
        path.write_text(json.dumps(recipe.to_dict(), indent=2), encoding="utf-8")
        self._save_family_variant(recipe, path)
        return str(path.resolve())

    def list_for_app(self, app_name: str) -> List[Dict[str, Any]]:
        target = (app_name or "").strip().lower()
        items: List[Dict[str, Any]] = []
        for path in sorted(self.root.glob("*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if str(payload.get("app_name", "")).strip().lower() == target:
                payload["path"] = str(path.resolve())
                items.append(payload)
        return items

    def list_families_for_app(self, app_name: str) -> List[Dict[str, Any]]:
        target = (app_name or "").strip().lower()
        items: List[Dict[str, Any]] = []
        for path in sorted(self.family_root.glob("*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if str(payload.get("app_name", "")).strip().lower() == target:
                payload = self._refresh_family_health(payload)
                payload["path"] = str(path.resolve())
                items.append(payload)
        return items

    def load_family(self, family_id: str) -> Dict[str, Any]:
        path = self.family_root / f"{family_id}.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        refreshed = self._refresh_family_health(payload)
        path.write_text(json.dumps(refreshed, indent=2), encoding="utf-8")
        return refreshed

    def record_variant_outcome(
        self,
        *,
        family_id: str,
        recipe_id: str,
        ok: bool,
        reason: str = "",
        current_state: Dict[str, Any] | None = None,
        checkpoint_id: str = "",
        checkpoint_name: str = "",
    ) -> Dict[str, Any]:
        family = self.load_family(family_id)
        variants = list(family.get("variants", []) or [])
        updated = False
        for variant in variants:
            if str(variant.get("recipe_id", "")) != str(recipe_id):
                continue
            history = dict(variant.get("replay_history", {}) or {})
            history["success_count"] = int(history.get("success_count", 0) or 0) + (1 if ok else 0)
            history["failure_count"] = int(history.get("failure_count", 0) or 0) + (0 if ok else 1)
            history["last_outcome"] = "success" if ok else "failure"
            history["last_reason"] = str(reason or "")
            history["last_used_at"] = time.time()
            if current_state:
                history["last_state"] = dict(current_state)
            recent_runs = list(history.get("recent_runs", []) or [])
            recent_runs.append(
                {
                    "timestamp": history["last_used_at"],
                    "ok": bool(ok),
                    "reason": str(reason or ""),
                    "checkpoint_id": str(checkpoint_id or ""),
                    "checkpoint_name": str(checkpoint_name or ""),
                }
            )
            history["recent_runs"] = recent_runs[-12:]
            variant["replay_history"] = history
            updated = True
            break
        if not updated:
            return {"ok": False, "error": "variant_not_found"}
        family["variants"] = variants
        family = self._refresh_family_health(family)
        family["updated_at"] = time.time()
        family_path = self.family_root / f"{family_id}.json"
        family_path.write_text(json.dumps(family, indent=2), encoding="utf-8")
        return {"ok": True, "family_id": family_id, "recipe_id": recipe_id}

    def _save_family_variant(self, recipe: LearnedMissionRecipe, recipe_path: Path) -> None:
        family_id = recipe.family_id or _family_id(recipe.app_name, recipe.learned_goal)
        family_path = self.family_root / f"{family_id}.json"
        family_payload: Dict[str, Any]
        if family_path.exists():
            try:
                family_payload = json.loads(family_path.read_text(encoding="utf-8"))
            except Exception:
                family_payload = {}
        else:
            family_payload = {}
        variants = list(family_payload.get("variants", []) or [])
        recipe_payload = recipe.to_dict()
        recipe_payload["path"] = str(recipe_path.resolve())
        variants = [item for item in variants if str(item.get("recipe_id", "")) != recipe.recipe_id]
        variants.append(recipe_payload)
        variants.sort(key=lambda item: float(item.get("confidence", 0.0) or 0.0), reverse=True)
        family_payload = {
            "family_id": family_id,
            "app_name": recipe.app_name,
            "learned_goal": recipe.learned_goal,
            "variant_count": len(variants),
            "variants": variants,
            "updated_at": time.time(),
        }
        family_payload = self._refresh_family_health(family_payload)
        family_path.write_text(json.dumps(family_payload, indent=2), encoding="utf-8")

    @staticmethod
    def _refresh_family_health(family_payload: Dict[str, Any]) -> Dict[str, Any]:
        family = dict(family_payload or {})
        variants = list(family.get("variants", []) or [])
        now = time.time()
        active_variants = 0
        for variant in variants:
            history = dict(variant.get("replay_history", {}) or {})
            success_count = int(history.get("success_count", 0) or 0)
            failure_count = int(history.get("failure_count", 0) or 0)
            total = success_count + failure_count
            success_rate = float(success_count / total) if total else 0.0
            last_used_at = float(history.get("last_used_at", 0.0) or 0.0)
            stale_age_hours = float((now - last_used_at) / 3600.0) if last_used_at > 0 else 999999.0
            decay_penalty = min(0.35, stale_age_hours / (24.0 * 180.0)) if stale_age_hours < 999999 else 0.35
            status = "active"
            if total >= 4 and success_rate <= 0.2:
                status = "pruned"
            elif total >= 3 and success_rate < 0.5:
                status = "demoted"
            elif stale_age_hours >= 24 * 45:
                status = "stale"
            if stale_age_hours >= 24 * 120:
                status = "retired"
            recent_runs = list(history.get("recent_runs", []) or [])
            variant["branch_health"] = {
                "success_count": success_count,
                "failure_count": failure_count,
                "total_runs": total,
                "success_rate": round(success_rate, 3),
                "stale_age_hours": round(stale_age_hours, 2) if stale_age_hours < 999999 else None,
                "decay_penalty": round(decay_penalty, 3),
                "status": status,
                "recent_runs": recent_runs[-8:],
            }
            if status == "active":
                active_variants += 1
        family["variants"] = variants
        family["variant_count"] = len(variants)
        family["active_variant_count"] = active_variants
        family["checkpoint_map"] = RecipeMemory._build_family_checkpoint_map(variants)
        return family

    @staticmethod
    def _build_family_checkpoint_map(variants: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        merged: Dict[str, Dict[str, Any]] = {}
        for variant in variants:
            recipe_id = str(variant.get("recipe_id", "") or "")
            variant_label = str(variant.get("variant_label", "") or recipe_id)
            replay_history = dict(variant.get("replay_history", {}) or {})
            branch_health = dict(variant.get("branch_health", {}) or {})
            success_count = int(replay_history.get("success_count", 0) or 0)
            recent_runs = list(replay_history.get("recent_runs", []) or [])
            snapshots = list(variant.get("state_snapshots", []) or [])
            for snapshot in snapshots:
                signature = _semantic_checkpoint_signature(snapshot)
                entry = merged.setdefault(
                    signature,
                    {
                        "semantic_id": signature,
                        "checkpoint_name": str(snapshot.get("checkpoint_name", "") or ""),
                        "segment_type": str(snapshot.get("segment_type", "") or ""),
                        "purpose": str(snapshot.get("purpose", "") or ""),
                        "variant_count": 0,
                        "success_count": 0,
                        "failure_count": 0,
                        "failure_heat": 0.0,
                        "variants": [],
                        "checkpoint_ids": [],
                        "latest_failure_at": 0.0,
                        "trend_history": {},
                        "variant_failure_breakdown": {},
                        "variant_success_breakdown": {},
                    },
                )
                entry["variant_count"] += 1
                entry["success_count"] += success_count
                checkpoint_id = str(snapshot.get("checkpoint_id", "") or "")
                if checkpoint_id and checkpoint_id not in entry["checkpoint_ids"]:
                    entry["checkpoint_ids"].append(checkpoint_id)
                entry["variants"].append(
                    {
                        "recipe_id": recipe_id,
                        "variant_label": variant_label,
                        "status": str(branch_health.get("status", "active") or "active"),
                        "checkpoint_id": checkpoint_id,
                        "success_rate": float(branch_health.get("success_rate", 0.0) or 0.0),
                        "stale_age_hours": branch_health.get("stale_age_hours", None),
                    }
                )
                for run in recent_runs:
                    run_checkpoint_id = str(run.get("checkpoint_id", "") or "")
                    if checkpoint_id and run_checkpoint_id and run_checkpoint_id != checkpoint_id:
                        continue
                    bucket = _trend_bucket(run.get("timestamp", 0.0))
                    trend = dict(entry.get("trend_history", {}) or {})
                    bucket_entry = dict(trend.get(bucket, {}) or {"success": 0, "failure": 0})
                    if bool(run.get("ok", False)):
                        bucket_entry["success"] = int(bucket_entry.get("success", 0) or 0) + 1
                        success_breakdown = dict(entry.get("variant_success_breakdown", {}) or {})
                        success_breakdown[recipe_id] = int(success_breakdown.get(recipe_id, 0) or 0) + 1
                        entry["variant_success_breakdown"] = success_breakdown
                    else:
                        bucket_entry["failure"] = int(bucket_entry.get("failure", 0) or 0) + 1
                        failure_breakdown = dict(entry.get("variant_failure_breakdown", {}) or {})
                        failure_breakdown[recipe_id] = int(failure_breakdown.get(recipe_id, 0) or 0) + 1
                        entry["variant_failure_breakdown"] = failure_breakdown
                        entry["failure_count"] += 1
                        try:
                            entry["latest_failure_at"] = max(float(entry.get("latest_failure_at", 0.0) or 0.0), float(run.get("timestamp", 0.0) or 0.0))
                        except Exception:
                            pass
                    trend[bucket] = bucket_entry
                    entry["trend_history"] = trend
        items = list(merged.values())
        for entry in items:
            total = int(entry.get("success_count", 0) or 0) + int(entry.get("failure_count", 0) or 0)
            entry["failure_heat"] = round(float(entry.get("failure_count", 0) or 0) / max(1, total), 3)
            entry["variants"] = sorted(
                list(entry.get("variants", []) or []),
                key=lambda item: (str(item.get("status", "")) != "active", str(item.get("variant_label", ""))),
            )
            entry["trend_points"] = _sorted_trend_points(dict(entry.get("trend_history", {}) or {}))
            entry["suggested_base_variant"] = _suggest_base_variant_for_checkpoint(entry)
            entry["variant_diffs"] = _variant_diffs_for_checkpoint(entry)
        items.sort(key=lambda item: (-float(item.get("failure_heat", 0.0) or 0.0), -float(item.get("latest_failure_at", 0.0) or 0.0), str(item.get("checkpoint_name", ""))))
        return items


def infer_goal(app_name: str, steps: List[RecipeStep]) -> str:
    labels = " ".join(step.target for step in steps if step.target).lower()
    if "save" in labels:
        return f"Complete and save a task in {app_name or 'the target app'}."
    if "search" in labels:
        return f"Search and navigate within {app_name or 'the target app'}."
    if any(step.action == "type_text" for step in steps):
        return f"Open {app_name or 'the app'} and complete a typed workflow."
    return f"Replay a taught workflow in {app_name or 'the target app'}."


def _preconditions(app_name: str, steps: List[RecipeStep]) -> List[str]:
    out = []
    if app_name:
        out.append(f"{app_name} is installed or reachable.")
    if any(step.action == "type_text" for step in steps):
        out.append("Required text inputs are provided at replay time.")
    return out


def _selector_label(selector: Dict[str, Any]) -> str:
    if not isinstance(selector, dict):
        return "captured element"
    metadata = selector.get("metadata", {}) if isinstance(selector.get("metadata", {}), dict) else {}
    for key in ("name", "automation_id", "class_name"):
        value = str(metadata.get(key, "") or "").strip()
        if value:
            return value
    value = str(selector.get("value", "") or "").strip()
    return value or "captured element"


def _selector_confidence(selector: Dict[str, Any]) -> float:
    if not isinstance(selector, dict):
        return 0.35
    metadata = selector.get("metadata", {}) if isinstance(selector.get("metadata", {}), dict) else {}
    if any(str(metadata.get(key, "")).strip() for key in ("name", "automation_id")):
        return 0.9
    if str(selector.get("value", "")).strip():
        return 0.68
    return 0.35


def _click_purpose(label: str) -> str:
    low = label.lower()
    if any(token in low for token in ["submit", "save", "send"]):
        return "Commit the current step"
    if any(token in low for token in ["next", "continue", "open"]):
        return "Move to the next screen or open a target"
    if any(token in low for token in ["search", "filter"]):
        return "Focus or trigger search and filtering"
    return "Select the required UI control"


def _fallback_selectors(selector: Dict[str, Any], label: str) -> List[Dict[str, Any]]:
    options: List[Dict[str, Any]] = []
    metadata = selector.get("metadata", {}) if isinstance(selector.get("metadata", {}), dict) else {}
    auto_id = str(metadata.get("automation_id", "") or "").strip()
    name = str(metadata.get("name", "") or "").strip()
    class_name = str(metadata.get("class_name", "") or "").strip()
    value = str(selector.get("value", "") or "").strip()
    if auto_id:
        options.append({"strategy": "uia", "value": f"AutomationId={auto_id}"})
    if name:
        options.append({"strategy": "text", "value": name})
    if value and value != name:
        options.append({"strategy": "text", "value": value})
    if class_name and name:
        options.append({"strategy": "text", "value": f"{name} {class_name}"})
    if label and not any(str(item.get("value", "")).strip().lower() == label.strip().lower() for item in options):
        options.append({"strategy": "text", "value": label})
    deduped: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for item in options:
        key = f"{item.get('strategy','')}::{item.get('value','')}".strip().lower()
        if key and key not in seen:
            seen.add(key)
            deduped.append(item)
    return deduped


def _expected_state_for_click(label: str) -> str:
    low = label.lower()
    if any(token in low for token in ["compose", "new", "create"]):
        return "A new entry or composition surface is visible."
    if any(token in low for token in ["save", "submit", "send"]):
        return "The action completes and the UI confirms submission or save."
    if any(token in low for token in ["search", "filter"]):
        return "Search or filter controls become active."
    return "The intended target screen or control becomes active."


def _recovery_hint_for_click(label: str) -> str:
    low = label.lower()
    if any(token in low for token in ["menu", "more", "profile"]):
        return "If the control moved, reopen the surrounding menu and retry the selection."
    if any(token in low for token in ["compose", "new", "create"]):
        return "If the create action is not visible, return to the app home screen and retry."
    return "If the control is missing, search nearby controls with the same label before failing."


def _variable_name(text: str, idx: int) -> str:
    low = text.lower()
    if "@" in text:
        return "email_input"
    if re.fullmatch(r"https?://.+", text.strip(), flags=re.I):
        return "url_input"
    if len(text.split()) >= 3:
        return f"text_block_{idx}"
    return f"text_input_{idx}"


def _recipe_confidence(recipe: LearnedMissionRecipe) -> float:
    if not recipe.steps:
        return 0.2
    base = sum(step.confidence for step in recipe.steps) / len(recipe.steps)
    if recipe.required_inputs:
        base += 0.03
    if recipe.success_signals:
        base += 0.04
    if recipe.state_snapshots:
        base += 0.03
    if recipe.robustness_notes:
        base -= 0.06
    return max(0.2, min(0.98, base))


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")[:80] or "recipe"


def _family_id(app_name: str, goal: str) -> str:
    return _slug(f"{app_name}_{goal}")[:100]


def _build_state_snapshots(frames: List[Dict[str, Any]], segments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    snapshots: List[Dict[str, Any]] = []
    for segment in segments:
        try:
            start_index = int(segment.get("start_index", -1))
            end_index = int(segment.get("end_index", -1))
        except Exception:
            continue
        if start_index < 0 or end_index < 0 or start_index >= len(frames) or end_index >= len(frames):
            continue
        start_frame = dict(frames[start_index] or {})
        end_frame = dict(frames[end_index] or {})
        pre_selectors = list(start_frame.get("selector_candidates", []) or [])
        post_selectors = list(end_frame.get("selector_candidates", []) or [])
        checkpoint_name = _snapshot_checkpoint_name(segment=segment, start_frame=start_frame, end_frame=end_frame, ordinal=len(snapshots) + 1)
        snapshots.append(
            {
                "segment_index": len(snapshots),
                "checkpoint_id": _slug(checkpoint_name),
                "checkpoint_name": checkpoint_name,
                "segment_type": str(segment.get("segment_type", "") or ""),
                "purpose": str(segment.get("purpose", "") or ""),
                "start_source_index": start_index,
                "end_source_index": end_index,
                "expected_pre_state": str(start_frame.get("expected_state", "") or ""),
                "expected_post_state": str(end_frame.get("expected_state", "") or ""),
                "expected_pre_role": str(start_frame.get("target_role", "") or ""),
                "expected_post_role": str(end_frame.get("target_role", "") or ""),
                "pre_labels": [str(start_frame.get("target_label", "") or "")] if str(start_frame.get("target_label", "") or "") else [],
                "post_labels": [str(end_frame.get("target_label", "") or "")] if str(end_frame.get("target_label", "") or "") else [],
                "precondition_selectors": pre_selectors,
                "success_selectors": post_selectors,
                "recovery_hint": _snapshot_recovery_hint(start_frame, end_frame),
            }
        )
    return snapshots


def _snapshot_checkpoint_name(
    *,
    segment: Dict[str, Any],
    start_frame: Dict[str, Any],
    end_frame: Dict[str, Any],
    ordinal: int,
) -> str:
    seg_type = str(segment.get("segment_type", "") or "").strip().lower() or "segment"
    purpose = str(segment.get("purpose", "") or "").strip()
    start_label = str(start_frame.get("target_label", "") or "").strip()
    end_label = str(end_frame.get("target_label", "") or "").strip()
    anchor = end_label or start_label or purpose or seg_type.title()
    return f"{ordinal}. {seg_type.replace('_', ' ').title()} - {anchor}"


def _semantic_checkpoint_signature(snapshot: Dict[str, Any]) -> str:
    seg_type = str(snapshot.get("segment_type", "") or "").strip().lower()
    purpose = str(snapshot.get("purpose", "") or "").strip().lower()
    pre_role = str(snapshot.get("expected_pre_role", "") or "").strip().lower()
    post_role = str(snapshot.get("expected_post_role", "") or "").strip().lower()
    labels = list(snapshot.get("pre_labels", []) or []) + list(snapshot.get("post_labels", []) or [])
    label_anchor = ""
    for label in labels:
        low = str(label or "").strip().lower()
        if low:
            label_anchor = low
            break
    return _slug(f"{seg_type}|{purpose}|{pre_role}|{post_role}|{label_anchor}")


def _trend_bucket(timestamp: Any) -> str:
    try:
        ts = float(timestamp or 0.0)
    except Exception:
        ts = 0.0
    if ts <= 0:
        return "unknown"
    return time.strftime("%Y-%m-%d", time.localtime(ts))


def _sorted_trend_points(trend_history: Dict[str, Any]) -> List[Dict[str, Any]]:
    points: List[Dict[str, Any]] = []
    for key in sorted(trend_history.keys()):
        value = dict(trend_history.get(key, {}) or {})
        points.append(
            {
                "date": key,
                "success": int(value.get("success", 0) or 0),
                "failure": int(value.get("failure", 0) or 0),
            }
        )
    return points[-8:]


def _suggest_base_variant_for_checkpoint(entry: Dict[str, Any]) -> Dict[str, Any]:
    variants = list(entry.get("variants", []) or [])
    failure_breakdown = dict(entry.get("variant_failure_breakdown", {}) or {})
    success_breakdown = dict(entry.get("variant_success_breakdown", {}) or {})
    ranked = sorted(
        variants,
        key=lambda item: (
            str(item.get("status", "")) != "active",
            -float(item.get("success_rate", 0.0) or 0.0),
            int(failure_breakdown.get(str(item.get("recipe_id", "") or ""), 0) or 0),
            -int(success_breakdown.get(str(item.get("recipe_id", "") or ""), 0) or 0),
            float(item.get("stale_age_hours", 999999.0) or 999999.0),
        ),
    )
    return dict(ranked[0] if ranked else {})


def _variant_diffs_for_checkpoint(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    variants = list(entry.get("variants", []) or [])
    base = dict(entry.get("suggested_base_variant", {}) or {})
    base_recipe_id = str(base.get("recipe_id", "") or "")
    failure_breakdown = dict(entry.get("variant_failure_breakdown", {}) or {})
    success_breakdown = dict(entry.get("variant_success_breakdown", {}) or {})
    diffs: List[Dict[str, Any]] = []
    for variant in variants:
        recipe_id = str(variant.get("recipe_id", "") or "")
        if recipe_id == base_recipe_id:
            continue
        diffs.append(
            {
                "recipe_id": recipe_id,
                "variant_label": str(variant.get("variant_label", "") or recipe_id),
                "status": str(variant.get("status", "") or ""),
                "failure_delta": int(failure_breakdown.get(recipe_id, 0) or 0) - int(failure_breakdown.get(base_recipe_id, 0) or 0),
                "success_delta": int(success_breakdown.get(recipe_id, 0) or 0) - int(success_breakdown.get(base_recipe_id, 0) or 0),
                "success_rate_delta": round(float(variant.get("success_rate", 0.0) or 0.0) - float(base.get("success_rate", 0.0) or 0.0), 3),
            }
        )
    return diffs[:6]


def _snapshot_recovery_hint(start_frame: Dict[str, Any], end_frame: Dict[str, Any]) -> str:
    start_label = str(start_frame.get("target_label", "") or "").strip()
    end_label = str(end_frame.get("target_label", "") or "").strip()
    if start_label and end_label and start_label != end_label:
        return f"Return to '{start_label}' and drive the workflow back toward '{end_label}'."
    if start_label:
        return f"Re-focus the workflow around '{start_label}' before retrying this segment."
    return "Re-establish the expected UI state before retrying this segment."
