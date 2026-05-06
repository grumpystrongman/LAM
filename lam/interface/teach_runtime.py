from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List

from lam.interface.learned_recipe import LearnedMissionRecipe


@dataclass(slots=True)
class ObservationFrame:
    index: int
    action: str
    app_name: str
    target_label: str = ""
    target_role: str = ""
    selector: Dict[str, Any] = field(default_factory=dict)
    selector_candidates: List[Dict[str, Any]] = field(default_factory=list)
    typed_text: str = ""
    hotkey: str = ""
    expected_state: str = ""
    note: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ObservationSegment:
    segment_type: str
    purpose: str
    start_index: int
    end_index: int
    actions: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class ScreenObservationStream:
    def build(self, *, app_name: str, compressed_events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        frames: List[ObservationFrame] = []
        for idx, event in enumerate(compressed_events):
            action = str(event.get("action", "") or "")
            payload = dict(event.get("payload", {}) or {})
            selector = dict(payload.get("selector", {}) or {})
            metadata = selector.get("metadata", {}) if isinstance(selector.get("metadata", {}), dict) else {}
            frame = ObservationFrame(
                index=idx,
                action=action,
                app_name=(app_name or "").strip().lower(),
                target_label=_selector_label(selector),
                target_role=str(metadata.get("class_name", "") or ""),
                selector=selector,
                selector_candidates=_selector_candidates(selector),
                typed_text=str(payload.get("text", "") or ""),
                hotkey=str(payload.get("keys", "") or ""),
                expected_state=_frame_expected_state(action, payload, selector),
                note=_frame_note(action, payload, selector),
            )
            frames.append(frame)
        return [frame.to_dict() for frame in frames]


class DemonstrationSegmenter:
    def segment(self, frames: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not frames:
            return []
        segments: List[ObservationSegment] = []
        start = 0
        current_type = _segment_type(frames[0])
        current_actions: List[str] = [str(frames[0].get("action", ""))]
        current_purpose = _segment_purpose(frames[0], current_type)
        for idx in range(1, len(frames)):
            frame = frames[idx]
            seg_type = _segment_type(frame)
            if seg_type != current_type:
                segments.append(
                    ObservationSegment(
                        segment_type=current_type,
                        purpose=current_purpose,
                        start_index=start,
                        end_index=idx - 1,
                        actions=current_actions,
                    )
                )
                start = idx
                current_type = seg_type
                current_actions = [str(frame.get("action", ""))]
                current_purpose = _segment_purpose(frame, seg_type)
            else:
                current_actions.append(str(frame.get("action", "")))
        segments.append(
            ObservationSegment(
                segment_type=current_type,
                purpose=current_purpose,
                start_index=start,
                end_index=len(frames) - 1,
                actions=current_actions,
            )
        )
        return [segment.to_dict() for segment in segments]


class TeachReplayRuntime:
    def choose_variant(
        self,
        *,
        family: Dict[str, Any],
        current_state: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        payload = dict(family or {})
        variants = list(payload.get("variants", []) or [])
        state = dict(current_state or {})
        ranked: List[Dict[str, Any]] = []
        for variant in variants:
            health = dict(variant.get("branch_health", {}) or {})
            status = str(health.get("status", "active") or "active").strip().lower()
            if status in {"pruned", "retired"}:
                continue
            score = self._variant_match_score(variant, state)
            ranked.append(
                {
                    "recipe_id": str(variant.get("recipe_id", "")),
                    "family_id": str(payload.get("family_id", "") or variant.get("family_id", "")),
                    "path": str(variant.get("path", "") or ""),
                    "confidence": float(variant.get("confidence", 0.0) or 0.0),
                    "match_score": score,
                    "branch_health": health,
                    "learned_goal": str(variant.get("learned_goal", "") or ""),
                    "variant": variant,
                }
            )
        ranked.sort(key=lambda item: (float(item.get("match_score", 0.0)), float(item.get("confidence", 0.0))), reverse=True)
        return {
            "ok": bool(ranked),
            "family_id": str(payload.get("family_id", "") or ""),
            "selected_variant": ranked[0] if ranked else {},
            "ranked_variants": ranked,
        }

    def build_plan(
        self,
        *,
        recipe: LearnedMissionRecipe | Dict[str, Any] | None = None,
        family: Dict[str, Any] | None = None,
        input_bindings: Dict[str, Any] | None = None,
        current_state: Dict[str, Any] | None = None,
        resume_from_source_index: int | None = None,
        resume_from_segment_index: int | None = None,
        resume_from_checkpoint_id: str = "",
    ) -> Dict[str, Any]:
        variant_selection: Dict[str, Any] = {}
        if family:
            variant_selection = self.choose_variant(family=family, current_state=current_state)
            selected = dict(variant_selection.get("selected_variant", {}).get("variant", {}) or {})
            payload = selected
        else:
            payload = recipe.to_dict() if isinstance(recipe, LearnedMissionRecipe) else dict(recipe or {})
        bindings = dict(input_bindings or {})
        snapshots = list(payload.get("state_snapshots", []) or [])
        steps: List[Dict[str, Any]] = []
        missing_inputs: List[str] = []
        for item in list(payload.get("steps", []) or []):
            step = dict(item or {})
            action = str(step.get("action", "") or "")
            try:
                source_index = int(step.get("source_index", -1))
            except Exception:
                source_index = -1
            step_segment_index = _segment_index_for_source(snapshots, source_index)
            step_checkpoint_id = _checkpoint_id_for_source(snapshots, source_index)
            if resume_from_checkpoint_id and step_checkpoint_id and _checkpoint_sort_key(snapshots, step_checkpoint_id) < _checkpoint_sort_key(snapshots, resume_from_checkpoint_id):
                continue
            if resume_from_segment_index is not None and step_segment_index >= 0 and step_segment_index < int(resume_from_segment_index):
                continue
            if resume_from_source_index is not None and source_index >= 0 and source_index < int(resume_from_source_index):
                continue
            for snapshot in _snapshots_starting(snapshots, source_index):
                snapshot_start = _int_value(snapshot.get("start_source_index", -1))
                snapshot_segment = _int_value(snapshot.get("segment_index", -1))
                snapshot_checkpoint_id = str(snapshot.get("checkpoint_id", "") or "")
                snapshot_checkpoint_name = str(snapshot.get("checkpoint_name", "") or "")
                if resume_from_checkpoint_id and snapshot_checkpoint_id and _checkpoint_sort_key(snapshots, snapshot_checkpoint_id) < _checkpoint_sort_key(snapshots, resume_from_checkpoint_id):
                    continue
                if resume_from_segment_index is not None and snapshot_segment >= 0 and snapshot_segment < int(resume_from_segment_index):
                    continue
                if resume_from_source_index is not None and snapshot_start >= 0 and snapshot_start < int(resume_from_source_index):
                    continue
                if snapshot.get("precondition_selectors"):
                    steps.append(
                        {
                            "action": "assert_state",
                            "phase": "pre",
                            "source_index": snapshot_start,
                            "segment_index": snapshot_segment,
                            "checkpoint_id": snapshot_checkpoint_id,
                            "checkpoint_name": snapshot_checkpoint_name,
                            "candidate_selectors": list(snapshot.get("precondition_selectors", []) or []),
                            "description": str(snapshot.get("expected_pre_state", "") or snapshot.get("purpose", "")),
                            "optional": True,
                            "recovery_hint": str(snapshot.get("recovery_hint", "") or ""),
                        }
                    )
            if action == "open_app":
                if resume_from_source_index is not None or resume_from_segment_index is not None or resume_from_checkpoint_id:
                    continue
                steps.append({"action": "open_app", "app": str(step.get("target", ""))})
            elif action == "click":
                selector = dict(step.get("selector", {}) or {})
                label = str(step.get("target", "") or "captured element")
                fallbacks = list(step.get("fallback_selectors", []) or [])
                if step.get("expected_state"):
                    steps.append(
                        {
                            "action": "assert_visible",
                            "selector": (selector or (fallbacks[0] if fallbacks else {"strategy": "text", "value": label})),
                            "timeout_ms": 2500,
                            "optional": True,
                            "purpose": "verify target is present before clicking",
                        }
                    )
                steps.append(
                        {
                            "action": "click",
                            "source_index": source_index,
                            "segment_index": step_segment_index,
                            "checkpoint_id": step_checkpoint_id,
                            "selector": selector or {"strategy": "text", "value": label},
                            "fallback_selectors": fallbacks,
                            "expected_state": str(step.get("expected_state", "") or ""),
                            "recovery_hint": str(step.get("recovery_hint", "") or ""),
                        }
                )
            elif action == "type_text":
                var_name = str(step.get("variable_name", "") or "")
                if var_name and var_name in bindings:
                    value = str(bindings.get(var_name, ""))
                else:
                    value = str(step.get("default_value", "") or "")
                    if var_name and not value:
                        missing_inputs.append(var_name)
                steps.append(
                    {
                        "action": "type_text",
                        "source_index": source_index,
                        "segment_index": step_segment_index,
                        "checkpoint_id": step_checkpoint_id,
                        "text": value,
                        "expected_state": str(step.get("expected_state", "") or ""),
                        "recovery_hint": str(step.get("recovery_hint", "") or ""),
                    }
                )
            elif action == "hotkey":
                steps.append(
                    {
                        "action": "hotkey",
                        "source_index": source_index,
                        "segment_index": step_segment_index,
                        "checkpoint_id": step_checkpoint_id,
                        "keys": str(step.get("target", "")),
                        "expected_state": str(step.get("expected_state", "") or ""),
                        "recovery_hint": str(step.get("recovery_hint", "") or ""),
                    }
                )
            elif action == "wait":
                try:
                    seconds = int(step.get("target", "1") or 1)
                except Exception:
                    seconds = 1
                steps.append(
                    {
                        "action": "wait",
                        "source_index": source_index,
                        "segment_index": step_segment_index,
                        "checkpoint_id": step_checkpoint_id,
                        "seconds": max(1, seconds),
                        "expected_state": str(step.get("expected_state", "") or ""),
                        "recovery_hint": str(step.get("recovery_hint", "") or ""),
                    }
                )
            for snapshot in _snapshots_ending(snapshots, source_index):
                snapshot_end = _int_value(snapshot.get("end_source_index", -1))
                snapshot_segment = _int_value(snapshot.get("segment_index", -1))
                snapshot_checkpoint_id = str(snapshot.get("checkpoint_id", "") or "")
                snapshot_checkpoint_name = str(snapshot.get("checkpoint_name", "") or "")
                if resume_from_checkpoint_id and snapshot_checkpoint_id and _checkpoint_sort_key(snapshots, snapshot_checkpoint_id) < _checkpoint_sort_key(snapshots, resume_from_checkpoint_id):
                    continue
                if resume_from_segment_index is not None and snapshot_segment >= 0 and snapshot_segment < int(resume_from_segment_index):
                    continue
                if resume_from_source_index is not None and snapshot_end >= 0 and snapshot_end < int(resume_from_source_index):
                    continue
                if snapshot.get("success_selectors"):
                    steps.append(
                        {
                            "action": "assert_state",
                            "phase": "post",
                            "source_index": snapshot_end,
                            "segment_index": snapshot_segment,
                            "checkpoint_id": snapshot_checkpoint_id,
                            "checkpoint_name": snapshot_checkpoint_name,
                            "candidate_selectors": list(snapshot.get("success_selectors", []) or []),
                            "description": str(snapshot.get("expected_post_state", "") or snapshot.get("purpose", "")),
                            "optional": False,
                            "recovery_hint": str(snapshot.get("recovery_hint", "") or ""),
                        }
                    )
        confidence = float(payload.get("confidence", 0.0) or 0.0)
        can_autorun = confidence >= 0.6 and not missing_inputs
        return {
            "ok": True,
            "recipe_id": str(payload.get("recipe_id", "")),
            "family_id": str(payload.get("family_id", "") or variant_selection.get("family_id", "")),
            "app_name": str(payload.get("app_name", "")),
            "learned_goal": str(payload.get("learned_goal", "")),
            "selected_variant": dict(variant_selection.get("selected_variant", {}) or {}),
            "ranked_variants": list(variant_selection.get("ranked_variants", []) or []),
            "steps": steps,
            "missing_inputs": missing_inputs,
            "preconditions": list(payload.get("preconditions", []) or []),
            "success_signals": list(payload.get("success_signals", []) or []),
            "state_snapshots": snapshots,
            "state_checks": _collect_state_checks(steps),
            "resume_from_source_index": resume_from_source_index,
            "resume_from_segment_index": resume_from_segment_index,
            "resume_from_checkpoint_id": resume_from_checkpoint_id,
            "confidence": confidence,
            "can_autorun": can_autorun,
            "pause_reason": "" if can_autorun else ("Missing required inputs." if missing_inputs else "Recipe confidence too low for direct autorun."),
        }

    def load_recipe(self, path: str | Path) -> Dict[str, Any]:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        return dict(payload)

    def _variant_match_score(self, variant: Dict[str, Any], state: Dict[str, Any]) -> float:
        if not state:
            return float(variant.get("confidence", 0.0) or 0.0)
        labels = [str(item).strip().lower() for item in list(state.get("visible_labels", []) or []) if str(item).strip()]
        selectors = [str(item).strip().lower() for item in list(state.get("selector_values", []) or []) if str(item).strip()]
        app_name = str(state.get("app_name", "") or "").strip().lower()
        score = float(variant.get("confidence", 0.0) or 0.0) * 0.4
        if app_name and app_name == str(variant.get("app_name", "") or "").strip().lower():
            score += 0.2
        replay_history = dict(variant.get("replay_history", {}) or {})
        branch_health = dict(variant.get("branch_health", {}) or {})
        success_count = int(replay_history.get("success_count", 0) or 0)
        failure_count = int(replay_history.get("failure_count", 0) or 0)
        if success_count or failure_count:
            total = max(1, success_count + failure_count)
            score += 0.2 * (success_count / total)
            score -= 0.12 * (failure_count / total)
        status = str(branch_health.get("status", "") or "").strip().lower()
        if status == "demoted":
            score -= 0.08
        elif status == "stale":
            score -= 0.05
        elif status == "retired":
            score -= 0.2
        stale_age_hours = branch_health.get("stale_age_hours", None)
        if stale_age_hours is not None:
            try:
                score -= min(0.08, float(stale_age_hours) / (24.0 * 365.0))
            except Exception:
                pass
        decay_penalty = branch_health.get("decay_penalty", None)
        if decay_penalty is not None:
            try:
                score -= float(decay_penalty)
            except Exception:
                pass
        for snapshot in list(variant.get("state_snapshots", []) or []):
            for selector in list(snapshot.get("precondition_selectors", []) or []):
                value = str(selector.get("value", "") or "").strip().lower()
                if value and (value in labels or value in selectors):
                    score += 0.15
                    break
            pre_role = str(snapshot.get("expected_pre_role", "") or "").strip().lower()
            post_role = str(snapshot.get("expected_post_role", "") or "").strip().lower()
            roles = [str(item).strip().lower() for item in list(state.get("visible_roles", []) or []) if str(item).strip()]
            if pre_role and pre_role in roles:
                score += 0.05
            if post_role and post_role in roles:
                score += 0.04
            for label in list(snapshot.get("pre_labels", []) or []) + list(snapshot.get("post_labels", []) or []):
                low = str(label or "").strip().lower()
                if low and low in labels:
                    score += 0.03
                    break
        tree_signature = str(state.get("tree_signature", "") or "").strip().lower()
        if tree_signature:
            for snapshot in list(variant.get("state_snapshots", []) or []):
                role_terms = [str(snapshot.get("expected_pre_role", "") or "").strip().lower(), str(snapshot.get("expected_post_role", "") or "").strip().lower()]
                if any(role and role in tree_signature for role in role_terms):
                    score += 0.03
                    break
        for step in list(variant.get("steps", []) or []):
            if str(step.get("action", "") or "") != "click":
                continue
            target = str(step.get("target", "") or "").strip().lower()
            if target and target in labels:
                score += 0.12
                break
            selector = step.get("selector", {}) if isinstance(step.get("selector", {}), dict) else {}
            selector_value = str(selector.get("value", "") or "").strip().lower()
            if selector_value and (selector_value in labels or selector_value in selectors):
                score += 0.1
                break
        goal = str(variant.get("learned_goal", "") or "").strip().lower()
        for label in labels:
            if label and label in goal:
                score += 0.05
        return round(score, 3)

    def should_reassign_branch(self, trace: List[Dict[str, Any]]) -> bool:
        if not trace:
            return False
        for item in reversed(trace):
            if str(item.get("action", "") or "") != "assert_state":
                continue
            if str(item.get("phase", "") or "").strip().lower() == "post" and not bool(item.get("ok", False)):
                return True
            break
        return False

    def reassignment_checkpoint(self, trace: List[Dict[str, Any]]) -> int | None:
        if not trace:
            return None
        for item in reversed(trace):
            if str(item.get("action", "") or "") != "assert_state":
                continue
            if str(item.get("phase", "") or "").strip().lower() == "post" and not bool(item.get("ok", False)):
                try:
                    return int(item.get("source_index", -1))
                except Exception:
                    return None
            break
        return None

    def reassignment_segment(self, trace: List[Dict[str, Any]]) -> int | None:
        if not trace:
            return None
        for item in reversed(trace):
            if str(item.get("action", "") or "") != "assert_state":
                continue
            if str(item.get("phase", "") or "").strip().lower() == "post" and not bool(item.get("ok", False)):
                try:
                    return int(item.get("segment_index", -1))
                except Exception:
                    return None
            break
        return None

    def reassignment_checkpoint_id(self, trace: List[Dict[str, Any]]) -> str:
        if not trace:
            return ""
        for item in reversed(trace):
            if str(item.get("action", "") or "") != "assert_state":
                continue
            if str(item.get("phase", "") or "").strip().lower() == "post" and not bool(item.get("ok", False)):
                return str(item.get("checkpoint_id", "") or "")
            break
        return ""


def _selector_label(selector: Dict[str, Any]) -> str:
    if not isinstance(selector, dict):
        return ""
    metadata = selector.get("metadata", {}) if isinstance(selector.get("metadata", {}), dict) else {}
    for key in ("name", "automation_id", "class_name"):
        value = str(metadata.get(key, "") or "").strip()
        if value:
            return value
    return str(selector.get("value", "") or "").strip()


def _frame_note(action: str, payload: Dict[str, Any], selector: Dict[str, Any]) -> str:
    if action == "click":
        return f"Observed click on {_selector_label(selector) or 'captured element'}."
    if action == "type_text":
        text = str(payload.get("text", "") or "")
        return f"Observed text input of {len(text)} character(s)."
    if action == "hotkey":
        return f"Observed hotkey {payload.get('keys', '')}."
    if action == "wait":
        return f"Observed wait of {payload.get('seconds', 1)} second(s)."
    return "Observed UI action."


def _frame_expected_state(action: str, payload: Dict[str, Any], selector: Dict[str, Any]) -> str:
    if action == "click":
        label = _selector_label(selector).lower()
        if any(token in label for token in ["compose", "new", "create"]):
            return "A create or compose surface should appear."
        if any(token in label for token in ["save", "submit", "send"]):
            return "The UI should confirm completion or advance."
        return "The intended control or next screen should become active."
    if action == "type_text":
        return "The target field should contain the entered value."
    if action == "hotkey":
        return "The current workflow step should advance."
    if action == "wait":
        return "The next screen state should become visible after waiting."
    return ""


def _selector_candidates(selector: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(selector, dict):
        return []
    metadata = selector.get("metadata", {}) if isinstance(selector.get("metadata", {}), dict) else {}
    items: List[Dict[str, Any]] = []
    auto_id = str(metadata.get("automation_id", "") or "").strip()
    name = str(metadata.get("name", "") or "").strip()
    value = str(selector.get("value", "") or "").strip()
    if auto_id:
        items.append({"strategy": "uia", "value": f"AutomationId={auto_id}"})
    if name:
        items.append({"strategy": "text", "value": name})
    if value and value != name:
        items.append({"strategy": "text", "value": value})
    return items


def _collect_state_checks(steps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    checks: List[Dict[str, Any]] = []
    for index, step in enumerate(steps):
        if str(step.get("action", "") or "") == "assert_state":
            checks.append(
                {
                    "step_index": index,
                    "action": "assert_state",
                    "checkpoint_id": str(step.get("checkpoint_id", "") or ""),
                    "checkpoint_name": str(step.get("checkpoint_name", "") or ""),
                    "expected_state": str(step.get("description", "") or ""),
                    "recovery_hint": str(step.get("recovery_hint", "") or ""),
                }
            )
            continue
        expected = str(step.get("expected_state", "") or "").strip()
        if not expected:
            continue
        checks.append(
            {
                "step_index": index,
                "action": str(step.get("action", "") or ""),
                "source_index": _int_value(step.get("source_index", -1)),
                "checkpoint_id": str(step.get("checkpoint_id", "") or ""),
                "expected_state": expected,
                "recovery_hint": str(step.get("recovery_hint", "") or ""),
            }
        )
    return checks


def _int_value(value: Any, default: int = -1) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _snapshots_starting(snapshots: List[Dict[str, Any]], source_index: int) -> List[Dict[str, Any]]:
    if source_index < 0:
        return []
    out: List[Dict[str, Any]] = []
    for snapshot in snapshots:
        try:
            start_index = int(snapshot.get("start_source_index", -1))
        except Exception:
            start_index = -1
        if start_index == source_index:
            out.append(snapshot)
    return out


def _snapshots_ending(snapshots: List[Dict[str, Any]], source_index: int) -> List[Dict[str, Any]]:
    if source_index < 0:
        return []
    out: List[Dict[str, Any]] = []
    for snapshot in snapshots:
        try:
            end_index = int(snapshot.get("end_source_index", -1))
        except Exception:
            end_index = -1
        if end_index == source_index:
            out.append(snapshot)
    return out


def _segment_index_for_source(snapshots: List[Dict[str, Any]], source_index: int) -> int:
    if source_index < 0:
        return -1
    for snapshot in snapshots:
        start_index = _int_value(snapshot.get("start_source_index", -1))
        end_index = _int_value(snapshot.get("end_source_index", -1))
        if start_index <= source_index <= end_index:
            return _int_value(snapshot.get("segment_index", -1))
    return -1


def _checkpoint_id_for_source(snapshots: List[Dict[str, Any]], source_index: int) -> str:
    if source_index < 0:
        return ""
    for snapshot in snapshots:
        start = _int_value(snapshot.get("start_source_index", -1))
        end = _int_value(snapshot.get("end_source_index", -1))
        if start <= source_index <= end:
            return str(snapshot.get("checkpoint_id", "") or "")
    return ""


def _checkpoint_sort_key(snapshots: List[Dict[str, Any]], checkpoint_id: str) -> int:
    target = str(checkpoint_id or "").strip()
    if not target:
        return -1
    for snapshot in snapshots:
        if str(snapshot.get("checkpoint_id", "") or "") == target:
            return _int_value(snapshot.get("segment_index", -1))
    return -1


def _segment_type(frame: Dict[str, Any]) -> str:
    action = str(frame.get("action", "") or "")
    if action == "click":
        label = str(frame.get("target_label", "") or "").lower()
        if any(token in label for token in ["open", "new", "compose", "menu", "search"]):
            return "navigation"
        return "selection"
    if action == "type_text":
        return "data_entry"
    if action == "hotkey":
        return "commit" if str(frame.get("hotkey", "")).lower() in {"enter", "return"} else "shortcut"
    if action == "wait":
        return "state_transition"
    return "other"


def _segment_purpose(frame: Dict[str, Any], seg_type: str) -> str:
    if seg_type == "navigation":
        return "Move to the required area of the app."
    if seg_type == "selection":
        return "Choose the target control or option."
    if seg_type == "data_entry":
        return "Provide task-specific input."
    if seg_type == "commit":
        return "Commit the current action or move to the next state."
    if seg_type == "state_transition":
        return "Allow the interface to update."
    return "Observed supporting workflow action."
