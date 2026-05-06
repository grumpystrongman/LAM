from __future__ import annotations

import re
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

from lam.adapters.uia_adapter import UIAAdapter
from lam.interface.app_launcher import normalize_app_name, open_installed_app
from lam.interface.clipboard_capture import capture_clipboard_image
from lam.interface.password_vault import LocalPasswordVault
from lam.interface.teach_runtime import TeachReplayRuntime


@dataclass(slots=True)
class SequenceResult:
    ok: bool
    steps: List[Dict[str, Any]]
    trace: List[Dict[str, Any]]
    next_step_index: int
    done: bool
    paused_for_credentials: bool
    pause_reason: str
    artifacts: Dict[str, str]
    error: str = ""


def assess_risk(plan: Dict[str, Any]) -> Dict[str, Any]:
    risky_keywords = {"submit", "finalize", "delete", "remove", "pay", "purchase", "send", "transfer"}
    risky_steps: List[Dict[str, Any]] = []
    for i, step in enumerate(plan.get("steps", [])):
        text_parts = [step.get("action", ""), step.get("text", ""), step.get("keys", "")]
        sel = step.get("selector", {})
        if isinstance(sel, dict):
            text_parts.append(str(sel.get("value", "")))
        hay = " ".join(t.lower() for t in text_parts if isinstance(t, str))
        if any(k in hay for k in risky_keywords):
            risky_steps.append({"index": i, "step": step})
    return {"requires_confirmation": len(risky_steps) > 0, "risky_steps": risky_steps}


def build_plan(instruction: str) -> Dict[str, Any]:
    text = instruction.strip()
    normalized = re.sub(r"\s+", " ", text)

    app_name = ""
    open_match = re.search(r"\bopen\s+(.+?)(?:\s+app)?(?:\s+then|\s+and|$)", normalized, flags=re.IGNORECASE)
    if open_match:
        app_name = normalize_app_name(open_match.group(1).strip())

    fragments = re.split(r"\bthen\b|;", normalized, flags=re.IGNORECASE)
    steps: List[Dict[str, Any]] = []
    for fragment in fragments:
        part = fragment.strip(" .")
        if not part:
            continue

        m_open = re.match(r"^open\s+(.+?)(?:\s+app)?$", part, flags=re.IGNORECASE)
        if m_open:
            target = normalize_app_name(m_open.group(1).strip())
            steps.append({"action": "open_app", "app": target})
            continue

        m_focus = re.match(r"^(?:focus|switch to)\s+(.+)$", part, flags=re.IGNORECASE)
        if m_focus:
            steps.append({"action": "focus_window", "selector": {"strategy": "text", "value": m_focus.group(1).strip()}})
            continue

        m_type = re.match(r'^type\s+"(.+)"$', part, flags=re.IGNORECASE)
        if not m_type:
            m_type = re.match(r"^type\s+(.+)$", part, flags=re.IGNORECASE)
        if m_type:
            steps.append({"action": "type_text", "text": m_type.group(1).strip()})
            continue

        m_press = re.match(r"^(?:press|hotkey)\s+(.+)$", part, flags=re.IGNORECASE)
        if m_press:
            steps.append({"action": "hotkey", "keys": m_press.group(1).strip()})
            continue

        m_wait = re.match(r"^wait\s+([0-9]+)\s*(?:s|sec|seconds)?$", part, flags=re.IGNORECASE)
        if m_wait:
            steps.append({"action": "wait", "seconds": int(m_wait.group(1))})
            continue

        m_scroll = re.match(r"^scroll\s+(down|up)\s*([0-9]*)$", part, flags=re.IGNORECASE)
        if m_scroll:
            direction = m_scroll.group(1).lower()
            amount = int(m_scroll.group(2)) if m_scroll.group(2).isdigit() else 1
            steps.append({"action": "scroll", "direction": direction, "amount": max(1, amount)})
            continue

        m_find = re.match(r'^(?:find|locate)\s+(?:text\s+)?["\']?(.+?)["\']?$', part, flags=re.IGNORECASE)
        if m_find:
            steps.append({"action": "visual_search", "text": m_find.group(1).strip()})
            continue

        if re.match(r"^click\s+found$", part, flags=re.IGNORECASE):
            steps.append({"action": "click_found"})
            continue

        m_click = re.match(r"^click\s+(.+)$", part, flags=re.IGNORECASE)
        if m_click:
            steps.append({"action": "click", "selector": {"strategy": "text", "value": m_click.group(1).strip()}})
            continue

        m_login = re.match(r"^(?:login with|use credentials(?: for)?)\s+(.+)$", part, flags=re.IGNORECASE)
        if m_login:
            steps.append({"action": "use_credentials", "service": m_login.group(1).strip(), "submit": ("submit" in part.lower() or "enter" in part.lower())})
            continue

        m_clipboard = re.match(
            r"^(?:capture\s+(?:the\s+)?)?clipboard(?:\s+image)?|^(?:save\s+(?:the\s+)?)clipboard(?:\s+image)?|^(?:import\s+)(?:image\s+from\s+)?clipboard(?:\s+image)?",
            part,
            flags=re.IGNORECASE,
        )
        if m_clipboard:
            target_match = re.search(r"\s+(?:to|as)\s+(.+)$", part, flags=re.IGNORECASE)
            target = str(target_match.group(1) if target_match else "").strip().strip("\"'")
            steps.append({"action": "capture_clipboard_image", "output_path": target, "source": "system_clipboard"})
            continue

        steps.append({"action": "note", "text": part})

    if app_name and (not steps or steps[0].get("action") != "open_app"):
        steps.insert(0, {"action": "open_app", "app": app_name})

    return {
        "instruction": instruction,
        "app_name": app_name,
        "steps": steps,
        "checkpoint_after_open": True if app_name else False,
    }


def execute_plan(
    plan: Dict[str, Any],
    start_index: int = 0,
    step_mode: bool = False,
    allow_input_fallback: bool = True,
    human_like_interaction: bool = False,
) -> SequenceResult:
    adapter = UIAAdapter(
        allow_input_fallback=allow_input_fallback,
        dry_run=False,
        human_like=bool(human_like_interaction),
    )
    vault = LocalPasswordVault()
    steps: List[Dict[str, Any]] = plan.get("steps", [])
    trace: List[Dict[str, Any]] = []
    artifacts: Dict[str, str] = {}
    index = start_index
    last_visual_point: Dict[str, int] | None = None
    app_hint = str(plan.get("app_name", "") or "")

    while index < len(steps):
        step = steps[index]
        action = step.get("action", "")
        try:
            if action == "open_app":
                ok, launched = open_installed_app(step.get("app", ""))
                trace.append({"step": index, "action": action, "ok": ok, "launched": launched})
                if not ok:
                    return SequenceResult(False, steps, trace, index, False, False, "", artifacts, "App not found")
                if plan.get("checkpoint_after_open") and start_index == 0:
                    return SequenceResult(
                        True,
                        steps,
                        trace,
                        index + 1,
                        False,
                        True,
                        "Login checkpoint: enter credentials if prompted, then click Resume.",
                        artifacts,
                    )
            elif action == "focus_window":
                adapter.focus_window(step.get("selector", {}))
                trace.append({"step": index, "action": action, "ok": True})
            elif action == "assert_visible":
                try:
                    adapter.assert_visible(step.get("selector", {}), timeout_ms=int(step.get("timeout_ms", 2500) or 2500))
                    trace.append({"step": index, "action": action, "ok": True, "optional": bool(step.get("optional", False))})
                except Exception as exc:
                    if bool(step.get("optional", False)):
                        trace.append({"step": index, "action": action, "ok": False, "optional": True, "warning": str(exc)})
                    else:
                        trace.append({"step": index, "action": action, "ok": False, "error": str(exc)})
                        return SequenceResult(False, steps, trace, index, False, False, "", artifacts, str(exc))
            elif action == "assert_state":
                selectors = [dict(item or {}) for item in list(step.get("candidate_selectors", []) or []) if isinstance(item, dict)]
                description = str(step.get("description", "") or "")
                last_error = ""
                matched_selector: Dict[str, Any] | None = None
                live_state_before = _safe_capture_live_state(adapter, app_hint=app_hint)
                if not selectors:
                    trace.append({"step": index, "action": action, "ok": False, "optional": bool(step.get("optional", False)), "warning": "no candidate selectors", "live_state_before": live_state_before})
                    if not bool(step.get("optional", False)):
                        return SequenceResult(False, steps, trace, index, False, False, "", artifacts, str(step.get("recovery_hint", "") or "missing state selectors"))
                else:
                    for selector_index, selector in enumerate(selectors):
                        try:
                            adapter.assert_visible(selector, timeout_ms=int(step.get("timeout_ms", 2200) or 2200))
                            matched_selector = selector
                            live_state_after = _safe_capture_live_state(adapter, app_hint=app_hint)
                            trace.append(
                                {
                                    "step": index,
                                    "action": action,
                                    "ok": True,
                                    "phase": step.get("phase", ""),
                                    "source_index": step.get("source_index", -1),
                                    "segment_index": step.get("segment_index", -1),
                                    "checkpoint_id": step.get("checkpoint_id", ""),
                                    "checkpoint_name": step.get("checkpoint_name", ""),
                                    "selector_index": selector_index,
                                    "description": description,
                                    "selector": selector,
                                    "live_state_before": live_state_before,
                                    "live_state_after": live_state_after,
                                }
                            )
                            last_error = ""
                            break
                        except Exception as exc:
                            last_error = str(exc)
                    if last_error and matched_selector is None:
                        if bool(step.get("optional", False)):
                            trace.append(
                                {
                                    "step": index,
                                    "action": action,
                                    "ok": False,
                                    "optional": True,
                                    "phase": step.get("phase", ""),
                                    "source_index": step.get("source_index", -1),
                                    "segment_index": step.get("segment_index", -1),
                                    "checkpoint_id": step.get("checkpoint_id", ""),
                                    "checkpoint_name": step.get("checkpoint_name", ""),
                                    "warning": last_error,
                                    "description": description,
                                    "live_state_before": live_state_before,
                                }
                            )
                        else:
                            trace.append({"step": index, "action": action, "ok": False, "error": last_error, "description": description, "phase": step.get("phase", ""), "source_index": step.get("source_index", -1), "segment_index": step.get("segment_index", -1), "checkpoint_id": step.get("checkpoint_id", ""), "checkpoint_name": step.get("checkpoint_name", ""), "live_state_before": live_state_before})
                            return SequenceResult(
                                False,
                                steps,
                                trace,
                                index,
                                False,
                                False,
                                "",
                                artifacts,
                                str(step.get("recovery_hint", "") or last_error),
                            )
            elif action == "click":
                selectors = [dict(step.get("selector", {}) or {})]
                selectors.extend(dict(item or {}) for item in list(step.get("fallback_selectors", []) or []))
                last_error = ""
                for selector_index, selector in enumerate(selectors):
                    try:
                        adapter.click(selector)
                        trace.append(
                            {
                                "step": index,
                                "action": action,
                                "ok": True,
                                "selector_index": selector_index,
                                "selector": selector,
                                "expected_state": step.get("expected_state", ""),
                            }
                        )
                        last_error = ""
                        break
                    except Exception as exc:
                        last_error = str(exc)
                        trace.append(
                            {
                                "step": index,
                                "action": action,
                                "ok": False,
                                "selector_index": selector_index,
                                "selector": selector,
                                "warning": str(exc),
                            }
                        )
                if last_error:
                    return SequenceResult(
                        False,
                        steps,
                        trace,
                        index,
                        False,
                        False,
                        "",
                        artifacts,
                        str(step.get("recovery_hint", "") or last_error),
                    )
            elif action == "type_text":
                adapter.type({}, step.get("text", ""))
                trace.append({"step": index, "action": action, "ok": True, "expected_state": step.get("expected_state", "")})
            elif action == "hotkey":
                adapter.hotkey(step.get("keys", ""))
                trace.append({"step": index, "action": action, "ok": True, "expected_state": step.get("expected_state", "")})
            elif action == "wait":
                adapter.wait_for({"strategy": "noop", "value": ""}, timeout_ms=int(step.get("seconds", 1) * 1000))
                trace.append({"step": index, "action": action, "ok": True, "expected_state": step.get("expected_state", "")})
            elif action == "scroll":
                adapter.scroll(step.get("direction", "down"), int(step.get("amount", 1)))
                trace.append({"step": index, "action": action, "ok": True})
            elif action == "visual_search":
                found = adapter.visual_search(text=step.get("text", ""), timeout_ms=6000)
                if not found.get("ok"):
                    trace.append({"step": index, "action": action, "ok": False, "error": found.get("error", "")})
                    return SequenceResult(False, steps, trace, index, False, False, "", artifacts, found.get("error", "visual_search_failed"))
                last_visual_point = {"x": int(found.get("x", 0)), "y": int(found.get("y", 0))}
                trace.append({"step": index, "action": action, "ok": True, "found": found})
            elif action == "click_found":
                if not last_visual_point:
                    trace.append({"step": index, "action": action, "ok": False, "error": "no previous visual target"})
                    return SequenceResult(False, steps, trace, index, False, False, "", artifacts, "no previous visual target")
                adapter.click_at(last_visual_point["x"], last_visual_point["y"])
                trace.append({"step": index, "action": action, "ok": True, "point": last_visual_point})
            elif action == "use_credentials":
                resolved = vault.find_entry_by_service(step.get("service", ""))
                if not resolved.get("ok"):
                    trace.append({"step": index, "action": action, "ok": False, "error": resolved.get("error", "credential_not_found")})
                    return SequenceResult(False, steps, trace, index, False, False, "", artifacts, str(resolved.get("error", "credential_not_found")))
                entry = resolved["entry"]
                # Fill currently focused username field, tab to password, then fill password.
                adapter.type({}, entry.get("username", ""))
                adapter.hotkey("TAB")
                adapter.type({}, entry.get("password", ""))
                if step.get("submit", False):
                    adapter.hotkey("ENTER")
                vault.touch_used(str(entry.get("id", "")))
                trace.append(
                    {
                        "step": index,
                        "action": action,
                        "ok": True,
                        "service": entry.get("service", ""),
                        "username_masked": (entry.get("username", "")[:2] + "***") if entry.get("username") else "",
                        "submitted": bool(step.get("submit", False)),
                    }
                )
            elif action == "capture_clipboard_image":
                requested_output = str(step.get("output_path", "") or "").strip()
                if requested_output:
                    output_path = Path(requested_output)
                else:
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    output_path = Path("data/reports/desktop_sequence") / ts / "clipboard_capture.png"
                captured = capture_clipboard_image(output_path)
                if not captured:
                    trace.append({"step": index, "action": action, "ok": False, "error": "clipboard_capture_empty"})
                    return SequenceResult(False, steps, trace, index, False, False, "", artifacts, "clipboard_capture_empty")
                resolved = str(Path(captured).resolve())
                artifacts["clipboard_image_png"] = resolved
                artifacts["primary_open_file"] = resolved
                if requested_output:
                    artifacts["requested_output_path"] = str(output_path.resolve())
                trace.append({"step": index, "action": action, "ok": True, "artifact": resolved})
            else:
                trace.append({"step": index, "action": action, "ok": True, "note": step.get("text", "")})
        except Exception as exc:  # pylint: disable=broad-exception-caught
            trace.append({"step": index, "action": action, "ok": False, "error": str(exc)})
            return SequenceResult(False, steps, trace, index, False, False, "", artifacts, str(exc))

        index += 1
        if step_mode:
            return SequenceResult(True, steps, trace, index, False, False, "Step mode checkpoint.", artifacts)

    return SequenceResult(True, steps, trace, index, True, False, "", artifacts)


def build_plan_from_recipe(
    recipe: Dict[str, Any],
    *,
    input_bindings: Dict[str, Any] | None = None,
    current_state: Dict[str, Any] | None = None,
    is_family: bool = False,
    resume_from_source_index: int | None = None,
    resume_from_segment_index: int | None = None,
    resume_from_checkpoint_id: str = "",
) -> Dict[str, Any]:
    runtime = TeachReplayRuntime()
    preview = runtime.build_plan(
        family=recipe if is_family else None,
        recipe=None if is_family else recipe,
        input_bindings=input_bindings or {},
        current_state=current_state or {},
        resume_from_source_index=resume_from_source_index,
        resume_from_segment_index=resume_from_segment_index,
        resume_from_checkpoint_id=resume_from_checkpoint_id,
    )
    plan = {
        "instruction": str(preview.get("learned_goal", "") or recipe.get("learned_goal", "Learned taught workflow")),
        "app_name": str(preview.get("app_name", "") or recipe.get("app_name", "")),
        "steps": list(preview.get("steps", []) or []),
        "checkpoint_after_open": False,
        "learned_recipe_id": str(preview.get("recipe_id", "") or recipe.get("recipe_id", "")),
        "preconditions": list(preview.get("preconditions", []) or []),
        "success_signals": list(preview.get("success_signals", []) or []),
        "state_snapshots": list(preview.get("state_snapshots", []) or []),
        "state_checks": list(preview.get("state_checks", []) or []),
        "resume_from_source_index": resume_from_source_index,
        "resume_from_segment_index": resume_from_segment_index,
        "resume_from_checkpoint_id": resume_from_checkpoint_id,
        "can_autorun": bool(preview.get("can_autorun", False)),
        "pause_reason": str(preview.get("pause_reason", "") or ""),
        "missing_inputs": list(preview.get("missing_inputs", []) or []),
        "confidence": float(preview.get("confidence", 0.0) or 0.0),
    }
    return {"ok": True, "plan": plan, "preview": preview}


def _safe_capture_live_state(adapter: UIAAdapter, app_hint: str = "") -> Dict[str, Any]:
    try:
        state = adapter.capture_live_state(app_hint=app_hint, max_candidates=8)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return {"ok": False, "error": str(exc), "app_name": str(app_hint or "").strip().lower(), "visible_labels": [], "selector_values": []}
    if not isinstance(state, dict):
        return {"ok": False, "error": "invalid_live_state", "app_name": str(app_hint or "").strip().lower(), "visible_labels": [], "selector_values": []}
    return state
