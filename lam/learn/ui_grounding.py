from __future__ import annotations

import re
from typing import Any, Dict, List


def infer_app_context(topic: str, required_tools: List[str], selected_sources: List[Dict[str, object]]) -> Dict[str, object]:
    low = topic.lower()
    tools_low = " ".join(x.lower() for x in required_tools)
    app_name = "browser"
    app_family = "web"
    if "power bi" in low or "power bi" in tools_low:
        app_name = "power bi desktop"
        app_family = "desktop_analytics"
    elif "react" in low:
        app_name = "code editor + browser"
        app_family = "frontend_build"
    elif "grant" in low or "budget narrative" in low:
        app_name = "document editor"
        app_family = "document_authoring"
    elif "github" in " ".join(str(item.get("platform", "")) for item in selected_sources).lower():
        app_name = "github + editor"
        app_family = "development"
    return {
        "app_name": app_name,
        "app_family": app_family,
        "window_hints": _window_hints(topic, required_tools),
        "primary_controls": _primary_controls(topic),
        "control_patterns": _control_patterns(app_family),
        "workflow_surface": _workflow_surface(app_family),
    }


def build_step_grounding(step: Dict[str, object], app_context: Dict[str, object], observations: List[Dict[str, str]] | None = None) -> Dict[str, object]:
    description = str(step.get("description", "") or "")
    target = str(step.get("target", "") or "")
    action = str(step.get("action_type", "workflow") or "workflow")
    selector_suggestions = generate_selector_suggestions(description=description, target=target, app_context=app_context, observations=observations or [])
    expected_state = infer_expected_state(description=description, target=target, app_context=app_context)
    return {
        "app_name": str(app_context.get("app_name", "")),
        "app_family": str(app_context.get("app_family", "")),
        "window_hints": list(app_context.get("window_hints", []) or []),
        "control_hints": selector_suggestions[:4],
        "action_hint": action,
        "expected_state": expected_state,
        "execution_target": build_execution_target(action=action, selector_suggestions=selector_suggestions, expected_state=expected_state),
        "grounding_confidence": round(min(0.95, 0.45 + (0.07 * min(4, len(selector_suggestions))) + (0.08 if expected_state.get("labels") else 0.0)), 3),
    }


def generate_selector_suggestions(*, description: str, target: str, app_context: Dict[str, object], observations: List[Dict[str, str]]) -> List[Dict[str, str]]:
    raw = f"{description} {target} {' '.join(str(item.get('ui_elements', '')) for item in observations)} {' '.join(str(x) for x in app_context.get('primary_controls', []) or [])}"
    tokens = [token for token in re.findall(r"[A-Za-z][A-Za-z0-9_./ -]{2,}", raw) if len(token.strip()) >= 3]
    seen = set()
    suggestions: List[Dict[str, str]] = []
    roles = list(app_context.get("control_patterns", {}).get("roles", []) or [])
    for token in tokens:
        label = token.strip()[:80]
        low = label.lower()
        if low in seen:
            continue
        seen.add(low)
        suggestions.append({"kind": "label", "value": label})
        suggestions.append({"kind": "automation_id", "value": re.sub(r"[^a-z0-9]+", "_", low).strip("_")[:60]})
        if roles:
            suggestions.append({"kind": "role", "value": roles[min(len(roles) - 1, len(seen) - 1)]})
        if len(suggestions) >= 10:
            break
    if not suggestions:
        suggestions.append({"kind": "window_hint", "value": str((app_context.get("window_hints", []) or ["application"])[0])})
    return suggestions[:10]


def infer_expected_state(*, description: str, target: str, app_context: Dict[str, object]) -> Dict[str, Any]:
    low = f"{description} {target}".lower()
    labels = []
    if any(item in low for item in ["validate", "review", "preview", "result", "output"]):
        labels.extend(["preview", "result", "output", "validation"])
    if any(item in low for item in ["save", "publish", "share"]):
        labels.extend(["save", "publish", "share"])
    if any(item in low for item in ["filter", "configure", "select"]):
        labels.extend(["filter", "settings", "options"])
    if not labels:
        labels = list(app_context.get("primary_controls", []) or [])[:2]
    return {
        "window_hints": list(app_context.get("window_hints", []) or [])[:2],
        "labels": _dedupe(labels)[:4],
        "roles": list(app_context.get("control_patterns", {}).get("roles", []) or [])[:3],
    }


def build_execution_target(*, action: str, selector_suggestions: List[Dict[str, str]], expected_state: Dict[str, Any]) -> Dict[str, Any]:
    normalized_action = action.lower().strip()
    if normalized_action in {"open", "launch"}:
        return {"action": "open_app", "safe": True}
    if normalized_action in {"click", "select", "filter", "configure", "save"} and selector_suggestions:
        return {"action": "click", "safe": normalized_action != "save", "primary_selector": selector_suggestions[0], "fallback_selectors": selector_suggestions[1:4]}
    if normalized_action in {"type", "enter"}:
        return {"action": "type_text", "safe": False, "requires_human_value": True}
    if normalized_action in {"validate", "review"}:
        return {"action": "assert_state", "safe": True, "expected_state": expected_state}
    return {"action": "note", "safe": True}


def _window_hints(topic: str, required_tools: List[str]) -> List[str]:
    joined = " ".join(required_tools)
    return [topic[:80], joined[:80], "tutorial workspace"]


def _primary_controls(topic: str) -> List[str]:
    low = topic.lower()
    if "power bi" in low:
        return ["report canvas", "fields pane", "visualizations pane", "kpi card", "filter pane"]
    if "grant" in low:
        return ["section heading", "narrative editor", "budget table", "save action"]
    if "react" in low:
        return ["editor tab", "terminal", "browser preview", "component tree"]
    return ["search box", "editor", "save action", "validation area"]


def _control_patterns(app_family: str) -> Dict[str, Any]:
    if app_family == "desktop_analytics":
        return {"roles": ["Pane", "Button", "ListItem", "Edit"], "preferred_strategies": ["automation_id", "label", "role"]}
    if app_family == "frontend_build":
        return {"roles": ["TabItem", "Edit", "Button", "Document"], "preferred_strategies": ["label", "automation_id", "role"]}
    if app_family == "document_authoring":
        return {"roles": ["Document", "Edit", "Button"], "preferred_strategies": ["label", "role"]}
    if app_family == "development":
        return {"roles": ["TreeItem", "Edit", "Button", "Link"], "preferred_strategies": ["label", "automation_id", "role"]}
    return {"roles": ["Button", "Edit", "Document"], "preferred_strategies": ["label", "role"]}


def _workflow_surface(app_family: str) -> Dict[str, Any]:
    return {
        "family": app_family,
        "supports_safe_open": True,
        "supports_selector_preview": True,
        "supports_checkpoint_practice": True,
    }


def _dedupe(items: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in items:
        low = str(item or "").strip().lower()
        if not low or low in seen:
            continue
        seen.add(low)
        out.append(str(item))
    return out
