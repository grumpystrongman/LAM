from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import yaml


def load_workflow(path: str | Path) -> Dict[str, Any]:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)
    raw = path.read_text(encoding="utf-8")
    if path.suffix.lower() in {".json"}:
        return json.loads(raw)
    loaded = yaml.safe_load(raw)
    if isinstance(loaded, dict) and "workflow" in loaded:
        return loaded["workflow"]
    return loaded or {}


def resolve_value_ref(value_ref: str, runtime_state: Dict[str, Any]) -> Any:
    if not value_ref:
        return None
    if "." not in value_ref:
        return runtime_state.get(value_ref)
    left, right = value_ref.split(".", 1)
    if left == "row":
        return runtime_state.get("row", {}).get(right)
    return runtime_state.get(left, {}).get(right)

