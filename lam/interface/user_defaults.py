from __future__ import annotations

import getpass
import json
from pathlib import Path
from typing import Any, Dict


def _defaults_path() -> Path:
    path = Path("data/interface/user_defaults.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def current_user() -> str:
    try:
        return getpass.getuser().strip().lower() or "default"
    except Exception:
        return "default"


def load_defaults(user: str | None = None) -> Dict[str, Any]:
    user_key = (user or current_user()).strip().lower()
    path = _defaults_path()
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(obj, dict):
        return {}
    value = obj.get(user_key, {})
    return value if isinstance(value, dict) else {}


def save_defaults(values: Dict[str, Any], user: str | None = None) -> None:
    user_key = (user or current_user()).strip().lower()
    path = _defaults_path()
    if path.exists():
        try:
            obj = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            obj = {}
    else:
        obj = {}
    if not isinstance(obj, dict):
        obj = {}
    obj[user_key] = values
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")

