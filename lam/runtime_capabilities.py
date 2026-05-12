from __future__ import annotations

import importlib.util
from typing import Any, Dict, List


CORE_CAPABILITIES = [
    {"name": "PyYAML", "module": "yaml", "install": "pip install pyyaml"},
    {"name": "jsonschema", "module": "jsonschema", "install": "pip install jsonschema"},
    {"name": "playwright", "module": "playwright", "install": "pip install playwright"},
    {"name": "pypdf", "module": "pypdf", "install": "pip install pypdf"},
    {"name": "PyMuPDF", "module": "fitz", "install": "pip install pymupdf"},
    {"name": "python-pptx", "module": "pptx", "install": "pip install python-pptx"},
]


OPTIONAL_CAPABILITY_GROUPS = {
    "desktop_automation": [
        {"name": "pywinauto", "module": "pywinauto", "install": "pip install pywinauto"},
        {"name": "pyautogui", "module": "pyautogui", "install": "pip install pyautogui"},
        {"name": "pytesseract", "module": "pytesseract", "install": "pip install pytesseract"},
        {"name": "Pillow", "module": "PIL", "install": "pip install pillow"},
    ],
    "spreadsheet_automation": [
        {"name": "openpyxl", "module": "openpyxl", "install": "pip install openpyxl"},
    ],
    "selenium_browser": [
        {"name": "selenium", "module": "selenium", "install": "pip install selenium"},
    ],
}


def _check_entries(entries: List[Dict[str, str]]) -> Dict[str, Any]:
    checks: List[Dict[str, Any]] = []
    missing: List[Dict[str, str]] = []
    for item in entries:
        module = str(item.get("module", ""))
        ok = bool(importlib.util.find_spec(module))
        checks.append(
            {
                "name": str(item.get("name", module)),
                "module": module,
                "ok": ok,
                "install": str(item.get("install", "")),
            }
        )
        if not ok:
            missing.append(
                {
                    "name": str(item.get("name", module)),
                    "module": module,
                    "install": str(item.get("install", "")),
                }
            )
    return {"ready": len(missing) == 0, "checks": checks, "missing": missing}


def check_runtime_capabilities() -> Dict[str, Any]:
    core = _check_entries(CORE_CAPABILITIES)
    optional: Dict[str, Any] = {}
    for group, entries in OPTIONAL_CAPABILITY_GROUPS.items():
        optional[group] = _check_entries(entries)
    return {"core": core, "optional": optional}


def format_capability_report(report: Dict[str, Any]) -> str:
    lines: List[str] = []
    core = dict(report.get("core", {}) or {})
    lines.append(f"Core capabilities: {'ready' if core.get('ready') else 'missing deps'}")
    for item in list(core.get("checks", []) or []):
        marker = "ok" if bool(item.get("ok", False)) else "missing"
        install = str(item.get("install", ""))
        suffix = "" if marker == "ok" else f" | install: {install}"
        lines.append(f"- {marker}: {item.get('name')} ({item.get('module')}){suffix}")
    optional = dict(report.get("optional", {}) or {})
    for group, payload in optional.items():
        ready = bool((payload or {}).get("ready", False))
        lines.append(f"{group}: {'ready' if ready else 'partial'}")
        for item in list((payload or {}).get("checks", []) or []):
            marker = "ok" if bool(item.get("ok", False)) else "missing"
            install = str(item.get("install", ""))
            suffix = "" if marker == "ok" else f" | install: {install}"
            lines.append(f"- {marker}: {item.get('name')} ({item.get('module')}){suffix}")
    return "\n".join(lines)

