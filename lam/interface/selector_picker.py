from __future__ import annotations

import ctypes
from dataclasses import dataclass, asdict
from typing import Any, Dict


@dataclass(slots=True)
class SelectorCapture:
    ok: bool
    x: int = 0
    y: int = 0
    selector: Dict[str, Any] | None = None
    error: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def capture_selector_at_cursor() -> SelectorCapture:
    """Capture a desktop element selector at current mouse position."""
    try:
        from pywinauto import Desktop
    except Exception as exc:
        return SelectorCapture(ok=False, error=f"pywinauto unavailable: {exc}")

    pt = _get_cursor_pos()
    if pt is None:
        return SelectorCapture(ok=False, error="Could not read cursor position.")

    x, y = pt
    return capture_selector_at_point(x, y)


def capture_selector_at_point(x: int, y: int) -> SelectorCapture:
    """Capture selector for element at specific screen coordinates."""
    try:
        desktop = Desktop(backend="uia")
        elem = desktop.from_point(x, y)
        info = elem.element_info
        selector = {
            "strategy": "uia",
            "value": _format_uia_selector(
                name=getattr(info, "name", "") or "",
                automation_id=getattr(info, "automation_id", "") or "",
                control_type=getattr(info, "control_type", "") or "",
                class_name=getattr(info, "class_name", "") or "",
            ),
            "metadata": {
                "name": getattr(info, "name", "") or "",
                "automation_id": getattr(info, "automation_id", "") or "",
                "control_type": getattr(info, "control_type", "") or "",
                "class_name": getattr(info, "class_name", "") or "",
            },
        }
        return SelectorCapture(ok=True, x=x, y=y, selector=selector)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return SelectorCapture(ok=False, x=x, y=y, error=str(exc))


def _format_uia_selector(name: str, automation_id: str, control_type: str, class_name: str) -> str:
    parts = []
    if name:
        parts.append(f"Name={name}")
    if automation_id:
        parts.append(f"AutomationId={automation_id}")
    if control_type:
        parts.append(f"ControlType={control_type}")
    if class_name:
        parts.append(f"ClassName={class_name}")
    return ";".join(parts)


def _get_cursor_pos() -> tuple[int, int] | None:
    class POINT(ctypes.Structure):
        _fields_ = [("x", ctypes.c_long), ("y", ctypes.c_long)]

    point = POINT()
    ok = ctypes.windll.user32.GetCursorPos(ctypes.byref(point))  # type: ignore[attr-defined]
    if ok:
        return int(point.x), int(point.y)
    return None
