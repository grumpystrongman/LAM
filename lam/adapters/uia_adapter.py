from __future__ import annotations

import re
import time
from typing import Any, Dict, List, Optional


class UIAAdapter:
    """
    UI Automation adapter.
    Keyboard/mouse fallback is disabled unless explicitly permitted.
    """

    def __init__(self, allow_input_fallback: bool = False, dry_run: bool = True) -> None:
        self.allow_input_fallback = allow_input_fallback
        self.dry_run = dry_run
        self._trace: List[Dict[str, Any]] = []
        self._desktop = None
        self._keyboard = None
        if not self.dry_run:
            try:
                from pywinauto import Desktop
                from pywinauto.keyboard import send_keys
                from pywinauto.mouse import click as mouse_click
                from pywinauto.mouse import scroll as mouse_scroll

                self._desktop = Desktop(backend="uia")
                self._keyboard = send_keys
                self._mouse_click = mouse_click
                self._mouse_scroll = mouse_scroll
            except Exception:
                self._desktop = None
                self._keyboard = None
                self._mouse_click = None
                self._mouse_scroll = None

    def open_app(self, path: str) -> None:
        self._trace.append({"action": "open_app", "path": path})

    def focus_window(self, selector_bundle: Dict[str, Any]) -> None:
        self._trace.append({"action": "focus_window", "selector": selector_bundle})
        if self.dry_run:
            return
        window = self._find_window(selector_bundle)
        window.set_focus()

    def click(self, selector_bundle: Dict[str, Any]) -> None:
        self._trace.append({"action": "click", "selector": selector_bundle})
        if self.dry_run:
            return
        element = self._find_element(selector_bundle)
        if hasattr(element, "click_input"):
            element.click_input()
        else:
            element.click()

    def type(self, selector_bundle: Dict[str, Any], text: str) -> None:
        self._trace.append({"action": "type", "selector": selector_bundle, "text": text})
        if self.dry_run:
            return
        if selector_bundle:
            element = self._find_element(selector_bundle)
            try:
                element.set_focus()
                element.type_keys(text, with_spaces=True, set_foreground=True)
                return
            except Exception:
                pass
        if self._keyboard:
            self._keyboard(text, with_spaces=True)
        else:
            raise RuntimeError("UIA keyboard backend unavailable. Install pywinauto.")

    def hotkey(self, keys: str) -> None:
        self._trace.append({"action": "hotkey", "keys": keys})
        if self.dry_run:
            return
        if not self._keyboard:
            raise RuntimeError("UIA keyboard backend unavailable. Install pywinauto.")
        translated = self._translate_hotkey(keys)
        self._keyboard(translated)

    def wait_for(self, selector_bundle: Dict[str, Any], timeout_ms: Optional[int] = None) -> None:
        self._trace.append({"action": "wait_for", "selector": selector_bundle, "timeout_ms": timeout_ms})
        if self.dry_run:
            return
        if selector_bundle.get("strategy") == "noop":
            time.sleep(float((timeout_ms or 1000) / 1000.0))
            return
        deadline = time.time() + float((timeout_ms or 10000) / 1000.0)
        while time.time() < deadline:
            try:
                _ = self._find_element(selector_bundle)
                return
            except Exception:
                time.sleep(0.2)
        raise TimeoutError(f"Element not found within timeout: {selector_bundle}")

    def assert_visible(self, selector_bundle: Dict[str, Any], timeout_ms: Optional[int] = None) -> None:
        self._trace.append({"action": "assert_visible", "selector": selector_bundle, "timeout_ms": timeout_ms})
        if self.dry_run:
            return
        self.wait_for(selector_bundle, timeout_ms=timeout_ms)

    def extract_field(self, selector_bundle: Dict[str, Any]) -> str:
        self._trace.append({"action": "extract_field", "selector": selector_bundle})
        if self.dry_run:
            return "stub_value"
        element = self._find_element(selector_bundle)
        for attr in ("window_text", "texts"):
            if hasattr(element, attr):
                try:
                    value = getattr(element, attr)()
                    if isinstance(value, list):
                        return " ".join(str(v) for v in value)
                    return str(value)
                except Exception:
                    continue
        return "stub_value"

    def screenshot_redacted(self, reason: str) -> None:
        self._trace.append({"action": "screenshot_redacted", "reason": reason})

    def generic_action(self, action: str, target: Dict[str, Any], data: Dict[str, Any]) -> None:
        if action in {"copy", "paste"} and not self.allow_input_fallback:
            self._trace.append({"action": action, "blocked": "input_fallback_disabled"})
            raise PermissionError("Input simulation is disabled by policy for this adapter.")
        self._trace.append({"action": action, "target": target, "data": data})
        if self.dry_run:
            return
        if action == "copy":
            self.hotkey("ctrl+c")
        elif action == "paste":
            self.hotkey("ctrl+v")

    def trace(self) -> List[Dict[str, Any]]:
        return list(self._trace)

    def click_at(self, x: int, y: int) -> None:
        self._trace.append({"action": "click_at", "x": x, "y": y})
        if self.dry_run:
            return
        if not self._mouse_click:
            raise RuntimeError("Mouse backend unavailable.")
        self._mouse_click(coords=(int(x), int(y)))

    def scroll(self, direction: str = "down", amount: int = 1) -> None:
        self._trace.append({"action": "scroll", "direction": direction, "amount": amount})
        if self.dry_run:
            return
        if not self._mouse_scroll:
            raise RuntimeError("Mouse backend unavailable.")
        wheel_dist = -int(amount) if direction.lower() == "down" else int(amount)
        self._mouse_scroll(wheel_dist=wheel_dist)

    def visual_search(
        self,
        text: str = "",
        image_path: str = "",
        confidence: float = 0.8,
        timeout_ms: int = 5000,
    ) -> Dict[str, Any]:
        self._trace.append(
            {"action": "visual_search", "text": text, "image_path": image_path, "confidence": confidence, "timeout_ms": timeout_ms}
        )
        if self.dry_run:
            return {"ok": True, "x": 0, "y": 0, "method": "dry_run"}

        deadline = time.time() + float(timeout_ms / 1000.0)
        while time.time() < deadline:
            if image_path:
                found = self._find_image_on_screen(image_path, confidence)
                if found.get("ok"):
                    return found
            if text:
                found = self._find_text_on_screen(text)
                if found.get("ok"):
                    return found
            time.sleep(0.25)
        return {"ok": False, "error": "Visual target not found in time."}

    def _find_window(self, selector_bundle: Dict[str, Any]):
        if self._desktop is None:
            raise RuntimeError("UIA backend unavailable. Install pywinauto.")
        strategy = selector_bundle.get("strategy", "text")
        value = selector_bundle.get("value", "")
        if strategy in {"text", "name"}:
            return self._desktop.window(title_re=f".*{re.escape(value)}.*")
        if strategy == "uia":
            if "Name=" in value:
                name = value.split("Name=", 1)[1].split(";", 1)[0].strip()
                return self._desktop.window(title_re=f".*{re.escape(name)}.*")
        return self._desktop.window(title_re=f".*{re.escape(value)}.*")

    def _find_element(self, selector_bundle: Dict[str, Any]):
        window = self._find_window(selector_bundle)
        strategy = selector_bundle.get("strategy", "text")
        value = selector_bundle.get("value", "")
        if strategy in {"text", "name"}:
            return window.child_window(title_re=f".*{re.escape(value)}.*")
        if strategy == "uia":
            auto_id = ""
            name = ""
            if "AutomationId=" in value:
                auto_id = value.split("AutomationId=", 1)[1].split(";", 1)[0].strip()
            if "Name=" in value:
                name = value.split("Name=", 1)[1].split(";", 1)[0].strip()
            if auto_id:
                return window.child_window(auto_id=auto_id)
            if name:
                return window.child_window(title_re=f".*{re.escape(name)}.*")
        return window.child_window(title_re=f".*{re.escape(value)}.*")

    @staticmethod
    def _translate_hotkey(keys: str) -> str:
        normalized = keys.strip().lower().replace(" ", "")
        mapping = {
            "ctrl": "^",
            "control": "^",
            "alt": "%",
            "shift": "+",
            "enter": "{ENTER}",
            "tab": "{TAB}",
            "esc": "{ESC}",
            "escape": "{ESC}",
            "backspace": "{BACKSPACE}",
            "delete": "{DELETE}",
        }
        parts = [p for p in normalized.split("+") if p]
        out = ""
        for i, part in enumerate(parts):
            if part in {"ctrl", "control", "alt", "shift"}:
                out += mapping[part]
            else:
                out += mapping.get(part, part)
        return out

    @staticmethod
    def _find_image_on_screen(image_path: str, confidence: float) -> Dict[str, Any]:
        try:
            import pyautogui
        except Exception as exc:
            return {"ok": False, "error": f"pyautogui unavailable: {exc}"}
        try:
            loc = pyautogui.locateCenterOnScreen(image_path, confidence=float(confidence))
            if loc:
                return {"ok": True, "x": int(loc.x), "y": int(loc.y), "method": "image"}
        except Exception as exc:  # pylint: disable=broad-exception-caught
            return {"ok": False, "error": str(exc)}
        return {"ok": False, "error": "image_not_found"}

    @staticmethod
    def _find_text_on_screen(text: str) -> Dict[str, Any]:
        try:
            import pytesseract
            from PIL import ImageGrab
        except Exception as exc:
            return {"ok": False, "error": f"OCR deps unavailable: {exc}"}
        try:
            img = ImageGrab.grab(all_screens=True)
            data = pytesseract.image_to_data(img, output_type=pytesseract.Output.DICT)
            target = text.lower().strip()
            for i, chunk in enumerate(data.get("text", [])):
                if not chunk:
                    continue
                if target in str(chunk).lower():
                    x = int(data["left"][i] + data["width"][i] / 2)
                    y = int(data["top"][i] + data["height"][i] / 2)
                    return {"ok": True, "x": x, "y": y, "method": "ocr"}
        except Exception as exc:  # pylint: disable=broad-exception-caught
            return {"ok": False, "error": str(exc)}
        return {"ok": False, "error": "text_not_found"}
