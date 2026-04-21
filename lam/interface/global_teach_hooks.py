from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from lam.interface.selector_picker import capture_selector_at_point
from lam.interface.teach_recorder import TeachRecorder


@dataclass(slots=True)
class GlobalTeachHooks:
    recorder: TeachRecorder
    active: bool = False
    mouse_listener: Any = None
    keyboard_listener: Any = None
    typed_buffer: str = ""
    lock: threading.Lock = field(default_factory=threading.Lock)
    last_click_ts: float = 0.0

    def start(self) -> Dict[str, Any]:
        try:
            from pynput import keyboard, mouse
        except Exception as exc:
            return {"ok": False, "error": f"pynput unavailable: {exc}"}

        if self.active:
            return {"ok": True, "message": "Global teach already active."}

        self.active = True

        def on_click(x: int, y: int, button: Any, pressed: bool) -> None:
            if not self.active or not pressed:
                return
            now = time.time()
            if now - self.last_click_ts < 0.2:
                return
            self.last_click_ts = now
            cap = capture_selector_at_point(int(x), int(y))
            selector = cap.selector if cap.ok else {"strategy": "point", "value": f"x={x};y={y}"}
            self._flush_buffer()
            self.recorder.capture_click(selector or {"strategy": "point", "value": f"x={x};y={y}"})

        def on_press(key: Any) -> None:
            if not self.active:
                return
            try:
                char = key.char  # type: ignore[attr-defined]
            except Exception:
                char = None
            if char and len(char) == 1 and char.isprintable():
                with self.lock:
                    self.typed_buffer += char
                return

            special = str(key).lower()
            if special in {"key.enter", "key.tab", "key.esc", "key.space", "key.backspace", "key.delete"}:
                self._flush_buffer()
                key_name = special.replace("key.", "")
                self.recorder.capture_hotkey(key_name)

        self.mouse_listener = mouse.Listener(on_click=on_click)
        self.keyboard_listener = keyboard.Listener(on_press=on_press)
        self.mouse_listener.start()
        self.keyboard_listener.start()
        return {"ok": True, "active": self.active}

    def stop(self) -> Dict[str, Any]:
        self._flush_buffer()
        self.active = False
        if self.mouse_listener:
            try:
                self.mouse_listener.stop()
            except Exception:
                pass
            self.mouse_listener = None
        if self.keyboard_listener:
            try:
                self.keyboard_listener.stop()
            except Exception:
                pass
            self.keyboard_listener = None
        return {"ok": True, "active": self.active}

    def _flush_buffer(self) -> None:
        with self.lock:
            text = self.typed_buffer
            self.typed_buffer = ""
        if text.strip():
            self.recorder.capture_type(text)

