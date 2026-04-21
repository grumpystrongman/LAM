from __future__ import annotations

import threading


class PauseController:
    def __init__(self) -> None:
        self._event = threading.Event()
        self._event.set()

    def pause(self) -> None:
        self._event.clear()

    def resume(self) -> None:
        self._event.set()

    def wait_if_paused(self) -> None:
        self._event.wait()

