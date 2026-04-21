from __future__ import annotations

import threading
from dataclasses import dataclass


@dataclass(slots=True)
class KillSwitchState:
    local_triggered: bool = False
    admin_triggered: bool = False

    @property
    def active(self) -> bool:
        return self.local_triggered or self.admin_triggered


class KillSwitch:
    def __init__(self) -> None:
        self._state = KillSwitchState()
        self._lock = threading.Lock()

    def trigger_local(self) -> None:
        with self._lock:
            self._state.local_triggered = True

    def trigger_admin(self) -> None:
        with self._lock:
            self._state.admin_triggered = True

    def reset(self) -> None:
        with self._lock:
            self._state = KillSwitchState()

    def is_active(self) -> bool:
        with self._lock:
            return self._state.active

