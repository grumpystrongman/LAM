from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, List


@dataclass(slots=True)
class CapturedAction:
    ts: float
    action_type: str
    target: Dict[str, Any]
    data: Dict[str, Any]
    confidence: float
    sensitive: bool


class TeachCapture:
    """
    Teach mode capture.
    Password/credential fields are intentionally stripped to avoid secret capture.
    """

    def __init__(self) -> None:
        self._actions: List[CapturedAction] = []
        self._recording = False

    def start(self) -> None:
        self._recording = True

    def stop(self) -> None:
        self._recording = False

    def record_action(
        self,
        action_type: str,
        target: Dict[str, Any],
        data: Dict[str, Any],
        confidence: float = 1.0,
        sensitive: bool = False,
    ) -> None:
        if not self._recording:
            return

        safe_data = dict(data)
        if target.get("is_credential_field") or data.get("credential", False):
            safe_data = {"suppressed": True}

        self._actions.append(
            CapturedAction(
                ts=time.time(),
                action_type=action_type,
                target=dict(target),
                data=safe_data,
                confidence=confidence,
                sensitive=sensitive,
            )
        )

    def export(self) -> List[Dict[str, Any]]:
        return [asdict(item) for item in self._actions]

