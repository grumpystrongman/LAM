from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass(slots=True)
class SeleniumWaiver:
    workflow_id: str
    reason: str
    approved_by: str


class SeleniumAdapter:
    """
    Legacy fallback only.
    Use only with explicit compatibility waiver.
    """

    def __init__(self, waiver: SeleniumWaiver | None = None, dry_run: bool = True) -> None:
        self.waiver = waiver
        self.dry_run = dry_run
        self._trace: List[Dict[str, Any]] = []

    def require_waiver(self) -> None:
        if self.waiver is None:
            raise PermissionError("Selenium fallback requires explicit compatibility waiver.")

    def click(self, selector: Dict[str, Any]) -> None:
        self.require_waiver()
        self._trace.append({"action": "click", "selector": selector})

    def type(self, selector: Dict[str, Any], text: str) -> None:
        self.require_waiver()
        self._trace.append({"action": "type", "selector": selector, "text": text})

    def trace(self) -> List[Dict[str, Any]]:
        return list(self._trace)

