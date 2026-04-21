from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Protocol


class ApprovalServiceProtocol(Protocol):
    def create_request(self, step: Dict[str, Any], approver_levels: List[str], context: Dict[str, Any]) -> str: ...

    def get_status(self, request_id: str) -> str: ...


@dataclass(slots=True)
class ApprovalResult:
    request_id: str
    status: str


class ApprovalClient:
    def __init__(self, service: ApprovalServiceProtocol, poll_interval_seconds: float = 2.0) -> None:
        self.service = service
        self.poll_interval_seconds = poll_interval_seconds

    def wait_for_approval(
        self,
        step: Dict[str, Any],
        approver_levels: List[str],
        timeout_seconds: int = 600,
        context: Dict[str, Any] | None = None,
    ) -> ApprovalResult:
        context = context or {}
        request_id = self.service.create_request(step=step, approver_levels=approver_levels, context=context)
        start = time.time()
        while True:
            status = self.service.get_status(request_id)
            if status == "approved":
                return ApprovalResult(request_id=request_id, status=status)
            if status == "denied":
                raise PermissionError(f"Approval denied for request {request_id}")
            if status == "expired":
                raise TimeoutError(f"Approval expired for request {request_id}")
            if time.time() - start > timeout_seconds:
                raise TimeoutError(f"Approval timed out for request {request_id}")
            time.sleep(self.poll_interval_seconds)

