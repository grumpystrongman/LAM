from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass(slots=True)
class ApprovalRequest:
    request_id: str
    step: Dict[str, Any]
    approver_levels: List[str]
    context: Dict[str, Any]
    status: str = "pending"
    created_ts: float = field(default_factory=time.time)
    reason: str = ""


class InMemoryApprovalService:
    """
    In-memory approval service for MVP and tests.
    Replace with durable on-prem service in production.
    """

    def __init__(self, auto_approve: bool = False) -> None:
        self._lock = threading.Lock()
        self._requests: Dict[str, ApprovalRequest] = {}
        self.auto_approve = auto_approve

    def create_request(self, step: Dict[str, Any], approver_levels: List[str], context: Dict[str, Any]) -> str:
        request_id = str(uuid.uuid4())
        request = ApprovalRequest(
            request_id=request_id,
            step=step,
            approver_levels=list(approver_levels),
            context=dict(context),
        )
        if self.auto_approve:
            request.status = "approved"
        with self._lock:
            self._requests[request_id] = request
        return request_id

    def get_status(self, request_id: str) -> str:
        with self._lock:
            request = self._requests.get(request_id)
            if request is None:
                return "expired"
            return request.status

    def approve(self, request_id: str) -> None:
        with self._lock:
            if request_id in self._requests:
                self._requests[request_id].status = "approved"

    def deny(self, request_id: str, reason: str = "") -> None:
        with self._lock:
            if request_id in self._requests:
                self._requests[request_id].status = "denied"
                self._requests[request_id].reason = reason

