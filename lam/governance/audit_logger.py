from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Protocol

from lam.governance.redaction import Redactor


class AuditSink(Protocol):
    def append(self, event: Dict[str, Any]) -> None: ...

    def latest_hash(self) -> str: ...

    def iter_events(self) -> Iterable[Dict[str, Any]]: ...


class JsonlAuditSink:
    """Local append-only JSONL sink. Immutable backend is provided by storage policy."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.path.write_text("", encoding="utf-8")

    def append(self, event: Dict[str, Any]) -> None:
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, sort_keys=True) + "\n")

    def latest_hash(self) -> str:
        last_hash = "GENESIS"
        with self.path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                evt = json.loads(line)
                last_hash = evt.get("event_hash", last_hash)
        return last_hash

    def iter_events(self) -> Iterable[Dict[str, Any]]:
        with self.path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    yield json.loads(line)


@dataclass(slots=True)
class AppendResult:
    event_id: str
    event_hash: str
    prev_hash: str


class AuditLogger:
    """Tamper-evident append-only logger with enforced redaction metadata."""

    def __init__(
        self,
        sink: AuditSink,
        redactor: Optional[Redactor] = None,
        min_redaction_confidence: float = 0.95,
        signing_key: Optional[str] = None,
    ) -> None:
        self.sink = sink
        self.redactor = redactor or Redactor()
        self.min_redaction_confidence = min_redaction_confidence
        self.prev_hash = self.sink.latest_hash()
        self.signing_key = signing_key or os.getenv("LAM_AUDIT_SIGNING_KEY", "")

    def append_event(
        self,
        event_type: str,
        payload: Dict[str, Any],
        actor_id: str = "",
        workflow_id: str = "",
        workflow_version: str = "",
        step_id: str = "",
        outcome: str = "",
    ) -> AppendResult:
        safe_payload, redaction_meta = self.redactor.redact_for_persistence(payload)
        confidence = float(redaction_meta.get("confidence", 0.0))
        if confidence < self.min_redaction_confidence:
            raise ValueError(f"Redaction confidence {confidence} below threshold {self.min_redaction_confidence}")

        event = {
            "event_id": str(uuid.uuid4()),
            "ts": time.time(),
            "type": event_type,
            "actor_id": actor_id,
            "workflow_id": workflow_id,
            "version": workflow_version,
            "step_id": step_id,
            "outcome": outcome,
            "payload": safe_payload,
            "redaction_meta": redaction_meta,
            "prev_hash": self.prev_hash,
        }
        canonical = json.dumps(event, sort_keys=True, separators=(",", ":")).encode("utf-8")
        event_hash = hashlib.sha256(canonical).hexdigest()
        event["event_hash"] = event_hash
        event["signature"] = self._sign(event_hash)

        self.sink.append(event)
        previous = self.prev_hash
        self.prev_hash = event_hash
        return AppendResult(event_id=event["event_id"], event_hash=event_hash, prev_hash=previous)

    def validate_chain(self) -> List[str]:
        errors: List[str] = []
        prev_hash = "GENESIS"
        for event in self.sink.iter_events():
            expected_prev = event.get("prev_hash")
            if expected_prev != prev_hash:
                errors.append(f"prev_hash_mismatch:event_id={event.get('event_id')}")
            current_hash = event.get("event_hash", "")
            rebuild = dict(event)
            rebuild.pop("event_hash", None)
            rebuild.pop("signature", None)
            canonical = json.dumps(rebuild, sort_keys=True, separators=(",", ":")).encode("utf-8")
            computed = hashlib.sha256(canonical).hexdigest()
            if current_hash != computed:
                errors.append(f"event_hash_mismatch:event_id={event.get('event_id')}")
            prev_hash = current_hash
        return errors

    def _sign(self, event_hash: str) -> str:
        if not self.signing_key:
            return ""
        return hmac.new(self.signing_key.encode("utf-8"), event_hash.encode("utf-8"), hashlib.sha256).hexdigest()
