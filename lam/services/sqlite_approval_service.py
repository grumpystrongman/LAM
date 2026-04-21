from __future__ import annotations

import json
import sqlite3
import time
import uuid
from contextlib import closing
from pathlib import Path
from typing import Any, Dict, List


class SqliteApprovalService:
    """Durable approval service with level-based dual approval support."""

    def __init__(self, path: str | Path = "data/approvals/approvals.db") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with closing(self._connect()) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS approvals (
                    request_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    created_ts REAL NOT NULL,
                    updated_ts REAL NOT NULL,
                    step_json TEXT NOT NULL,
                    context_json TEXT NOT NULL,
                    required_levels_json TEXT NOT NULL,
                    satisfied_levels_json TEXT NOT NULL,
                    reason TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS approval_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    request_id TEXT NOT NULL,
                    approver_id TEXT NOT NULL,
                    approver_level TEXT NOT NULL,
                    decision TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    ts REAL NOT NULL
                )
                """
            )
            conn.commit()

    def create_request(self, step: Dict[str, Any], approver_levels: List[str], context: Dict[str, Any]) -> str:
        request_id = str(uuid.uuid4())
        now = time.time()
        required = sorted(set(approver_levels))
        with closing(self._connect()) as conn:
            conn.execute(
                """
                INSERT INTO approvals(
                    request_id, status, created_ts, updated_ts,
                    step_json, context_json, required_levels_json, satisfied_levels_json, reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request_id,
                    "pending",
                    now,
                    now,
                    json.dumps(step, sort_keys=True),
                    json.dumps(context, sort_keys=True),
                    json.dumps(required),
                    json.dumps([]),
                    "",
                ),
            )
            conn.commit()
        return request_id

    def get_status(self, request_id: str) -> str:
        with closing(self._connect()) as conn:
            row = conn.execute("SELECT status FROM approvals WHERE request_id = ?", (request_id,)).fetchone()
            return row["status"] if row else "expired"

    def get_request(self, request_id: str) -> Dict[str, Any]:
        with closing(self._connect()) as conn:
            row = conn.execute("SELECT * FROM approvals WHERE request_id = ?", (request_id,)).fetchone()
            if row is None:
                return {}
            return {
                "request_id": row["request_id"],
                "status": row["status"],
                "created_ts": row["created_ts"],
                "updated_ts": row["updated_ts"],
                "step": json.loads(row["step_json"]),
                "context": json.loads(row["context_json"]),
                "required_levels": json.loads(row["required_levels_json"]),
                "satisfied_levels": json.loads(row["satisfied_levels_json"]),
                "reason": row["reason"],
            }

    def approve(self, request_id: str, approver_id: str, approver_level: str, reason: str = "") -> str:
        now = time.time()
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT status, required_levels_json, satisfied_levels_json FROM approvals WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if row is None:
                return "expired"
            if row["status"] in {"denied", "approved", "expired"}:
                return row["status"]

            required = set(json.loads(row["required_levels_json"]))
            satisfied = set(json.loads(row["satisfied_levels_json"]))
            if approver_level in required:
                satisfied.add(approver_level)

            new_status = "approved" if required.issubset(satisfied) else "pending"
            conn.execute(
                "UPDATE approvals SET status = ?, updated_ts = ?, satisfied_levels_json = ? WHERE request_id = ?",
                (new_status, now, json.dumps(sorted(satisfied)), request_id),
            )
            conn.execute(
                """
                INSERT INTO approval_events(request_id, approver_id, approver_level, decision, reason, ts)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (request_id, approver_id, approver_level, "approve", reason, now),
            )
            conn.commit()
            return new_status

    def deny(self, request_id: str, approver_id: str, approver_level: str, reason: str = "") -> str:
        now = time.time()
        with closing(self._connect()) as conn:
            row = conn.execute("SELECT status FROM approvals WHERE request_id = ?", (request_id,)).fetchone()
            if row is None:
                return "expired"
            if row["status"] in {"denied", "approved", "expired"}:
                return row["status"]
            conn.execute(
                "UPDATE approvals SET status = ?, updated_ts = ?, reason = ? WHERE request_id = ?",
                ("denied", now, reason, request_id),
            )
            conn.execute(
                """
                INSERT INTO approval_events(request_id, approver_id, approver_level, decision, reason, ts)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (request_id, approver_id, approver_level, "deny", reason, now),
            )
            conn.commit()
            return "denied"

    def expire_pending(self, max_age_seconds: int) -> int:
        threshold = time.time() - max_age_seconds
        with closing(self._connect()) as conn:
            cur = conn.execute(
                """
                UPDATE approvals
                SET status = 'expired', updated_ts = ?
                WHERE status = 'pending' AND created_ts < ?
                """,
                (time.time(), threshold),
            )
            conn.commit()
            return int(cur.rowcount)
