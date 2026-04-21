from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator

from lam.governance.audit_logger import JsonlAuditSink


class SqliteAuditSink:
    """
    SQLite-backed append-only sink.
    UPDATE/DELETE are prevented by triggers to emulate WORM-like semantics.
    """

    def __init__(self, path: str | Path = "data/audit/events.db") -> None:
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
                CREATE TABLE IF NOT EXISTS audit_events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_json TEXT NOT NULL,
                    event_hash TEXT NOT NULL UNIQUE,
                    prev_hash TEXT NOT NULL,
                    created_ts REAL NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TRIGGER IF NOT EXISTS audit_events_no_update
                BEFORE UPDATE ON audit_events
                BEGIN
                    SELECT RAISE(ABORT, 'audit_events are append-only');
                END;
                """
            )
            conn.execute(
                """
                CREATE TRIGGER IF NOT EXISTS audit_events_no_delete
                BEFORE DELETE ON audit_events
                BEGIN
                    SELECT RAISE(ABORT, 'audit_events are append-only');
                END;
                """
            )
            conn.commit()

    def append(self, event: Dict[str, Any]) -> None:
        with closing(self._connect()) as conn:
            conn.execute(
                "INSERT INTO audit_events(event_json, event_hash, prev_hash, created_ts) VALUES(?, ?, ?, ?)",
                (
                    json.dumps(event, sort_keys=True),
                    event.get("event_hash", ""),
                    event.get("prev_hash", "GENESIS"),
                    float(event.get("ts", 0.0)),
                ),
            )
            conn.commit()

    def latest_hash(self) -> str:
        with closing(self._connect()) as conn:
            row = conn.execute("SELECT event_hash FROM audit_events ORDER BY seq DESC LIMIT 1").fetchone()
            return row["event_hash"] if row else "GENESIS"

    def iter_events(self) -> Iterator[Dict[str, Any]]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT event_json FROM audit_events ORDER BY seq ASC").fetchall()
            for row in rows:
                yield json.loads(row["event_json"])


class AuditStore:
    def __init__(self, path: str | Path = "data/audit/events.jsonl", backend: str = "jsonl") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if backend == "sqlite":
            sqlite_path = self.path if self.path.suffix == ".db" else self.path.with_suffix(".db")
            self.sink = SqliteAuditSink(sqlite_path)
        else:
            self.sink = JsonlAuditSink(self.path)

    def append(self, event: Dict[str, Any]) -> None:
        self.sink.append(event)

    def latest_hash(self) -> str:
        return self.sink.latest_hash()

    def iter_events(self) -> Iterable[Dict[str, Any]]:
        return self.sink.iter_events()
