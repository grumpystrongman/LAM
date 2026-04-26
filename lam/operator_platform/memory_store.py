from __future__ import annotations

import json
import sqlite3
import time
from contextlib import closing
from pathlib import Path
from typing import Any, Dict, List


class MemoryStore:
    def __init__(self, path: str | Path = "data/operator_platform/memory.db") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with closing(self._connect()) as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS kv_memory (
                    namespace TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value_json TEXT NOT NULL,
                    updated_ts REAL NOT NULL,
                    PRIMARY KEY(namespace, key)
                );
                CREATE TABLE IF NOT EXISTS artifact_memory (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    path TEXT NOT NULL,
                    domain TEXT NOT NULL,
                    geography TEXT NOT NULL,
                    invalidation_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_ts REAL NOT NULL
                );
                """
            )
            conn.commit()

    def put(self, namespace: str, key: str, value: Dict[str, Any]) -> None:
        with closing(self._connect()) as conn:
            conn.execute(
                "REPLACE INTO kv_memory(namespace, key, value_json, updated_ts) VALUES (?, ?, ?, ?)",
                (namespace, key, json.dumps(value), time.time()),
            )
            conn.commit()

    def get(self, namespace: str, key: str) -> Dict[str, Any]:
        with closing(self._connect()) as conn:
            row = conn.execute("SELECT value_json FROM kv_memory WHERE namespace = ? AND key = ?", (namespace, key)).fetchone()
        if not row:
            return {}
        return json.loads(row["value_json"])

    def remember_artifact(self, *, task_id: str, path: str, domain: str, geography: str, invalidation_key: str, status: str, metadata: Dict[str, Any]) -> None:
        with closing(self._connect()) as conn:
            conn.execute(
                """
                INSERT INTO artifact_memory(task_id, path, domain, geography, invalidation_key, status, metadata_json, created_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (task_id, path, domain, geography, invalidation_key, status, json.dumps(metadata), time.time()),
            )
            conn.commit()

    def recent_artifacts(self, invalidation_key: str, limit: int = 10) -> List[Dict[str, Any]]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT path, domain, geography, invalidation_key, status, metadata_json, created_ts
                FROM artifact_memory
                WHERE invalidation_key = ?
                ORDER BY created_ts DESC
                LIMIT ?
                """,
                (invalidation_key, limit),
            ).fetchall()
        return [
            {
                "path": row["path"],
                "domain": row["domain"],
                "geography": row["geography"],
                "invalidation_key": row["invalidation_key"],
                "status": row["status"],
                "metadata": json.loads(row["metadata_json"]),
                "created_ts": row["created_ts"],
            }
            for row in rows
        ]
