from __future__ import annotations

import json
import sqlite3
import time
import uuid
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
                CREATE TABLE IF NOT EXISTS memory_items (
                    memory_id TEXT PRIMARY KEY,
                    type TEXT NOT NULL,
                    scope TEXT NOT NULL,
                    project_id TEXT NOT NULL,
                    content_json TEXT NOT NULL,
                    tags_json TEXT NOT NULL,
                    source TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    created_ts REAL NOT NULL,
                    updated_ts REAL NOT NULL,
                    expires_ts REAL,
                    retrieval_policy TEXT NOT NULL,
                    invalidation_keys_json TEXT NOT NULL,
                    usage_count INTEGER NOT NULL DEFAULT 0,
                    last_used_ts REAL,
                    status TEXT NOT NULL DEFAULT 'active',
                    rejection_reason TEXT NOT NULL DEFAULT ''
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

    def save_memory(self, item: Dict[str, Any]) -> str:
        memory_id = str(item.get("memory_id", "")).strip() or uuid.uuid4().hex
        now = time.time()
        expires_at = item.get("expires_at")
        expires_ts = float(expires_at) if isinstance(expires_at, (int, float)) else None
        with closing(self._connect()) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO memory_items(
                    memory_id, type, scope, project_id, content_json, tags_json, source, confidence,
                    created_ts, updated_ts, expires_ts, retrieval_policy, invalidation_keys_json,
                    usage_count, last_used_ts, status, rejection_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT created_ts FROM memory_items WHERE memory_id = ?), ?), ?, ?, ?, ?, COALESCE((SELECT usage_count FROM memory_items WHERE memory_id = ?), 0), COALESCE((SELECT last_used_ts FROM memory_items WHERE memory_id = ?), 0), ?, ?)
                """,
                (
                    memory_id,
                    str(item.get("type", "project_context")),
                    str(item.get("scope", "project")),
                    str(item.get("project_id", "")),
                    json.dumps(item.get("content", {})),
                    json.dumps(item.get("tags", [])),
                    str(item.get("source", "runtime")),
                    float(item.get("confidence", 0.5) or 0.5),
                    memory_id,
                    now,
                    now,
                    expires_ts,
                    str(item.get("retrieval_policy", "strict")),
                    json.dumps(item.get("invalidation_keys", {})),
                    memory_id,
                    memory_id,
                    str(item.get("status", "active")),
                    str(item.get("rejection_reason", "")),
                ),
            )
            conn.commit()
        return memory_id

    def retrieve_relevant_memory(self, *, task_contract: Dict[str, Any], query: str, limit: int = 5, project_id: str = "") -> Dict[str, Any]:
        now = time.time()
        rows: List[sqlite3.Row]
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM memory_items
                WHERE status = 'active'
                  AND (expires_ts IS NULL OR expires_ts > ?)
                  AND (? = '' OR project_id = ? OR project_id = '')
                ORDER BY updated_ts DESC
                """,
                (now, project_id, project_id),
            ).fetchall()
        used: List[Dict[str, Any]] = []
        rejected: List[Dict[str, Any]] = []
        for row in rows:
            item = self._row_to_memory_item(row)
            conflict = self._memory_conflicts(item=item, task_contract=task_contract)
            if conflict:
                rejected.append({"memory_id": item["memory_id"], "reason": conflict, "type": item["type"]})
                continue
            score = self._retrieval_score(item=item, task_contract=task_contract, query=query)
            item["retrieval_score"] = score
            used.append(item)
        used.sort(key=lambda x: float(x.get("retrieval_score", 0.0)), reverse=True)
        picked = used[: max(1, int(limit))]
        for item in picked:
            self.mark_memory_used(item["memory_id"], str(task_contract.get("task_id", "")))
        project_preferences = [item for item in picked if item.get("type") in {"user_preference", "style_preference", "stakeholder_preference"}]
        confidence = round(sum(float(item.get("retrieval_score", 0.0)) for item in picked) / max(1, len(picked)), 3)
        return {
            "used": picked,
            "rejected": rejected[:10],
            "project_preferences": project_preferences[:5],
            "retrieval_confidence": confidence,
        }

    def mark_memory_used(self, memory_id: str, task_id: str) -> None:
        with closing(self._connect()) as conn:
            conn.execute(
                "UPDATE memory_items SET usage_count = usage_count + 1, last_used_ts = ?, updated_ts = ? WHERE memory_id = ?",
                (time.time(), time.time(), memory_id),
            )
            conn.commit()

    def reject_memory(self, memory_id: str, reason: str) -> None:
        with closing(self._connect()) as conn:
            conn.execute(
                "UPDATE memory_items SET status = 'rejected', rejection_reason = ?, updated_ts = ? WHERE memory_id = ?",
                (reason[:300], time.time(), memory_id),
            )
            conn.commit()

    def expire_memory(self, memory_id: str) -> None:
        with closing(self._connect()) as conn:
            conn.execute(
                "UPDATE memory_items SET expires_ts = ?, updated_ts = ? WHERE memory_id = ?",
                (time.time() - 1, time.time(), memory_id),
            )
            conn.commit()

    def list_project_memory(self, project_id: str) -> List[Dict[str, Any]]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM memory_items WHERE project_id = ? ORDER BY updated_ts DESC",
                (project_id,),
            ).fetchall()
        return [self._row_to_memory_item(row) for row in rows]

    def _row_to_memory_item(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "memory_id": row["memory_id"],
            "type": row["type"],
            "scope": row["scope"],
            "project_id": row["project_id"],
            "content": json.loads(row["content_json"]),
            "tags": json.loads(row["tags_json"]),
            "source": row["source"],
            "confidence": row["confidence"],
            "created_at": row["created_ts"],
            "updated_at": row["updated_ts"],
            "expires_at": row["expires_ts"],
            "retrieval_policy": row["retrieval_policy"],
            "invalidation_keys": json.loads(row["invalidation_keys_json"]),
            "usage_count": row["usage_count"],
            "last_used_at": row["last_used_ts"],
            "status": row["status"],
            "rejection_reason": row["rejection_reason"],
        }

    def _memory_conflicts(self, *, item: Dict[str, Any], task_contract: Dict[str, Any]) -> str:
        policy = str(item.get("retrieval_policy", "strict"))
        if policy == "template_safe":
            return ""
        item_keys = item.get("invalidation_keys", {}) if isinstance(item.get("invalidation_keys"), dict) else {}
        task_keys = task_contract.get("invalidation_keys", {}) if isinstance(task_contract.get("invalidation_keys"), dict) else {}
        for key in ["geography", "domain", "audience"]:
            item_value = str(item_keys.get(key, "")).strip().lower()
            task_value = str(task_keys.get(key, "")).strip().lower()
            if item_value and task_value and item_value != task_value:
                return f"conflicts on {key}"
        return ""

    def _retrieval_score(self, *, item: Dict[str, Any], task_contract: Dict[str, Any], query: str) -> float:
        score = float(item.get("confidence", 0.5) or 0.5)
        task_keys = task_contract.get("invalidation_keys", {}) if isinstance(task_contract.get("invalidation_keys"), dict) else {}
        item_keys = item.get("invalidation_keys", {}) if isinstance(item.get("invalidation_keys"), dict) else {}
        for key in ["domain", "geography", "audience"]:
            if str(item_keys.get(key, "")).strip().lower() == str(task_keys.get(key, "")).strip().lower() and str(task_keys.get(key, "")).strip():
                score += 0.3
        hay = json.dumps(item.get("content", {})).lower() + " " + " ".join(str(x).lower() for x in (item.get("tags", []) or []))
        for token in [t for t in str(query or "").lower().split() if len(t) > 3][:8]:
            if token in hay:
                score += 0.05
        recency_hours = max(1.0, (time.time() - float(item.get("updated_at", time.time()) or time.time())) / 3600.0)
        score += max(0.0, 0.2 - min(0.2, recency_hours / 1000.0))
        return round(score, 4)
