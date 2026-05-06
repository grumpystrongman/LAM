from __future__ import annotations

import sqlite3
from typing import Any, Dict, List

from lam.operator_platform.memory_store import MemoryStore


class LearnMemory:
    def __init__(self, store: MemoryStore | None = None) -> None:
        self.store = store or MemoryStore()
        self._ensure_store()

    def save_topic(self, payload: Dict[str, Any]) -> str:
        topic = str(payload.get("topic", "")).strip()
        key = f"learned_topic:{topic.lower()}" if topic else "learned_topic:unknown"
        try:
            self.store.put("topic_mastery", key, payload)
        except sqlite3.OperationalError:
            self._ensure_store()
            self.store.put("topic_mastery", key, payload)
        return key

    def get_topic(self, topic: str) -> Dict[str, Any]:
        try:
            return self.store.get("topic_mastery", f"learned_topic:{str(topic or '').strip().lower()}")
        except sqlite3.OperationalError:
            self._ensure_store()
            return self.store.get("topic_mastery", f"learned_topic:{str(topic or '').strip().lower()}")

    def forget_topic(self, topic: str) -> None:
        self.store.put("topic_mastery", f"learned_topic:{str(topic or '').strip().lower()}", {})

    def save_memory_item(self, payload: Dict[str, Any]) -> str:
        item = {
            "type": "learned_topic",
            "scope": "project",
            "project_id": str(payload.get("topic", "topic_mastery")),
            "content": payload,
            "tags": ["topic_mastery", str(payload.get("topic", "")).lower()],
            "source": "topic_mastery_runtime",
            "confidence": float(payload.get("confidence", 0.5) or 0.5),
            "retrieval_policy": "strict",
            "invalidation_keys": {"domain": "topic_learning", "topic": str(payload.get("topic", ""))},
        }
        try:
            return self.store.save_memory(item)
        except sqlite3.OperationalError:
            self._ensure_store()
            return self.store.save_memory(item)

    def retrieve(self, topic: str, limit: int = 5) -> Dict[str, Any]:
        try:
            return self.store.retrieve_relevant_memory(task_contract={"domain": "topic_learning", "invalidation_keys": {"domain": "topic_learning"}}, query=topic, limit=limit, project_id=topic)
        except sqlite3.OperationalError:
            self._ensure_store()
            return self.store.retrieve_relevant_memory(task_contract={"domain": "topic_learning", "invalidation_keys": {"domain": "topic_learning"}}, query=topic, limit=limit, project_id=topic)

    def _ensure_store(self) -> None:
        init = getattr(self.store, "_init_db", None)
        if callable(init):
            init()
