from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
import time
from contextlib import closing
from pathlib import Path
from typing import Dict, List


class LocalVectorStore:
    def __init__(self, path: str | Path = "data/knowledge/app_knowledge.db", dims: int = 256) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.dims = dims
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with closing(self._connect()) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS knowledge_docs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    app_name TEXT NOT NULL,
                    source_url TEXT NOT NULL,
                    title TEXT NOT NULL,
                    content TEXT NOT NULL,
                    vector_json TEXT NOT NULL,
                    created_ts REAL NOT NULL
                )
                """
            )
            conn.commit()

    def add_document(self, app_name: str, source_url: str, title: str, content: str) -> None:
        vector = self.embed(content)
        with closing(self._connect()) as conn:
            conn.execute(
                """
                INSERT INTO knowledge_docs(app_name, source_url, title, content, vector_json, created_ts)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (app_name.lower(), source_url, title, content[:8000], json.dumps(vector), time.time()),
            )
            conn.commit()

    def search(self, app_name: str, query: str, top_k: int = 5) -> List[Dict]:
        qvec = self.embed(query)
        rows: List[Dict] = []
        with closing(self._connect()) as conn:
            data = conn.execute(
                "SELECT app_name, source_url, title, content, vector_json, created_ts FROM knowledge_docs WHERE app_name = ?",
                (app_name.lower(),),
            ).fetchall()
            for row in data:
                dvec = json.loads(row["vector_json"])
                score = _cosine_similarity(qvec, dvec)
                rows.append(
                    {
                        "app_name": row["app_name"],
                        "source_url": row["source_url"],
                        "title": row["title"],
                        "content": row["content"],
                        "created_ts": row["created_ts"],
                        "score": score,
                    }
                )
        rows.sort(key=lambda x: x["score"], reverse=True)
        return rows[:top_k]

    def embed(self, text: str) -> List[float]:
        vec = [0.0] * self.dims
        tokens = re.findall(r"[a-z0-9]{2,}", text.lower())
        if not tokens:
            return vec
        for tok in tokens:
            idx = int(hashlib.sha1(tok.encode("utf-8")).hexdigest(), 16) % self.dims
            vec[idx] += 1.0
        norm = math.sqrt(sum(v * v for v in vec)) or 1.0
        return [v / norm for v in vec]


def _cosine_similarity(a: List[float], b: List[float]) -> float:
    size = min(len(a), len(b))
    if size == 0:
        return 0.0
    return sum(a[i] * b[i] for i in range(size))
