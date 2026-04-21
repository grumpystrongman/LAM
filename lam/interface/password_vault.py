from __future__ import annotations

import json
import secrets
import sqlite3
import string
import time
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from lam.interface.dpapi import dpapi_available, protect_text, unprotect_text


def _vault_path() -> Path:
    path = Path("data/interface/vault.db")
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _usage_log_path() -> Path:
    path = Path("data/interface/vault_usage.jsonl")
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


@dataclass(slots=True)
class VaultEntry:
    id: str
    service: str
    username: str
    password: str
    notes: str
    tags: List[str]
    favorite: bool
    created_ts: float
    updated_ts: float
    last_used_ts: float

    def redacted(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "service": self.service,
            "username_masked": _mask_value(self.username),
            "password_masked": "***",
            "notes_present": bool(self.notes),
            "tags": list(self.tags),
            "favorite": self.favorite,
            "created_ts": self.created_ts,
            "updated_ts": self.updated_ts,
            "last_used_ts": self.last_used_ts,
        }


def _mask_value(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 2:
        return "*" * len(value)
    return value[:2] + "*" * (len(value) - 2)


class LocalPasswordVault:
    def __init__(self, db_path: Optional[Path] = None) -> None:
        self.db_path = db_path or _vault_path()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with closing(self._connect()) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS vault_entries(
                    id TEXT PRIMARY KEY,
                    service TEXT NOT NULL,
                    username_enc TEXT NOT NULL,
                    password_enc TEXT NOT NULL,
                    notes_enc TEXT NOT NULL,
                    tags_json TEXT NOT NULL,
                    favorite INTEGER NOT NULL DEFAULT 0,
                    created_ts REAL NOT NULL,
                    updated_ts REAL NOT NULL,
                    last_used_ts REAL NOT NULL DEFAULT 0
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_vault_service ON vault_entries(service)")
            conn.commit()

    def status(self) -> Dict[str, Any]:
        with closing(self._connect()) as conn:
            row = conn.execute("SELECT COUNT(*) AS c FROM vault_entries").fetchone()
            return {
                "ok": True,
                "dpapi_available": dpapi_available(),
                "entries": int(row["c"] if row else 0),
                "path": str(self.db_path.resolve()),
            }

    def put_entry(
        self,
        service: str,
        username: str,
        password: str,
        notes: str = "",
        tags: Optional[List[str]] = None,
        favorite: bool = False,
        entry_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        now = time.time()
        safe_tags = [t.strip().lower() for t in (tags or []) if t.strip()]
        eid = entry_id or secrets.token_hex(8)
        with closing(self._connect()) as conn:
            existing = conn.execute("SELECT id, created_ts FROM vault_entries WHERE id = ?", (eid,)).fetchone()
            created_ts = float(existing["created_ts"]) if existing else now
            conn.execute(
                """
                INSERT OR REPLACE INTO vault_entries
                (id, service, username_enc, password_enc, notes_enc, tags_json, favorite, created_ts, updated_ts, last_used_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT last_used_ts FROM vault_entries WHERE id = ?), 0))
                """,
                (
                    eid,
                    service.strip(),
                    protect_text(username),
                    protect_text(password),
                    protect_text(notes),
                    json.dumps(safe_tags),
                    1 if favorite else 0,
                    created_ts,
                    now,
                    eid,
                ),
            )
            conn.commit()
        self._audit("put_entry", {"id": eid, "service": service.strip()})
        return {"ok": True, "id": eid}

    def list_entries(self, query: str = "", tag: str = "", favorite_only: bool = False) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM vault_entries"
        where: List[str] = []
        vals: List[Any] = []
        if query.strip():
            where.append("LOWER(service) LIKE ?")
            vals.append(f"%{query.strip().lower()}%")
        if favorite_only:
            where.append("favorite = 1")
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY favorite DESC, updated_ts DESC"

        out: List[Dict[str, Any]] = []
        with closing(self._connect()) as conn:
            rows = conn.execute(sql, tuple(vals)).fetchall()
            for row in rows:
                tags = _parse_tags(row["tags_json"])
                if tag and tag.lower() not in tags:
                    continue
                entry = VaultEntry(
                    id=row["id"],
                    service=row["service"],
                    username=unprotect_text(row["username_enc"]),
                    password="",
                    notes="",
                    tags=tags,
                    favorite=bool(row["favorite"]),
                    created_ts=float(row["created_ts"]),
                    updated_ts=float(row["updated_ts"]),
                    last_used_ts=float(row["last_used_ts"]),
                )
                out.append(entry.redacted())
        return out

    def get_entry(self, entry_id: str, include_secret: bool = False) -> Dict[str, Any]:
        with closing(self._connect()) as conn:
            row = conn.execute("SELECT * FROM vault_entries WHERE id = ?", (entry_id,)).fetchone()
            if not row:
                return {"ok": False, "error": "Entry not found"}
            entry = VaultEntry(
                id=row["id"],
                service=row["service"],
                username=unprotect_text(row["username_enc"]),
                password=unprotect_text(row["password_enc"]),
                notes=unprotect_text(row["notes_enc"]),
                tags=_parse_tags(row["tags_json"]),
                favorite=bool(row["favorite"]),
                created_ts=float(row["created_ts"]),
                updated_ts=float(row["updated_ts"]),
                last_used_ts=float(row["last_used_ts"]),
            )
        if include_secret:
            return {
                "ok": True,
                "entry": {
                    "id": entry.id,
                    "service": entry.service,
                    "username": entry.username,
                    "password": entry.password,
                    "notes": entry.notes,
                    "tags": entry.tags,
                    "favorite": entry.favorite,
                },
            }
        return {"ok": True, "entry": entry.redacted()}

    def delete_entry(self, entry_id: str) -> Dict[str, Any]:
        with closing(self._connect()) as conn:
            cur = conn.execute("DELETE FROM vault_entries WHERE id = ?", (entry_id,))
            conn.commit()
        self._audit("delete_entry", {"id": entry_id, "deleted": cur.rowcount})
        return {"ok": cur.rowcount > 0}

    def find_entry_by_service(self, service: str) -> Dict[str, Any]:
        low = service.strip().lower()
        with closing(self._connect()) as conn:
            row = conn.execute(
                """
                SELECT * FROM vault_entries
                WHERE LOWER(service)=?
                ORDER BY favorite DESC, updated_ts DESC
                LIMIT 1
                """,
                (low,),
            ).fetchone()
            if row is None:
                row = conn.execute(
                    """
                    SELECT * FROM vault_entries
                    WHERE LOWER(service) LIKE ?
                    ORDER BY favorite DESC, updated_ts DESC
                    LIMIT 1
                    """,
                    (f"%{low}%",),
                ).fetchone()
        if row is None:
            return {"ok": False, "error": f"No vault entry for '{service}'"}
        return {
            "ok": True,
            "entry": {
                "id": row["id"],
                "service": row["service"],
                "username": unprotect_text(row["username_enc"]),
                "password": unprotect_text(row["password_enc"]),
                "notes": unprotect_text(row["notes_enc"]),
                "tags": _parse_tags(row["tags_json"]),
                "favorite": bool(row["favorite"]),
            },
        }

    def touch_used(self, entry_id: str) -> None:
        with closing(self._connect()) as conn:
            conn.execute("UPDATE vault_entries SET last_used_ts=?, updated_ts=updated_ts WHERE id=?", (time.time(), entry_id))
            conn.commit()

    def export_encrypted(self, output_path: str) -> Dict[str, Any]:
        data: Dict[str, Any] = {"exported_ts": time.time(), "entries": []}
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT * FROM vault_entries ORDER BY updated_ts DESC").fetchall()
        for row in rows:
            data["entries"].append({k: row[k] for k in row.keys()})
        payload = json.dumps(data).encode("utf-8")
        wrapped = protect_text(payload.decode("utf-8"))
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(wrapped, encoding="utf-8")
        self._audit("export_encrypted", {"output_path": str(out.resolve()), "entries": len(data["entries"])})
        return {"ok": True, "path": str(out.resolve()), "entries": len(data["entries"])}

    def import_encrypted(self, input_path: str, merge: bool = True) -> Dict[str, Any]:
        src = Path(input_path)
        if not src.exists():
            return {"ok": False, "error": "Input file not found"}
        raw = src.read_text(encoding="utf-8")
        doc = json.loads(unprotect_text(raw))
        imported = 0
        with closing(self._connect()) as conn:
            if not merge:
                conn.execute("DELETE FROM vault_entries")
            for item in doc.get("entries", []):
                conn.execute(
                    """
                    INSERT OR REPLACE INTO vault_entries
                    (id, service, username_enc, password_enc, notes_enc, tags_json, favorite, created_ts, updated_ts, last_used_ts)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        item.get("id", secrets.token_hex(8)),
                        item.get("service", ""),
                        item.get("username_enc", ""),
                        item.get("password_enc", ""),
                        item.get("notes_enc", ""),
                        item.get("tags_json", "[]"),
                        int(item.get("favorite", 0)),
                        float(item.get("created_ts", time.time())),
                        float(item.get("updated_ts", time.time())),
                        float(item.get("last_used_ts", 0)),
                    ),
                )
                imported += 1
            conn.commit()
        self._audit("import_encrypted", {"input_path": str(src.resolve()), "imported": imported, "merge": merge})
        return {"ok": True, "imported": imported}

    def generate_password(
        self,
        length: int = 20,
        include_upper: bool = True,
        include_lower: bool = True,
        include_digits: bool = True,
        include_symbols: bool = True,
        exclude_ambiguous: bool = True,
    ) -> Dict[str, Any]:
        length = max(12, min(128, int(length)))
        pools: List[str] = []
        if include_upper:
            pools.append(string.ascii_uppercase)
        if include_lower:
            pools.append(string.ascii_lowercase)
        if include_digits:
            pools.append(string.digits)
        if include_symbols:
            pools.append("!@#$%^&*()-_=+[]{};:,.?")
        if not pools:
            pools.append(string.ascii_letters + string.digits)
        if exclude_ambiguous:
            ambiguous = set("O0Il1|")
            pools = ["".join(ch for ch in pool if ch not in ambiguous) for pool in pools]
        all_chars = "".join(pools)
        pwd_chars = [secrets.choice(pool) for pool in pools if pool]
        while len(pwd_chars) < length:
            pwd_chars.append(secrets.choice(all_chars))
        secrets.SystemRandom().shuffle(pwd_chars)
        password = "".join(pwd_chars[:length])
        return {"ok": True, "password": password, "strength": _score_password(password)}

    def _audit(self, event: str, payload: Dict[str, Any]) -> None:
        safe = {"ts": time.time(), "event": event, "payload": payload}
        with _usage_log_path().open("a", encoding="utf-8") as f:
            f.write(json.dumps(safe) + "\n")


def _parse_tags(raw: str) -> List[str]:
    try:
        parsed = json.loads(raw or "[]")
        if isinstance(parsed, list):
            return [str(x).lower() for x in parsed]
    except Exception:
        return []
    return []


def _score_password(value: str) -> Dict[str, Any]:
    length = len(value)
    has_upper = any(c.isupper() for c in value)
    has_lower = any(c.islower() for c in value)
    has_digit = any(c.isdigit() for c in value)
    has_symbol = any(c in "!@#$%^&*()-_=+[]{};:,.?" for c in value)
    score = 0
    score += 25 if length >= 16 else 15 if length >= 12 else 5
    score += 20 if has_upper else 0
    score += 20 if has_lower else 0
    score += 20 if has_digit else 0
    score += 15 if has_symbol else 0
    rating = "strong" if score >= 80 else "medium" if score >= 55 else "weak"
    return {
        "score": min(score, 100),
        "rating": rating,
        "length": length,
        "upper": has_upper,
        "lower": has_lower,
        "digit": has_digit,
        "symbol": has_symbol,
    }
