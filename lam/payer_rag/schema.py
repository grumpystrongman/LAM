from __future__ import annotations

import hashlib
import re
from typing import Iterable


def slugify(value: str) -> str:
    token = re.sub(r"[^a-z0-9]+", "-", (value or "").strip().lower()).strip("-")
    return token or "unknown"


def make_id(prefix: str, *parts: str) -> str:
    joined = "|".join(part.strip() for part in parts if part is not None)
    digest = hashlib.sha1(joined.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def unique_rows(rows: Iterable[dict], key: str) -> list[dict]:
    seen: dict[str, dict] = {}
    for row in rows:
        seen[row[key]] = row
    return list(seen.values())

