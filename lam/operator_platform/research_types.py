from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(slots=True)
class SearchResult:
    title: str
    url: str
    price: Optional[float]
    source: str
    snippet: str = ""
