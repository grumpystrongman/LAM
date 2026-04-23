from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlparse


@dataclass(slots=True)
class SessionDecision:
    allow_retry: bool
    reason: str
    failed_attempts: int
    reusable_authenticated_tab: str


class SessionManager:
    def __init__(self, path: str = "data/interface/session_state.json") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._state = self._load()

    def _load(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {"tabs": [], "auth_attempts": []}
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                raw.setdefault("tabs", [])
                raw.setdefault("auth_attempts", [])
                return raw
        except Exception:
            pass
        return {"tabs": [], "auth_attempts": []}

    def _save(self) -> None:
        self.path.write_text(json.dumps(self._state, indent=2), encoding="utf-8")

    def remember_tab(self, *, url: str, title: str = "", authenticated: bool = False) -> None:
        url_value = str(url or "").strip()
        if not url_value:
            return
        host = urlparse(url_value).netloc.lower()
        now = time.time()
        entry = {
            "ts": now,
            "url": url_value,
            "host": host,
            "title": str(title or "")[:200],
            "authenticated": bool(authenticated),
        }
        tabs = [x for x in self._state.get("tabs", []) if isinstance(x, dict)]
        tabs.append(entry)
        self._state["tabs"] = tabs[-120:]
        self._save()

    def record_auth_attempt(self, *, domain: str, status: str, detail: str = "") -> None:
        attempts = [x for x in self._state.get("auth_attempts", []) if isinstance(x, dict)]
        attempts.append(
            {
                "ts": time.time(),
                "domain": str(domain or "").strip().lower(),
                "status": str(status or "").strip().lower(),
                "detail": str(detail or "")[:300],
            }
        )
        self._state["auth_attempts"] = attempts[-200:]
        self._save()

    def find_reusable_authenticated_tab(self, host_hint: str) -> str:
        hint = str(host_hint or "").strip().lower()
        tabs = [x for x in self._state.get("tabs", []) if isinstance(x, dict)]
        for tab in reversed(tabs):
            host = str(tab.get("host", "")).lower()
            if hint and hint not in host:
                continue
            if bool(tab.get("authenticated", False)):
                return str(tab.get("url", ""))
        return ""

    def find_reusable_url(self, url: str) -> str:
        target = str(url or "").strip().lower()
        if not target:
            return ""
        tabs = [x for x in self._state.get("tabs", []) if isinstance(x, dict)]
        for tab in reversed(tabs):
            value = str(tab.get("url", "")).strip().lower()
            if value and value == target:
                return str(tab.get("url", ""))
        return ""

    def auth_retry_decision(self, *, domain: str, max_failed_attempts: int = 2) -> SessionDecision:
        key = str(domain or "").strip().lower()
        attempts = [x for x in self._state.get("auth_attempts", []) if isinstance(x, dict) and str(x.get("domain", "")).lower() == key]
        recent = attempts[-8:]
        failed = [x for x in recent if str(x.get("status", "")) in {"failed", "blocked", "loop"}]
        reusable = self.find_reusable_authenticated_tab(host_hint=key)
        if reusable:
            return SessionDecision(
                allow_retry=True,
                reason="reusable_authenticated_tab",
                failed_attempts=len(failed),
                reusable_authenticated_tab=reusable,
            )
        if len(failed) >= max_failed_attempts:
            return SessionDecision(
                allow_retry=False,
                reason="auth_retry_budget_exhausted",
                failed_attempts=len(failed),
                reusable_authenticated_tab="",
            )
        return SessionDecision(
            allow_retry=True,
            reason="retry_available",
            failed_attempts=len(failed),
            reusable_authenticated_tab="",
        )

    def snapshot(self) -> Dict[str, Any]:
        tabs = [x for x in self._state.get("tabs", []) if isinstance(x, dict)]
        attempts = [x for x in self._state.get("auth_attempts", []) if isinstance(x, dict)]
        return {
            "tabs_count": len(tabs),
            "auth_attempts_count": len(attempts),
            "latest_tab": tabs[-1] if tabs else {},
            "latest_auth_attempt": attempts[-1] if attempts else {},
        }
