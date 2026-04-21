from __future__ import annotations

from typing import Any, Dict, List, Optional
from urllib.parse import urlparse


class PlaywrightAdapter:
    """Playwright-first web automation adapter with domain allowlist enforcement."""

    def __init__(self, domain_allowlist: Optional[List[str]] = None, dry_run: bool = True) -> None:
        self.domain_allowlist = set(domain_allowlist or [])
        self.dry_run = dry_run
        self._trace: List[Dict[str, Any]] = []

    def navigate_url(self, url: str) -> None:
        self._assert_domain_allowlisted(url)
        self._trace.append({"action": "navigate_url", "url": url})
        if self.dry_run:
            return
        # Real implementation should use Playwright page.goto().

    def click(self, selector_bundle: Dict[str, Any]) -> None:
        self._trace.append({"action": "click", "selector": selector_bundle})

    def type(self, selector_bundle: Dict[str, Any], text: str) -> None:
        self._trace.append({"action": "type", "selector": selector_bundle, "text": text})

    def wait_for(self, selector_bundle: Dict[str, Any], timeout_ms: Optional[int] = None) -> None:
        self._trace.append({"action": "wait_for", "selector": selector_bundle, "timeout_ms": timeout_ms})

    def assert_visible(self, selector_bundle: Dict[str, Any], timeout_ms: Optional[int] = None) -> None:
        self._trace.append({"action": "assert_visible", "selector": selector_bundle, "timeout_ms": timeout_ms})

    def extract_field(self, selector_bundle: Dict[str, Any]) -> str:
        self._trace.append({"action": "extract_field", "selector": selector_bundle})
        return "stub_value"

    def screenshot_redacted(self, reason: str) -> None:
        # Persist only pre-redacted artifacts when explicitly enabled by policy.
        self._trace.append({"action": "screenshot_redacted", "reason": reason})

    def generic_action(self, action: str, target: Dict[str, Any], data: Dict[str, Any]) -> None:
        self._trace.append({"action": action, "target": target, "data": data})

    def trace(self) -> List[Dict[str, Any]]:
        return list(self._trace)

    def _assert_domain_allowlisted(self, url: str) -> None:
        if not self.domain_allowlist:
            return
        host = (urlparse(url).hostname or "").lower()
        if host not in self.domain_allowlist:
            raise PermissionError(f"Domain not allowlisted: {host}")

