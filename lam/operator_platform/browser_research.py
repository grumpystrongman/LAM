from __future__ import annotations

import re
import urllib.parse
from typing import Any, Callable, Dict, List, Optional

from lam.interface.browser_worker import ensure_browser_worker


def browser_query_url(query: str) -> str:
    return f"https://duckduckgo.com/?q={urllib.parse.quote_plus(str(query or '').strip())}"


def select_best_context_page(context: Any, target_url: str) -> Any:
    best = None
    try:
        pages = list(getattr(context, "pages", []) or [])
    except Exception:
        pages = []
    target_host = urllib.parse.urlparse(target_url).netloc.lower()
    for page in pages:
        try:
            url = str(page.url or "").lower()
        except Exception:
            continue
        if target_host and target_host in url:
            return page
        if "about:blank" in url and best is None:
            best = page
    return best


def attach_generic_browser_context(*, playwright: Any, browser_worker_mode: str, human_like_interaction: bool) -> tuple[Any, Any, bool, Dict[str, Any]]:
    worker_info = ensure_browser_worker(mode=browser_worker_mode)
    browser = None
    context = None
    attached = False
    if bool(worker_info.get("ok")):
        debug_port = int(worker_info.get("debug_port", 9222) or 9222)
        try:
            browser = playwright.chromium.connect_over_cdp(f"http://127.0.0.1:{debug_port}", timeout=3500)
            contexts = list(getattr(browser, "contexts", []) or [])
            context = contexts[0] if contexts else browser.new_context()
            attached = True
        except Exception:
            browser = None
            context = None
    if context is None:
        browser = playwright.chromium.launch(headless=not bool(human_like_interaction))
        context = browser.new_context()
    return browser, context, attached, worker_info


def browser_extract_page_text(page: Any, limit: int = 900) -> str:
    try:
        body = page.locator("body")
        text = str(body.inner_text(timeout=2000) or "")
    except Exception:
        try:
            text = str(page.content() or "")
        except Exception:
            text = ""
    text = re.sub(r"\s+", " ", text or "").strip()
    return text[:limit]


def browser_note_for_page(*, query: str, page_url: str, title: str, text: str) -> Dict[str, Any]:
    cleaned_title = re.sub(r"\s+", " ", str(title or "")).strip()
    cleaned_text = re.sub(r"\s+", " ", str(text or "")).strip()
    sentences = re.split(r"(?<=[.!?])\s+", cleaned_text)
    summary = ""
    terms = [term for term in re.split(r"[^a-z0-9]+", query.lower()) if len(term) > 3]
    for sentence in sentences:
        low = sentence.lower()
        if any(term in low for term in terms):
            summary = sentence.strip()
            break
    if not summary:
        summary = cleaned_text[:220]
    return {
        "url": str(page_url or ""),
        "title": cleaned_title,
        "summary": summary[:260],
        "excerpt": cleaned_text[:900],
    }


def browser_research_walk(
    *,
    query: str,
    candidates: List[Any],
    browser_worker_mode: str,
    human_like_interaction: bool,
    progress_cb: Optional[Callable[[int, str], None]] = None,
    max_pages: int = 4,
) -> Dict[str, Any]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return {"ok": False, "notes": [], "opened_url": "", "worker_status": "playwright_unavailable"}

    from lam.interface import search_agent as search_agent_mod

    notes: List[Dict[str, Any]] = []
    opened_url = ""
    with sync_playwright() as p:
        browser = None
        attached = False
        try:
            browser, context, attached, worker_info = attach_generic_browser_context(
                playwright=p,
                browser_worker_mode=browser_worker_mode,
                human_like_interaction=human_like_interaction,
            )
            pages = list(getattr(context, "pages", []) or [])
            page = pages[0] if pages else context.new_page()
            search_url = browser_query_url(query)
            current_url = str(getattr(page, "url", "") or "").strip()
            if not current_url:
                page.goto(search_url, timeout=30000)
                page.wait_for_timeout(900 if human_like_interaction else 250)
                opened_url = search_url
            for idx, candidate in enumerate(candidates[: max(1, max_pages)], start=1):
                target = str(getattr(candidate, "url", "") or (candidate.get("url", "") if isinstance(candidate, dict) else "")).strip()
                title = str(getattr(candidate, "title", "") or (candidate.get("title", "") if isinstance(candidate, dict) else ""))
                snippet = str(getattr(candidate, "snippet", "") or (candidate.get("snippet", "") if isinstance(candidate, dict) else ""))
                if not target:
                    continue
                search_agent_mod._emit_progress(progress_cb, min(92, 70 + idx * 4), f"Reviewing source page {idx}")  # type: ignore[attr-defined]
                try:
                    page.goto(target, timeout=30000)
                    page.wait_for_timeout(1100 if human_like_interaction else 250)
                    page_title = str(page.title() or title or "")
                    text = browser_extract_page_text(page)
                    notes.append(browser_note_for_page(query=query, page_url=target, title=page_title, text=text))
                    opened_url = target
                except Exception:
                    notes.append({"url": target, "title": title, "summary": snippet[:260]})
            return {
                "ok": True,
                "notes": notes,
                "opened_url": opened_url,
                "worker_status": str(worker_info.get("status", worker_info.get("mode", "unknown"))),
            }
        finally:
            if browser is not None and not attached:
                try:
                    browser.close()
                except Exception:
                    pass
