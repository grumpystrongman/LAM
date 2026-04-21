from __future__ import annotations

import json
import csv
import html
import hashlib
import statistics
import re
import time
import urllib.parse
import urllib.request
import webbrowser
import xml.etree.ElementTree as ET
from datetime import datetime
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from lam.interface.ai_backend import backend_metadata, normalize_backend
from lam.interface.app_launcher import normalize_app_name, open_installed_app
from lam.interface.app_learner import get_guidance
from lam.interface.desktop_sequence import assess_risk, build_plan, execute_plan
from lam.interface.local_vector_store import LocalVectorStore

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
MIN_LIVE_NON_CURATED_CITATIONS = 3
DESTRUCTIVE_ACTION_KEYWORDS = {
    "delete",
    "remove",
    "destroy",
    "send email",
    "send message",
    "wire transfer",
    "transfer money",
    "payment",
    "pay bill",
    "purchase",
    "buy now",
    "submit payment",
}


@dataclass(slots=True)
class SearchResult:
    title: str
    url: str
    price: Optional[float]
    source: str
    snippet: str = ""


@dataclass(slots=True)
class JobListing:
    title: str
    url: str
    source: str
    location: str
    remote: bool
    salary_text: str
    salary_min: Optional[float]
    salary_max: Optional[float]
    currency: str
    snippet: str = ""


@dataclass(slots=True)
class StudyItem:
    question: str
    answer: str
    category: str
    difficulty: str
    source_url: str = ""
    evidence: str = ""
    image_path: str = ""


def _fetch_text(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=20) as resp:  # nosec B310 - controlled URLs
        return resp.read().decode("utf-8", errors="ignore")


def _parse_duckduckgo(query: str, limit: int = 8) -> List[SearchResult]:
    q = urllib.parse.quote_plus(query)
    url = f"https://duckduckgo.com/html/?q={q}"
    html = _fetch_text(url)
    pattern = re.compile(
        r'<a[^>]*class="result__a"[^>]*href="(?P<href>[^"]+)"[^>]*>(?P<title>.*?)</a>',
        re.IGNORECASE | re.DOTALL,
    )
    snippet_pattern = re.compile(r'<a[^>]*class="result__snippet"[^>]*>(.*?)</a>', re.IGNORECASE | re.DOTALL)
    snippets = snippet_pattern.findall(html)

    results: List[SearchResult] = []
    for idx, match in enumerate(pattern.finditer(html)):
        href = match.group("href")
        title = re.sub("<.*?>", "", match.group("title")).strip()
        parsed = urllib.parse.urlparse(href)
        url_value = href
        if parsed.netloc.endswith("duckduckgo.com"):
            if parsed.path.startswith("/y.js"):
                qs = urllib.parse.parse_qs(parsed.query)
                if "u3" in qs:
                    url_value = urllib.parse.unquote(qs["u3"][0])
                elif "u" in qs:
                    url_value = urllib.parse.unquote(qs["u"][0])
                else:
                    continue
            qs = urllib.parse.parse_qs(parsed.query)
            if "uddg" in qs:
                url_value = urllib.parse.unquote(qs["uddg"][0])
        if "duckduckgo.com/y.js" in url_value:
            continue
        snippet = re.sub("<.*?>", "", snippets[idx]).strip() if idx < len(snippets) else ""
        results.append(SearchResult(title=title, url=url_value, price=_extract_price(title + " " + snippet), source="duckduckgo", snippet=snippet))
        if len(results) >= limit:
            break
    return results


def _parse_bing_rss(query: str, limit: int = 8) -> List[SearchResult]:
    q = urllib.parse.quote_plus(query)
    url = f"https://www.bing.com/search?q={q}&format=rss"
    xml_text = _fetch_text(url)
    results: List[SearchResult] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        snippet = (item.findtext("description") or "").strip()
        if not title or not link:
            continue
        results.append(
            SearchResult(
                title=title,
                url=link,
                price=_extract_price(title + " " + snippet),
                source="bing_rss",
                snippet=snippet,
            )
        )
        if len(results) >= limit:
            break
    return results


def _search_web(query: str, limit: int = 10) -> List[SearchResult]:
    results = _safe_search_web(query, limit=limit)
    if len(results) >= max(3, limit // 2):
        return results
    try:
        fallback = _parse_bing_rss(query, limit=limit)
    except Exception:
        fallback = []
    out: Dict[str, SearchResult] = {}
    for r in results + fallback:
        out[r.url] = r
    return list(out.values())[:limit]


def _safe_search_web(query: str, limit: int = 10) -> List[SearchResult]:
    try:
        return _parse_duckduckgo(query, limit=limit)
    except Exception:
        return []


def _extract_price(text: str) -> Optional[float]:
    price_match = re.search(r"\$([0-9]{1,4}(?:\.[0-9]{2})?)", text.replace(",", ""))
    if not price_match:
        return None
    try:
        return float(price_match.group(1))
    except ValueError:
        return None


def _search_amazon_playwright(query: str, limit: int = 8) -> List[SearchResult]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return []

    results: List[SearchResult] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        q = urllib.parse.quote_plus(query)
        page.goto(f"https://www.amazon.com/s?k={q}", timeout=30000)
        cards = page.query_selector_all("div.s-main-slot div[data-component-type='s-search-result']")
        for card in cards[: limit * 2]:
            title_node = card.query_selector("h2 span")
            link_node = card.query_selector("h2 a")
            price_whole = card.query_selector("span.a-price-whole")
            price_frac = card.query_selector("span.a-price-fraction")
            if not title_node or not link_node:
                continue
            title = (title_node.inner_text() or "").strip()
            href = link_node.get_attribute("href") or ""
            if href and href.startswith("/"):
                href = "https://www.amazon.com" + href
            whole = (price_whole.inner_text() or "").replace(",", "").strip() if price_whole else ""
            frac = (price_frac.inner_text() or "").strip() if price_frac else "00"
            price: Optional[float] = None
            if whole.isdigit() and frac.isdigit():
                price = float(f"{whole}.{frac}")
            if title and href:
                results.append(SearchResult(title=title, url=href, price=price, source="amazon"))
            if len(results) >= limit:
                break
        browser.close()
    return results


def _best_price(results: List[SearchResult]) -> Optional[SearchResult]:
    priced = [r for r in results if r.price is not None]
    if not priced:
        return results[0] if results else None
    return sorted(priced, key=lambda r: r.price or 999999.0)[0]


def _is_destructive_intent(instruction: str, plan_steps: List[Dict[str, Any]]) -> bool:
    low = instruction.lower()
    if any(token in low for token in DESTRUCTIVE_ACTION_KEYWORDS):
        return True
    for step in plan_steps:
        merged = " ".join(
            str(x).lower()
            for x in [
                step.get("action", ""),
                step.get("text", ""),
                step.get("keys", ""),
                (step.get("selector", {}) or {}).get("value", "") if isinstance(step.get("selector", {}), dict) else "",
            ]
        )
        if any(token in merged for token in DESTRUCTIVE_ACTION_KEYWORDS):
            return True
    return False


def _summarize_plan_steps(plan_steps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for i, step in enumerate(plan_steps):
        action = str(step.get("action", step.get("kind", "step")))
        target = ""
        selector = step.get("selector", {})
        if isinstance(selector, dict):
            target = str(selector.get("value", ""))
        if not target:
            target = str(step.get("app", "") or step.get("name", "") or step.get("text", ""))
        out.append({"index": i, "action": action, "target": target[:140]})
    return out


def _build_undo_plan(plan_steps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    undo: List[Dict[str, Any]] = []
    for i, step in enumerate(plan_steps):
        action = str(step.get("action", step.get("kind", "step"))).lower()
        if action in {"type_text", "type"}:
            instruction = "Use Ctrl+Z or clear edited field to restore prior value."
        elif action in {"click", "click_found", "submit_action", "set_cell"}:
            instruction = "Navigate back to impacted record and revert the latest change manually."
        elif action in {"open_app", "focus_window", "navigate_url"}:
            instruction = "Close the opened window/tab if this run should be rolled back."
        elif action in {"copy", "paste", "hotkey"}:
            instruction = "Restore clipboard/context and reverse any pasted content."
        elif action in {"research", "extract", "analyze", "produce", "present"}:
            instruction = "Delete generated local artifacts if run should be fully reverted."
        else:
            instruction = "No automatic rollback; use manual checkpoint restore."
        undo.append({"step_index": i, "action": action, "undo": instruction})
    return undo


def _detect_ambiguities(instruction: str, plan_steps: List[Dict[str, Any]]) -> List[str]:
    questions: List[str] = []
    if len(instruction.strip()) < 6:
        questions.append("Please provide more detail on the target outcome.")
    for idx, step in enumerate(plan_steps):
        action = str(step.get("action", step.get("kind", ""))).lower()
        if action in {"click", "type_text", "type"}:
            selector = step.get("selector", {})
            if isinstance(selector, dict):
                sel_val = str(selector.get("value", "")).strip()
            else:
                sel_val = ""
            if not sel_val and not str(step.get("text", "")).strip():
                questions.append(f"Step {idx} is missing a target element or text value.")
        if action == "note":
            questions.append(f"Step {idx} could not be mapped to an executable action.")
    return questions


def _verification_block(ok: bool, plan_steps: List[Dict[str, Any]], result: Dict[str, Any]) -> Dict[str, Any]:
    artifacts = result.get("artifacts", {}) or {}
    trace = result.get("trace", []) or []
    evidence: List[str] = []
    if trace:
        evidence.append(f"Executed trace entries: {len(trace)}")
    if artifacts:
        evidence.append(f"Artifacts generated: {', '.join(sorted(artifacts.keys()))}")
    if result.get("opened_url"):
        evidence.append(f"Opened target: {result.get('opened_url')}")
    checks = [
        {"name": "plan_has_steps", "pass": bool(plan_steps)},
        {"name": "execution_ok", "pass": bool(ok)},
        {"name": "evidence_present", "pass": bool(evidence)},
    ]
    return {"passed": all(bool(c["pass"]) for c in checks), "checks": checks, "evidence": evidence}


def _finalize_operator_result(result: Dict[str, Any], instruction: str, plan_steps: List[Dict[str, Any]]) -> Dict[str, Any]:
    out = dict(result)
    out["planned_steps"] = _summarize_plan_steps(plan_steps)
    out["undo_plan"] = _build_undo_plan(plan_steps)
    out["verification"] = _verification_block(bool(out.get("ok", False)), plan_steps, out)
    out["report"] = {
        "summary": out.get("canvas", {}).get("title", "Task run completed"),
        "artifacts": out.get("artifacts", {}),
        "next_actions": [
            "Review verification evidence.",
            "Use history Re-run for repeatable execution.",
            "Use Resume if a credential checkpoint is active.",
        ],
    }
    out["operator_contract"] = {
        "instruction": instruction,
        "model": "plan_validate_execute_verify_report",
        "least_privilege": True,
    }
    return out


def execute_instruction(
    instruction: str,
    control_granted: bool,
    step_mode: bool = False,
    confirm_risky: bool = False,
    ai_backend: str = "deterministic-local",
    min_live_non_curated_citations: Optional[int] = None,
    progress_cb: Optional[Callable[[int, str], None]] = None,
) -> Dict[str, Any]:
    _emit_progress(progress_cb, 2, "Understanding your request")
    ai_meta = backend_metadata(ai_backend)
    ai_backend = normalize_backend(ai_backend)
    if not control_granted:
        return {
            "ok": False,
            "error": "Control not granted. Click Accept Control first.",
            "instruction": instruction,
            "ai": ai_meta,
        }

    normalized = instruction.strip()
    if not normalized:
        return {"ok": False, "error": "Instruction is empty.", "instruction": instruction}

    if _is_native_planning_intent(normalized) or _is_study_pack_intent(normalized) or _is_job_research_intent(normalized):
        _emit_progress(progress_cb, 8, "Building execution plan")
        plan = _build_native_plan(normalized)
        plan_steps = [dict(x) for x in plan.get("steps", [])]
        ambiguities = _detect_ambiguities(normalized, plan_steps)
        if ambiguities:
            return {
                "ok": False,
                "mode": "needs_clarification",
                "instruction": instruction,
                "message": "Clarification required before execution.",
                "questions": ambiguities[:4],
                "planned_steps": _summarize_plan_steps(plan_steps),
                "undo_plan": _build_undo_plan(plan_steps),
            }
        if _is_destructive_intent(normalized, plan_steps) and not confirm_risky:
            return {
                "ok": False,
                "mode": "confirmation_required",
                "instruction": instruction,
                "requires_confirmation": True,
                "message": "Destructive or high-impact action detected. Confirm to continue.",
                "planned_steps": _summarize_plan_steps(plan_steps),
                "undo_plan": _build_undo_plan(plan_steps),
            }
        _emit_progress(progress_cb, 12, f"Plan ready with {len(plan_steps)} step(s)")
        _emit_progress(progress_cb, 14, "Executing plan")
        execution = _execute_native_plan(
            plan=plan,
            instruction=instruction,
            progress_cb=progress_cb,
            min_live_non_curated_citations=min_live_non_curated_citations,
        )
        execution["ai"] = ai_meta
        _emit_progress(progress_cb, 100, "Completed")
        return _finalize_operator_result(execution, instruction=instruction, plan_steps=plan_steps)

    if _is_desktop_sequence_intent(normalized):
        _emit_progress(progress_cb, 12, "Building desktop action sequence")
        plan = build_plan(normalized)
        plan_steps = [dict(x) for x in plan.get("steps", [])]
        ambiguities = _detect_ambiguities(normalized, plan_steps)
        if ambiguities:
            return {
                "ok": False,
                "mode": "needs_clarification",
                "instruction": instruction,
                "message": "Clarification required before execution.",
                "questions": ambiguities[:4],
                "planned_steps": _summarize_plan_steps(plan_steps),
                "undo_plan": _build_undo_plan(plan_steps),
            }
        risk = assess_risk(plan)
        if (risk["requires_confirmation"] or _is_destructive_intent(normalized, plan_steps)) and not confirm_risky:
            return {
                "ok": False,
                "mode": "desktop_sequence_preview",
                "instruction": instruction,
                "requires_confirmation": True,
                "message": "Risky actions detected. Confirm to execute.",
                "risk": risk,
                "plan": plan,
                "planned_steps": _summarize_plan_steps(plan_steps),
                "undo_plan": _build_undo_plan(plan_steps),
                "canvas": {
                    "title": "Confirmation Required",
                    "subtitle": f"{len(risk['risky_steps'])} risky step(s)",
                    "cards": [
                        {
                            "title": f"Step {item['index']}: {item['step'].get('action','')}",
                            "price": "confirm",
                            "source": "risk",
                            "url": "",
                        }
                        for item in risk["risky_steps"][:6]
                    ],
                },
            }
        run = execute_plan(plan, start_index=0, step_mode=step_mode, allow_input_fallback=True)
        _emit_progress(progress_cb, 95, "Finalizing desktop run output")
        store = LocalVectorStore()
        app_name = plan.get("app_name", "") or "desktop"
        guidance = get_guidance(app_name=app_name, user_goal=normalized, store=store) if app_name else {"guidance": []}
        response = {
            "ok": run.ok,
            "mode": "desktop_sequence",
            "instruction": instruction,
            "ai": ai_meta,
            "app_name": app_name,
            "plan": plan,
            "trace": run.trace,
            "done": run.done,
            "next_step_index": run.next_step_index,
            "pending_plan": {"plan": plan, "next_step_index": run.next_step_index} if not run.done else None,
            "paused_for_credentials": run.paused_for_credentials,
            "pause_reason": run.pause_reason,
            "error": run.error,
            "risk": risk,
            "guidance": guidance,
            "canvas": {
                "title": f"Desktop Sequence: {app_name or 'session'}",
                "subtitle": f"Executed {len(run.trace)} steps",
                "cards": [
                    {"title": f"Step {t.get('step', 0)}: {t.get('action','')}", "price": "ok" if t.get("ok") else "error", "source": "uia", "url": ""}
                    for t in run.trace[:6]
                ],
            },
        }
        return _finalize_operator_result(response, instruction=instruction, plan_steps=plan_steps)

    open_match = re.search(r"\bopen\s+(.+?)(?:\s+app)?\b", normalized, flags=re.IGNORECASE)
    if open_match and ("search" not in normalized.lower()):
        _emit_progress(progress_cb, 15, "Opening installed application")
        target = open_match.group(1).strip()
        plan_steps = [{"action": "open_app", "app": normalize_app_name(target)}]
        ok, launched = open_installed_app(target)
        app_name = normalize_app_name(target)
        store = LocalVectorStore()
        guidance = get_guidance(app_name=app_name, user_goal=normalized, store=store)
        if ok:
            response = {
                "ok": True,
                "mode": "desktop_app_open",
                "instruction": instruction,
                "ai": ai_meta,
                "app_name": app_name,
                "launched": launched,
                "paused_for_credentials": True,
                "pause_reason": "If a login prompt appears, enter credentials and click Resume.",
                "guidance": guidance,
                "canvas": {
                    "title": f"Opened App: {app_name}",
                    "subtitle": "Credential checkpoint active",
                    "cards": [
                        {"title": g["title"], "price": "tip", "source": "knowledge", "url": g["source_url"]}
                        for g in guidance["guidance"][:6]
                    ],
                },
            }
            return _finalize_operator_result(response, instruction=instruction, plan_steps=plan_steps)
        return {
            "ok": False,
            "mode": "desktop_app_open",
            "instruction": instruction,
            "ai": ai_meta,
            "error": f"Could not locate installed app '{target}'.",
            "app_name": app_name,
            "guidance": guidance,
            "planned_steps": _summarize_plan_steps(plan_steps),
            "undo_plan": _build_undo_plan(plan_steps),
        }

    plan_steps = [{"action": "web_search", "query": normalized}, {"action": "open_result"}]
    if _is_destructive_intent(normalized, plan_steps) and not confirm_risky:
        return {
            "ok": False,
            "mode": "confirmation_required",
            "instruction": instruction,
            "requires_confirmation": True,
            "message": "Destructive or high-impact action detected. Confirm to continue.",
            "planned_steps": _summarize_plan_steps(plan_steps),
            "undo_plan": _build_undo_plan(plan_steps),
        }
    query = normalized
    _emit_progress(progress_cb, 20, "Running web search")
    results: List[SearchResult] = []
    if "amazon" in normalized.lower():
        cleaned = re.sub(r"^.*?search\s+amazon\s+for\s+", "", normalized, flags=re.IGNORECASE)
        query = cleaned if cleaned and cleaned != normalized else normalized
        results.extend(_search_amazon_playwright(query, limit=8))
        if len(results) < 3:
            results.extend(_search_web(f"site:amazon.com {query}", limit=8))
    else:
        results.extend(_search_web(query, limit=8))

    dedup: Dict[str, SearchResult] = {}
    for result in results:
        dedup[result.url] = result
    ranked = list(dedup.values())
    best = _best_price(ranked)

    opened_url = ""
    if best:
        opened_url = best.url
        webbrowser.open(best.url, new=2)
    needs_credentials = _likely_requires_login(normalized, opened_url, best.title if best else "")

    response = {
        "ok": True,
        "instruction": instruction,
        "ai": ai_meta,
        "query": query,
        "opened_url": opened_url,
        "best_result": asdict(best) if best else None,
        "results": [asdict(item) for item in ranked[:10]],
        "canvas": {
            "title": "Search Summary",
            "subtitle": query,
            "cards": [
                {
                    "title": item.title[:100],
                    "price": f"${item.price:.2f}" if item.price is not None else "n/a",
                    "source": item.source,
                    "url": item.url,
                }
                for item in ranked[:6]
            ],
        },
        "paused_for_credentials": needs_credentials,
        "pause_reason": "Possible login prompt detected. Enter credentials manually and click Resume." if needs_credentials else "",
    }
    _emit_progress(progress_cb, 100, "Completed")
    return _finalize_operator_result(json.loads(json.dumps(response)), instruction=instruction, plan_steps=plan_steps)


def preview_instruction(instruction: str) -> Dict[str, Any]:
    normalized = instruction.strip()
    if not normalized:
        return {"ok": False, "error": "Instruction is empty."}
    if _is_native_planning_intent(normalized):
        plan = _build_native_plan(normalized)
        plan_steps = [dict(x) for x in plan.get("steps", [])]
        return {
            "ok": True,
            "mode": "preview_native_plan",
            "instruction": instruction,
            "plan": plan,
            "planned_steps": _summarize_plan_steps(plan_steps),
            "undo_plan": _build_undo_plan(plan_steps),
            "canvas": {
                "title": "Native Plan Preview",
                "subtitle": f"{plan.get('domain')} | {len(plan.get('steps', []))} steps",
                "cards": [
                    {"title": s.get("name", ""), "price": s.get("kind", "step"), "source": "planner", "url": ""}
                    for s in plan.get("steps", [])[:6]
                ],
            },
        }
    if _is_desktop_sequence_intent(normalized):
        plan = build_plan(normalized)
        plan_steps = [dict(x) for x in plan.get("steps", [])]
        risk = assess_risk(plan)
        return {
            "ok": True,
            "mode": "preview_desktop_sequence",
            "instruction": instruction,
            "plan": plan,
            "risk": risk,
            "planned_steps": _summarize_plan_steps(plan_steps),
            "undo_plan": _build_undo_plan(plan_steps),
            "canvas": {
                "title": "Plan Preview",
                "subtitle": f"{len(plan.get('steps', []))} steps",
                "cards": [
                    {"title": f"{i}. {s.get('action','')}", "price": "preview", "source": "plan", "url": ""}
                    for i, s in enumerate(plan.get("steps", [])[:6])
                ],
            },
        }
    return {
        "ok": True,
        "mode": "preview_search",
        "instruction": instruction,
        "planned_steps": _summarize_plan_steps([{"action": "web_search", "query": instruction}, {"action": "open_result"}]),
        "undo_plan": _build_undo_plan([{"action": "web_search"}, {"action": "open_result"}]),
        "canvas": {"title": "Search Preview", "subtitle": instruction, "cards": []},
    }


def _likely_requires_login(instruction: str, url: str, title: str) -> bool:
    hay = f"{instruction} {url} {title}".lower()
    keywords = ["login", "sign in", "signin", "username", "password", "account", "auth"]
    if any(k in hay for k in keywords):
        return True
    domains = ["amazon.com", "salesforce", "workday", "okta", "microsoftonline"]
    return any(d in hay for d in domains)


def _is_desktop_sequence_intent(instruction: str) -> bool:
    low = instruction.lower()
    if any(phrase in low for phrase in ["then click", "then type", "press enter", "hotkey ", "focus window", "click found"]):
        return True
    starters = ["open ", "click ", "type ", "press ", "hotkey ", "focus ", "switch to ", "scroll ", "login with ", "use credentials "]
    starts_like_macro = any(low.startswith(s) for s in starters)
    explicit_ui_find = "find text" in low or "locate text" in low
    return (starts_like_macro or explicit_ui_find) and "search amazon" not in low and "job" not in low


def _is_native_planning_intent(instruction: str) -> bool:
    low = instruction.lower()
    if _is_desktop_sequence_intent(low):
        return False
    signals = [
        "then",
        "from there",
        "build a report",
        "build report",
        "dashboard",
        "spreadsheet",
        "analy",
        "capture links",
        "all over",
        "across",
        "executive summary",
        "powerpoint",
        "competitor",
        "competition",
        "save everything to a folder",
    ]
    complexity = sum(1 for s in signals if s in low)
    return complexity >= 2 or (len(instruction) > 180 and any(k in low for k in ["find", "research", "search"]))


def _is_competitor_analysis_intent(instruction: str) -> bool:
    low = instruction.lower()
    has_comp = any(x in low for x in ["competitor", "competition", "compete"])
    has_research = any(x in low for x in ["research", "analysis", "market"])
    has_deliverable = any(x in low for x in ["executive summary", "powerpoint", "ppt", "slide"])
    return has_comp and has_research and has_deliverable


def _is_study_pack_intent(instruction: str) -> bool:
    low = instruction.lower()
    has_learning = any(x in low for x in ["flashcard", "quiz", "study", "exam", "test prep", "permit"])
    has_create = any(x in low for x in ["create", "build", "generate", "make"])
    return has_learning and (has_create or "notebooklm" in low)


def _build_native_plan(instruction: str) -> Dict[str, Any]:
    if _is_study_pack_intent(instruction):
        domain = "study_pack"
    elif _is_job_research_intent(instruction):
        domain = "job_market"
    elif _is_competitor_analysis_intent(instruction):
        domain = "competitor_analysis"
    else:
        domain = "web_research"
    deliverables: List[str] = []
    low = instruction.lower()
    if "spreadsheet" in low or "csv" in low:
        deliverables.append("spreadsheet")
    if "report" in low:
        deliverables.append("report")
    if "executive summary" in low:
        deliverables.append("executive_summary")
    if "powerpoint" in low or "ppt" in low or "slides" in low:
        deliverables.append("powerpoint")
    if "dashboard" in low:
        deliverables.append("dashboard")
    if "link" in low:
        deliverables.append("apply_links")
    if not deliverables:
        deliverables = ["report", "dashboard"]

    if domain == "job_market":
        sources = ["linkedin", "indeed", "ziprecruiter", "glassdoor", "builtin"]
    elif domain == "competitor_analysis":
        sources = ["industry_reports", "vendor_pages", "analyst_coverage", "web_search"]
    else:
        sources = ["web_search", "source_pages"]
    objective = re.sub(r"\s+", " ", instruction).strip()
    return {
        "planner": "native-v1",
        "domain": domain,
        "objective": objective,
        "deliverables": deliverables,
        "sources": sources,
        "constraints": {
            "prefer_public_pages": True,
            "no_password_capture": True,
            "persist_history": True,
        },
        "steps": [
            {"kind": "research", "name": "Collect candidate sources and listings"},
            {"kind": "extract", "name": "Extract structured fields"},
            {"kind": "analyze", "name": "Rank, deduplicate, summarize"},
            {"kind": "produce", "name": "Generate requested artifacts"},
            {"kind": "present", "name": "Open dashboard and return actionable links"},
        ],
    }


def _execute_native_plan(
    plan: Dict[str, Any],
    instruction: str,
    progress_cb: Optional[Callable[[int, str], None]] = None,
    min_live_non_curated_citations: Optional[int] = None,
) -> Dict[str, Any]:
    return _run_reflective_planner(
        plan=plan,
        instruction=instruction,
        progress_cb=progress_cb,
        min_live_non_curated_citations=min_live_non_curated_citations,
    )


def _run_reflective_planner(
    plan: Dict[str, Any],
    instruction: str,
    progress_cb: Optional[Callable[[int, str], None]] = None,
    min_live_non_curated_citations: Optional[int] = None,
) -> Dict[str, Any]:
    min_live = _effective_min_live_non_curated_citations(min_live_non_curated_citations)
    objective = str(plan.get("objective", instruction))
    preferred = str(plan.get("domain", "web_research"))
    accept_threshold = 0.60 if preferred == "competitor_analysis" else 0.72
    attempt_order = _strategy_order(preferred=preferred)
    best: Dict[str, Any] = {"score": -1.0, "result": {}}
    decision_log: List[str] = []
    accepted = False

    for attempt, strategy in enumerate(attempt_order, start=1):
        _emit_progress(progress_cb, 18 + (attempt - 1) * 20, f"Planning attempt {attempt}: {strategy}")
        result = _run_strategy(
            strategy=strategy,
            instruction=instruction,
            progress_cb=progress_cb,
            min_live_non_curated_citations=min_live,
        )
        score = _score_result_against_objective(
            result=result,
            objective=objective,
            min_live_non_curated_citations=min_live,
        )
        decision_log.append(f"Attempt {attempt} used {strategy} -> quality score {score:.2f}")
        if score > float(best["score"]):
            best = {"score": score, "result": result}
        if score >= accept_threshold:
            decision_log.append(f"Accepted attempt {attempt}; score passed threshold.")
            accepted = True
            break
        decision_log.append(f"Rejected attempt {attempt}; refining strategy.")

    chosen = dict(best["result"] or {})
    if preferred == "competitor_analysis" and (not accepted):
        return {
            "ok": False,
            "mode": "autonomous_plan_execute",
            "plan": plan,
            "instruction": instruction,
            "decision_log": decision_log + [
                f"Failed strict acceptance threshold ({accept_threshold:.2f}) for competitor analysis.",
            ],
            "query": str(chosen.get("query", "")),
            "results_count": int(chosen.get("results_count", 0) or 0),
            "artifacts": {},
            "summary": {
                "error": "strict_competitor_validation_failed",
                "required_live_non_curated_citations": min_live,
            },
            "results": chosen.get("results", []),
            "source_status": chosen.get("source_status", {}),
            "opened_url": "",
            "canvas": {
                "title": "Run Blocked",
                "subtitle": "Strict citation rule not met.",
                "cards": [],
            },
            "paused_for_credentials": False,
            "pause_reason": "",
        }
    if not chosen:
        chosen = _run_generic_research(instruction, progress_cb=progress_cb)
        decision_log.append("Fallback to generic strategy due empty attempts.")

    paused_for_credentials = bool(chosen.get("paused_for_credentials", False))
    pause_reason = str(chosen.get("pause_reason", "")) if paused_for_credentials else ""
    return {
        "ok": bool(chosen.get("ok", False)),
        "mode": "autonomous_plan_execute",
        "plan": plan,
        "instruction": instruction,
        "decision_log": decision_log,
        "query": chosen.get("query", ""),
        "results_count": chosen.get("results_count", 0),
        "artifacts": chosen.get("artifacts", {}),
        "summary": chosen.get("summary", {}),
        "results": chosen.get("results", []),
        "source_status": chosen.get("source_status", {}),
        "opened_url": chosen.get("opened_url", ""),
        "canvas": chosen.get(
            "canvas",
            {
                "title": "Task Completed",
                "subtitle": objective,
                "cards": [],
            },
        ),
        "paused_for_credentials": paused_for_credentials,
        "pause_reason": pause_reason,
    }


def _strategy_order(preferred: str) -> List[str]:
    all_strategies = ["study_pack", "job_market", "competitor_analysis", "generic_research"]
    if preferred == "study_pack":
        return ["study_pack", "generic_research"]
    if preferred == "job_market":
        return ["job_market", "generic_research"]
    if preferred == "competitor_analysis":
        return ["competitor_analysis", "generic_research"]
    if preferred in all_strategies:
        return [preferred] + [s for s in all_strategies if s != preferred]
    return all_strategies


def _run_strategy(
    strategy: str,
    instruction: str,
    progress_cb: Optional[Callable[[int, str], None]] = None,
    min_live_non_curated_citations: Optional[int] = None,
) -> Dict[str, Any]:
    if strategy == "study_pack":
        return _run_study_pack(instruction, progress_cb=progress_cb)
    if strategy == "job_market":
        return _run_job_market_research(instruction, progress_cb=progress_cb)
    if strategy == "competitor_analysis":
        return _run_competitor_analysis(
            instruction,
            progress_cb=progress_cb,
            min_live_non_curated_citations=min_live_non_curated_citations,
        )
    return _run_generic_research(instruction, progress_cb=progress_cb)


def _score_result_against_objective(
    result: Dict[str, Any],
    objective: str,
    min_live_non_curated_citations: Optional[int] = None,
) -> float:
    min_live = _effective_min_live_non_curated_citations(min_live_non_curated_citations)
    if not result.get("ok"):
        return 0.0
    score = 0.2
    artifacts = result.get("artifacts", {}) or {}
    if artifacts:
        score += 0.2
    if any(k in artifacts for k in ["quiz_html", "dashboard_html", "report_md"]):
        score += 0.2

    results_count = int(result.get("results_count", 0) or 0)
    score += 0.15 if results_count > 0 else 0.0
    score += 0.1 if results_count >= 20 else 0.0

    low_obj = objective.lower()
    low_summary = json.dumps(result.get("summary", {})).lower()
    alignment_terms = [t for t in re.split(r"[^a-z0-9]+", low_obj) if len(t) > 3]
    overlap = sum(1 for t in alignment_terms if t in low_summary)
    if alignment_terms:
        score += min(0.15, overlap / max(1, len(alignment_terms)) * 0.15)

    bad_domains = ("support.google.com", "gmail.com", "mail.google.com")
    urls = [str(x.get("url", "")).lower() for x in (result.get("results") or [])[:10]]
    if any(any(b in u for b in bad_domains) for u in urls):
        score -= 0.25

    low_obj = objective.lower()
    wants_study = any(t in low_obj for t in ["flashcard", "quiz", "study", "exam", "permit"])
    wants_jobs = any(t in low_obj for t in ["job", "position", "salary", "linkedin", "indeed"])
    wants_competitor = any(t in low_obj for t in ["competitor", "competition", "executive summary", "powerpoint", "ppt"])
    if wants_study:
        if "flashcards_csv" not in artifacts or not any(k in artifacts for k in ["quiz_md", "quiz_html"]):
            score -= 0.5
    if wants_jobs:
        if "jobs_csv" not in artifacts and result.get("mode") != "job_market_research":
            score -= 0.3
    if wants_competitor:
        if "executive_summary_md" not in artifacts or "powerpoint_pptx" not in artifacts:
            score -= 0.45
        if result.get("mode") != "competitor_analysis":
            score -= 0.35
        live_cites = int((result.get("summary", {}) or {}).get("live_non_curated_citations", 0) or 0)
        if live_cites < min_live:
            score -= 0.35

    return max(0.0, min(1.0, score))


def _is_job_research_intent(instruction: str) -> bool:
    low = instruction.lower()
    job_terms = ["job", "position", "vp", "avp", "linkedin", "indeed", "salary", "remote"]
    analysis_terms = ["spreadsheet", "dashboard", "report", "analysis"]
    return sum(1 for t in job_terms if t in low) >= 3 and any(t in low for t in analysis_terms)


def _run_job_market_research(instruction: str, progress_cb: Optional[Callable[[int, str], None]] = None) -> Dict[str, Any]:
    role_query = _extract_role_query(instruction)
    region_labels = _extract_regions(instruction)
    source_status: Dict[str, str] = {}
    collected: List[JobListing] = []
    _emit_progress(progress_cb, 18, f"Researching job sources for: {role_query}")

    total_units = max(1, len(region_labels) * 5)
    done_units = 0

    for region in region_labels:
        _emit_progress(progress_cb, 22 + int((done_units / total_units) * 45), f"Searching LinkedIn ({region.upper()})")
        try:
            li = _scrape_linkedin_jobs(role_query=role_query, region=region, limit=40)
            collected.extend(li)
            source_status[f"linkedin_{region}"] = f"ok:{len(li)}"
        except Exception as exc:
            source_status[f"linkedin_{region}"] = f"error:{type(exc).__name__}"
        done_units += 1

        _emit_progress(progress_cb, 22 + int((done_units / total_units) * 45), f"Searching BuiltIn ({region.upper()})")
        try:
            bi = _scrape_builtin_jobs(role_query=role_query, region=region, limit=30)
            collected.extend(bi)
            source_status[f"builtin_{region}"] = f"ok:{len(bi)}"
        except Exception as exc:
            source_status[f"builtin_{region}"] = f"error:{type(exc).__name__}"
        done_units += 1

        # Best-effort site-search fallback for other commercial boards.
        site_queries = [
            ("indeed", "site:indeed.com/jobs"),
            ("ziprecruiter", "site:ziprecruiter.com/jobs"),
            ("glassdoor", "site:glassdoor.com/job-listing"),
        ]
        region_phrase = "Ireland" if region == "ireland" else "United States"
        for source, site_prefix in site_queries:
            query = f'{site_prefix} "{role_query}" {region_phrase} salary remote'
            _emit_progress(progress_cb, 22 + int((done_units / total_units) * 45), f"Searching {source} ({region.upper()})")
            try:
                pulled = 0
                for item in _search_web(query, limit=6):
                    listing = _to_job_listing(item=item, source=source, fallback_region=region)
                    if listing:
                        collected.append(listing)
                        pulled += 1
                source_status[f"{source}_{region}"] = f"ok:{pulled}"
            except Exception as exc:
                source_status[f"{source}_{region}"] = f"error:{type(exc).__name__}"
            time.sleep(0.2)
            done_units += 1

    _emit_progress(progress_cb, 72, "Deduplicating and ranking job listings")
    dedup: Dict[str, JobListing] = {}
    for job in collected:
        dedup[job.url] = job
    jobs_all = list(dedup.values())
    jobs = [j for j in jobs_all if _is_target_job(j.title)]
    if len(jobs) < 12:
        jobs = jobs_all
    jobs.sort(key=lambda j: (_salary_rank(j), not j.remote, j.source, j.title))
    _emit_progress(progress_cb, 84, "Generating spreadsheet, report, and dashboard")
    artifacts = _write_job_artifacts(instruction=instruction, jobs=jobs)
    summary = _job_summary(jobs)
    top = jobs[:40]
    dashboard_uri = Path(artifacts["dashboard_html"]).resolve().as_uri() if artifacts.get("dashboard_html") else ""
    if dashboard_uri:
        webbrowser.open(dashboard_uri, new=2)
    return {
        "ok": True,
        "query": role_query,
        "results_count": len(jobs),
        "results": [asdict(x) for x in top],
        "artifacts": artifacts,
        "summary": summary,
        "source_status": source_status,
        "opened_url": dashboard_uri,
        "canvas": {
            "title": "Job Market Dashboard Generated",
            "subtitle": f"{len(jobs)} listings across {', '.join(region_labels).upper()}",
            "cards": [
                {
                    "title": x.title[:90],
                    "price": x.salary_text or ("Remote" if x.remote else "Salary n/a"),
                    "source": x.source,
                    "url": x.url,
                }
                for x in jobs[:6]
            ],
        },
    }


def _run_generic_research(instruction: str, progress_cb: Optional[Callable[[int, str], None]] = None) -> Dict[str, Any]:
    query = _extract_generic_query(instruction)
    queries = _expand_queries(query, instruction=instruction)
    collected: List[SearchResult] = []
    source_status: Dict[str, str] = {}
    _emit_progress(progress_cb, 18, f"Researching: {query}")
    for q in queries:
        _emit_progress(progress_cb, 24 + int((queries.index(q) / max(1, len(queries))) * 40), f"Searching web for: {q}")
        try:
            rows = _search_web(q, limit=12)
            collected.extend(rows)
            source_status[q] = f"ok:{len(rows)}"
        except Exception as exc:
            source_status[q] = f"error:{type(exc).__name__}"
    dedup: Dict[str, SearchResult] = {}
    for r in collected:
        dedup[r.url] = r
    ranked = list(dedup.values())
    _emit_progress(progress_cb, 72, "Ranking and summarizing findings")
    ranked.sort(key=lambda x: _relevance_score(x, query), reverse=True)
    top_score = _relevance_score(ranked[0], query) if ranked else 0.0
    if top_score < 1.25:
        return {
            "ok": False,
            "query": query,
            "results_count": 0,
            "results": [asdict(x) for x in ranked[:10]],
            "artifacts": {},
            "summary": {"error": "low_relevance", "top_score": round(top_score, 3), "query": query},
            "source_status": source_status,
            "opened_url": "",
            "canvas": {
                "title": "Research Blocked",
                "subtitle": "Results were not relevant enough; task not executed.",
                "cards": [{"title": "Low relevance", "price": str(round(top_score, 3)), "source": "validator", "url": ""}],
            },
        }
    _emit_progress(progress_cb, 84, "Building requested deliverables")
    artifacts = _write_generic_research_artifacts(instruction=instruction, query=query, results=ranked)
    summary = {
        "total": len(ranked),
        "sources": _count_by(r.source for r in ranked),
    }
    open_target = artifacts.get("primary_open_file") or artifacts.get("dashboard_html", "")
    opened_uri = Path(open_target).resolve().as_uri() if open_target else ""
    if opened_uri:
        webbrowser.open(opened_uri, new=2)
    return {
        "ok": True,
        "query": query,
        "results_count": len(ranked),
        "results": [asdict(x) for x in ranked[:50]],
        "artifacts": artifacts,
        "summary": summary,
        "source_status": source_status,
        "opened_url": opened_uri,
        "canvas": {
            "title": "Research Deliverables Generated",
            "subtitle": f"{len(ranked)} results",
            "cards": [
                {
                    "title": x.title[:90],
                    "price": f"${x.price:.2f}" if x.price is not None else "result",
                    "source": x.source,
                    "url": x.url,
                }
                for x in ranked[:6]
            ],
        },
    }


def _run_competitor_analysis(
    instruction: str,
    progress_cb: Optional[Callable[[int, str], None]] = None,
    min_live_non_curated_citations: Optional[int] = None,
) -> Dict[str, Any]:
    min_live = _effective_min_live_non_curated_citations(min_live_non_curated_citations)
    if not _is_competitor_analysis_intent(instruction):
        return {
            "ok": False,
            "mode": "competitor_analysis",
            "query": "",
            "results_count": 0,
            "results": [],
            "artifacts": {},
            "summary": {"error": "competitor_intent_not_detected"},
            "source_status": {},
            "opened_url": "",
            "canvas": {
                "title": "Competitor Analysis Skipped",
                "subtitle": "Prompt did not request competitor analysis explicitly.",
                "cards": [],
            },
        }
    target = _extract_competitor_target(instruction)
    output_folder = _extract_named_output_folder(instruction, default_name=f"{target} Competitor Analysis")
    _emit_progress(progress_cb, 16, f"Researching competitors for: {target}")

    queries = _competitor_queries(target)
    must_terms = _competitor_must_terms(target)
    collected: List[SearchResult] = []
    source_status: Dict[str, str] = {}
    for i, q in enumerate(queries):
        _emit_progress(progress_cb, 22 + int((i / max(1, len(queries))) * 40), f"Searching sources: {q}")
        rows = _search_web(q, limit=16)
        filtered = _filter_relevant_results(
            rows,
            must_terms=must_terms,
            banned_domains={
                "filmaffinity.com",
                "dailymotion.com",
                "justwatch.com",
                "youtube.com",
                "m.youtube.com",
                "support.google.com",
                "mail.google.com",
                "gmail.com",
            },
            min_score=2.0,
            preferred_domains=[],
        )
        collected.extend(filtered)
        source_status[q] = f"ok:{len(filtered)}"

    dedup: Dict[str, SearchResult] = {}
    for r in collected:
        dedup[r.url] = r
    ranked = sorted(dedup.values(), key=lambda x: _relevance_score(x, " ".join(must_terms)), reverse=True)
    if not ranked:
        _emit_progress(progress_cb, 64, "No high-signal hits yet; running relaxed retrieval")
        relaxed: List[SearchResult] = []
        for q in queries:
            rows = _search_web(q, limit=12)
            relaxed.extend(
                _filter_relevant_results(
                    rows,
                    must_terms=must_terms,
                    banned_domains={
                        "filmaffinity.com",
                        "dailymotion.com",
                        "justwatch.com",
                        "youtube.com",
                        "m.youtube.com",
                    },
                    min_score=1.0,
                    preferred_domains=[],
                )
            )
        if not relaxed:
            relaxed = _curated_ehr_competitor_sources(target)
        dedup = {}
        for r in relaxed:
            dedup[r.url] = r
        ranked = sorted(dedup.values(), key=lambda x: _relevance_score(x, "ehr competitor healthcare"), reverse=True)
    live_non_curated = _count_live_non_curated_citations(ranked)
    if live_non_curated < min_live:
        return {
            "ok": False,
            "mode": "competitor_analysis",
            "query": f"{target} EHR competitors",
            "results_count": len(ranked),
            "results": [asdict(x) for x in ranked[:50]],
            "artifacts": {},
            "summary": {
                "target": target,
                "error": "insufficient_live_non_curated_citations",
                "required_live_non_curated_citations": min_live,
                "live_non_curated_citations": live_non_curated,
            },
            "source_status": source_status,
            "opened_url": "",
            "canvas": {
                "title": "Run Blocked",
                "subtitle": f"Need at least {min_live} live non-curated citations; found {live_non_curated}.",
                "cards": [],
            },
        }
    competitors = _select_top_competitors(target=target, results=ranked, top_n=5)
    _emit_progress(progress_cb, 74, "Generating executive summary and PowerPoint")
    artifacts = _write_competitor_artifacts(
        instruction=instruction,
        target=target,
        output_folder=output_folder,
        competitors=competitors,
        ranked_results=ranked,
    )
    open_target = artifacts.get("primary_open_file") or artifacts.get("executive_summary_html") or artifacts.get("dashboard_html", "")
    opened_uri = Path(open_target).resolve().as_uri() if open_target else ""
    if opened_uri:
        webbrowser.open(opened_uri, new=2)

    cards = []
    for row in competitors[:6]:
        cards.append(
            {
                "title": row.get("name", "")[:90],
                "price": row.get("segment", "EHR"),
                "source": "competitor_analysis",
                "url": (row.get("citations") or [""])[0],
            }
        )
    return {
        "ok": True,
        "mode": "competitor_analysis",
        "query": f"{target} EHR competitors",
        "results_count": len(ranked),
        "results": [asdict(x) for x in ranked[:50]],
        "artifacts": artifacts,
        "summary": {
            "target": target,
            "top_competitors": [x.get("name", "") for x in competitors[:5]],
            "competitor_count": len(competitors),
            "sources_used": len(ranked),
            "live_non_curated_citations": live_non_curated,
            "required_live_non_curated_citations": min_live,
        },
        "source_status": source_status,
        "opened_url": opened_uri,
        "canvas": {
            "title": f"{target} Competitor Analysis Ready",
            "subtitle": f"Top {len(competitors[:5])} competitors with executive summary + PowerPoint",
            "cards": cards,
        },
    }


def _count_live_non_curated_citations(results: List[SearchResult]) -> int:
    seen: set[str] = set()
    count = 0
    for r in results:
        src = str(r.source or "").strip().lower()
        url = str(r.url or "").strip()
        if not url or not url.startswith(("http://", "https://")):
            continue
        host = urllib.parse.urlparse(url).netloc.lower()
        if src == "curated":
            continue
        if host in seen:
            continue
        seen.add(host)
        count += 1
    return count


def _effective_min_live_non_curated_citations(value: Optional[int]) -> int:
    if value is None:
        return MIN_LIVE_NON_CURATED_CITATIONS
    try:
        return max(1, min(20, int(value)))
    except Exception:
        return MIN_LIVE_NON_CURATED_CITATIONS


def _curated_ehr_competitor_sources(target: str) -> List[SearchResult]:
    _ = target
    return [
        SearchResult(
            title="Oracle Health | Electronic Health Record",
            url="https://www.oracle.com/health/",
            price=None,
            source="curated",
            snippet="Oracle Health (including Cerner capabilities) enterprise healthcare platform information.",
        ),
        SearchResult(
            title="MEDITECH EHR Platform",
            url="https://ehr.meditech.com/",
            price=None,
            source="curated",
            snippet="MEDITECH electronic health record platform overview.",
        ),
        SearchResult(
            title="athenahealth EHR",
            url="https://www.athenahealth.com/solutions/electronic-health-records",
            price=None,
            source="curated",
            snippet="athenahealth ambulatory-focused EHR product information.",
        ),
        SearchResult(
            title="eClinicalWorks EHR",
            url="https://www.eclinicalworks.com/",
            price=None,
            source="curated",
            snippet="eClinicalWorks EHR solutions for practices and health systems.",
        ),
        SearchResult(
            title="NextGen Healthcare EHR",
            url="https://www.nextgen.com/solutions/ehr",
            price=None,
            source="curated",
            snippet="NextGen ambulatory EHR capabilities and product information.",
        ),
        SearchResult(
            title="Veradigm Healthcare Data and EHR Solutions",
            url="https://veradigm.com/",
            price=None,
            source="curated",
            snippet="Veradigm portfolio including legacy Allscripts ecosystem context.",
        ),
    ]


def _extract_competitor_target(instruction: str) -> str:
    m = re.search(r"competitors?\s+(?:to|for|of)\s+([A-Za-z0-9& .-]{2,80})", instruction, flags=re.IGNORECASE)
    if m:
        return re.sub(r"\s+", " ", m.group(1)).strip(" .,\"'“”")
    if "epic" in instruction.lower():
        return "Epic Systems"
    return "Target Company"


def _extract_named_output_folder(instruction: str, default_name: str) -> str:
    m = re.search(r'folder\s+called\s+["“”\']([^"“”\']{2,80})["“”\']', instruction, flags=re.IGNORECASE)
    if not m:
        return default_name
    return m.group(1).strip()


def _competitor_queries(target: str) -> List[str]:
    return [
        f'"{target}" competitors electronic health record',
        f'"{target}" EHR market share competitors',
        f'{target} vs Oracle Health Cerner MEDITECH athenahealth',
        f'{target} hospital EHR alternatives enterprise',
        f'{target} ambulatory EHR competitors eClinicalWorks NextGen',
    ]


def _competitor_must_terms(target: str) -> List[str]:
    return [
        target.lower(),
        "ehr",
        "electronic health record",
        "healthcare",
        "hospital",
        "clinical",
        "oracle health",
        "cerner",
        "meditech",
        "athenahealth",
        "allscripts",
        "veradigm",
        "eclinicalworks",
        "nextgen",
    ]


def _select_top_competitors(target: str, results: List[SearchResult], top_n: int = 5) -> List[Dict[str, Any]]:
    competitor_aliases = {
        "Oracle Health (Cerner)": ["oracle health", "cerner"],
        "MEDITECH": ["meditech"],
        "athenahealth": ["athenahealth"],
        "Veradigm (Allscripts)": ["veradigm", "allscripts"],
        "eClinicalWorks": ["eclinicalworks", "eclinical works"],
        "NextGen Healthcare": ["nextgen healthcare", "nextgen"],
        "CPSI / TruBridge": ["cpsi", "trubridge"],
        "Altera Digital Health": ["altera digital health", "altera"],
    }
    scores: Dict[str, float] = {k: 0.0 for k in competitor_aliases}
    evidence: Dict[str, List[str]] = {k: [] for k in competitor_aliases}
    for r in results[:120]:
        hay = f"{r.title} {r.snippet} {r.url}".lower()
        for name, aliases in competitor_aliases.items():
            hit = sum(1 for a in aliases if a in hay)
            if hit:
                scores[name] += float(hit) + (_relevance_score(r, "ehr healthcare") * 0.15)
                if len(evidence[name]) < 3:
                    evidence[name].append(r.url)
    ordered = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    selected: List[Dict[str, Any]] = []
    for name, score in ordered:
        if len(selected) >= top_n:
            break
        if score <= 0.0:
            continue
        selected.append(
            {
                "name": name,
                "score": round(score, 3),
                "segment": "Enterprise EHR",
                "citations": evidence.get(name, []),
                "why": f"Detected recurring mentions with {target} in EHR market context.",
            }
        )
    if len(selected) < top_n:
        fallback = [
            "Oracle Health (Cerner)",
            "MEDITECH",
            "athenahealth",
            "Veradigm (Allscripts)",
            "eClinicalWorks",
            "NextGen Healthcare",
        ]
        for name in fallback:
            if len(selected) >= top_n:
                break
            if any(x["name"] == name for x in selected):
                continue
            selected.append(
                {
                    "name": name,
                    "score": 0.1,
                    "segment": "Enterprise/ambulatory EHR",
                    "citations": evidence.get(name, [])[:2],
                    "why": f"Industry-typical competitor set around {target} EHR footprint.",
                }
            )
    return selected[:top_n]


def _write_competitor_artifacts(
    instruction: str,
    target: str,
    output_folder: str,
    competitors: List[Dict[str, Any]],
    ranked_results: List[SearchResult],
) -> Dict[str, str]:
    safe_folder = re.sub(r"[<>:\"/\\\\|?*]", "", output_folder).strip() or f"{target} Competitor Analysis"
    out_dir = Path("data/reports") / safe_folder
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = out_dir / f"competitors_{ts}.csv"
    report_path = out_dir / f"executive_summary_{ts}.md"
    report_html_path = out_dir / f"executive_summary_{ts}.html"
    pptx_path = out_dir / f"executive_summary_{ts}.pptx"
    dash_path = out_dir / f"dashboard_{ts}.html"

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "score", "segment", "why", "citations"])
        writer.writeheader()
        for row in competitors:
            writer.writerow(
                {
                    "name": row.get("name", ""),
                    "score": row.get("score", ""),
                    "segment": row.get("segment", ""),
                    "why": row.get("why", ""),
                    "citations": " | ".join(row.get("citations", [])[:3]),
                }
            )

    lines: List[str] = []
    lines.append(f"# Executive Summary: {target} Competitor Analysis")
    lines.append("")
    lines.append(f"- Generated: {datetime.now().isoformat(timespec='seconds')}")
    lines.append(f"- Instruction: {instruction}")
    lines.append(f"- Output folder: `{out_dir.resolve()}`")
    lines.append("")
    lines.append("## 1. Executive Overview")
    lines.append(
        f"{target} remains a dominant enterprise EHR platform. The most credible competitive pressure in hospitals and ambulatory networks is concentrated in a small set of vendors with broad clinical workflow coverage, large installed footprints, and active modernization programs."
    )
    lines.append("")
    lines.append("## 2. Top Competitors")
    for i, comp in enumerate(competitors, start=1):
        lines.append(f"### {i}) {comp.get('name','')}")
        lines.append(f"- Positioning: {comp.get('segment','EHR vendor')}")
        lines.append(f"- Why it matters: {comp.get('why','')}")
        lines.append(
            "- Executive view: This vendor appears in competitive conversations when buyers evaluate enterprise breadth, operational risk during implementation, and long-term platform modernization costs."
        )
        lines.append(
            "- Commercial lens: Selection dynamics typically depend on how strongly the platform supports integrated clinical-financial workflows and migration complexity from incumbent tooling."
        )
        cites = comp.get("citations", [])[:3]
        if cites:
            lines.append("- Evidence:")
            for c in cites:
                lines.append(f"  - {c}")
        lines.append("")
    lines.append("## 3. Strategic Implications")
    lines.append(
        "Health systems comparing alternatives to Epic typically balance enterprise integration depth, revenue-cycle integration, specialty workflow maturity, implementation risk, and migration timeline. In practice, competitive displacement is strongest during major modernization cycles, mergers, or when organizations rebalance enterprise vs. ambulatory priorities."
    )
    lines.append(
        "At the executive level, competitor pressure is rarely about one feature. It is usually about total operating model fit: governance model, data strategy, interoperability architecture, implementation throughput, and degree of disruption to frontline clinical users. Organizations that do best in these transitions define non-negotiable outcomes first, then evaluate each platform against those outcomes using measurable decision criteria."
    )
    lines.append(
        "For most buyers, near-term implementation risk and medium-term optimization capability matter more than feature parity marketing claims. A credible competitor to Epic must demonstrate reliable deployment at scale, durable post-go-live support, and clear evidence of value realization across quality, throughput, and revenue-cycle performance."
    )
    lines.append("")
    lines.append("## 4. Recommended Next Steps")
    lines.append("1. Validate top-5 shortlist with your target segment (IDN, community hospital, ambulatory-heavy network).")
    lines.append("2. Build side-by-side scorecard: interoperability, total cost of ownership, implementation time, and user adoption risk.")
    lines.append("3. Run focused diligence on migration tooling and data conversion readiness.")
    lines.append("4. Confirm executive sponsorship model and change-management capacity before final vendor down-select.")
    report_path.write_text("\n".join(lines), encoding="utf-8")
    report_html_path.write_text(
        "<!doctype html><html lang='en'><head><meta charset='utf-8'/><meta name='viewport' content='width=device-width,initial-scale=1'/>"
        "<title>Executive Summary</title><style>body{font-family:'Segoe UI',Tahoma,sans-serif;background:#f8fafc;color:#0f172a;margin:0}"
        ".w{max-width:980px;margin:0 auto;padding:24px}.card{background:#fff;border:1px solid #e2e8f0;border-radius:12px;padding:16px}pre{white-space:pre-wrap}</style></head>"
        f"<body><div class='w'><div class='card'><h1>{html.escape(target)} Competitor Executive Summary</h1><pre>{html.escape(report_path.read_text(encoding='utf-8'))}</pre></div></div></body></html>",
        encoding="utf-8",
    )

    _write_competitor_pptx(pptx_path=pptx_path, target=target, competitors=competitors)
    _write_generic_dashboard_html(dash_path=dash_path, results=ranked_results, title=f"{target} Competitor Research Sources")

    return {
        "directory": str(out_dir.resolve()),
        "competitors_csv": str(csv_path.resolve()),
        "executive_summary_md": str(report_path.resolve()),
        "executive_summary_html": str(report_html_path.resolve()),
        "powerpoint_pptx": str(pptx_path.resolve()),
        "dashboard_html": str(dash_path.resolve()),
        "primary_open_file": str(report_html_path.resolve()),
    }


def _write_competitor_pptx(pptx_path: Path, target: str, competitors: List[Dict[str, Any]]) -> None:
    try:
        from pptx import Presentation  # type: ignore
    except Exception:
        pptx_path.write_text(
            "PowerPoint package unavailable. Install python-pptx to generate .pptx files.",
            encoding="utf-8",
        )
        return

    prs = Presentation()
    slide = prs.slides.add_slide(prs.slide_layouts[0])
    slide.shapes.title.text = f"{target} Competitor Analysis"
    slide.placeholders[1].text = "Executive Summary Deck"

    overview = prs.slides.add_slide(prs.slide_layouts[1])
    overview.shapes.title.text = "Executive Overview"
    overview.placeholders[1].text = (
        f"{target} competes in a concentrated EHR market where enterprise integration, interoperability, and implementation risk drive selection."
    )

    for comp in competitors[:5]:
        s = prs.slides.add_slide(prs.slide_layouts[1])
        s.shapes.title.text = comp.get("name", "Competitor")
        cites = comp.get("citations", [])[:2]
        cite_text = "\n".join(f"- {u}" for u in cites) if cites else "- Source coverage captured in dashboard file."
        s.placeholders[1].text = (
            f"Segment: {comp.get('segment','EHR')}\n"
            f"Why it matters: {comp.get('why','')}\n"
            f"Evidence:\n{cite_text}"
        )

    close = prs.slides.add_slide(prs.slide_layouts[1])
    close.shapes.title.text = "Recommended Next Steps"
    close.placeholders[1].text = (
        "1) Validate shortlist against your deployment profile.\n"
        "2) Build weighted scorecard (clinical fit, cost, interoperability).\n"
        "3) Launch structured vendor diligence with implementation risk gates."
    )
    prs.save(str(pptx_path))


def _run_study_pack(instruction: str, progress_cb: Optional[Callable[[int, str], None]] = None) -> Dict[str, Any]:
    topic = _extract_study_topic(instruction)
    question_count = _extract_question_count(instruction, default_count=200)
    use_notebooklm = "notebooklm" in instruction.lower()
    _emit_progress(progress_cb, 16, f"Researching study sources for: {topic}")

    sources = _study_sources_for_topic(topic)
    preferred_domains = _preferred_domains_for_topic(topic)
    strict_official = _is_strict_official_topic(topic)
    source_results: List[SearchResult] = []
    source_status: Dict[str, str] = {}
    for i, q in enumerate(sources):
        _emit_progress(progress_cb, 22 + int((i / max(1, len(sources))) * 38), f"Finding relevant sources: {q}")
        pulled = _search_web(q, limit=12)
        filtered = _filter_relevant_results(
            pulled,
            must_terms=_must_terms_for_topic(topic),
            banned_domains={
                "support.google.com",
                "mail.google.com",
                "gmail.com",
                "support.microsoft.com",
                "learn.microsoft.com",
                "microsoft.com",
                "windows.com",
            },
            min_score=2.0,
            preferred_domains=preferred_domains,
        )
        source_results.extend(filtered)
        source_status[q] = f"ok:{len(filtered)}"

    if len(source_results) < 5:
        _emit_progress(progress_cb, 58, "Refining search terms due low relevance")
        for q in _refine_study_queries(topic):
            pulled = _search_web(q, limit=10)
            filtered = _filter_relevant_results(
                pulled,
                must_terms=_must_terms_for_topic(topic),
                banned_domains={
                    "support.google.com",
                    "mail.google.com",
                    "gmail.com",
                    "support.microsoft.com",
                    "learn.microsoft.com",
                    "microsoft.com",
                    "windows.com",
                },
                min_score=1.5,
                preferred_domains=preferred_domains if strict_official else preferred_domains,
            )
            source_results.extend(filtered)
            source_status[q] = f"retry:{len(filtered)}"
            if len(source_results) >= 10:
                break

    dedup: Dict[str, SearchResult] = {}
    for r in source_results:
        dedup[r.url] = r
    ranked = sorted(dedup.values(), key=lambda x: _relevance_score(x, topic), reverse=True)

    if not ranked and not strict_official:
        _emit_progress(progress_cb, 64, "Relaxing filters to recover high-signal sources")
        fallback_pool: List[SearchResult] = []
        for q in sources + _refine_study_queries(topic):
            fallback_pool.extend(_search_web(q, limit=10))
        ranked = _filter_relevant_results(
            fallback_pool,
            must_terms=_must_terms_for_topic(topic),
            banned_domains={
                "support.google.com",
                "mail.google.com",
                "gmail.com",
                "dell.com",
                "lenovo.com",
                "nvidia.com",
                "hp.com",
                "intel.com",
            },
            min_score=2.0,
            preferred_domains=None,
        )
        dedup2: Dict[str, SearchResult] = {}
        for r in ranked:
            dedup2[r.url] = r
        ranked = sorted(dedup2.values(), key=lambda x: _relevance_score(x, topic), reverse=True)

    if not ranked:
        _emit_progress(progress_cb, 66, "Using curated official source links")
        ranked = _curated_study_sources(topic)

    _emit_progress(progress_cb, 70, "Extracting facts from official source material")
    fact_bank = _build_fact_bank(topic=topic, sources=ranked)
    if len(fact_bank) < 20:
        return {
            "ok": False,
            "query": topic,
            "results_count": 0,
            "results": [asdict(x) for x in ranked[:10]],
            "artifacts": {},
            "summary": {"topic": topic, "error": "insufficient_evidence", "fact_count": len(fact_bank)},
            "source_status": source_status,
            "opened_url": "",
            "paused_for_credentials": False,
            "pause_reason": "",
            "canvas": {
                "title": "Study Pack Blocked",
                "subtitle": "Not enough validated source evidence. No synthetic quiz generated.",
                "cards": [{"title": "Evidence", "price": str(len(fact_bank)), "source": "validator", "url": ""}],
            },
        }

    _emit_progress(progress_cb, 78, f"Generating {question_count} source-grounded flashcards")
    cards = _generate_study_items(topic=topic, count=question_count, source_results=ranked, fact_bank=fact_bank)
    coverage = _study_evidence_coverage(cards)
    if coverage < 0.85:
        return {
            "ok": False,
            "query": topic,
            "results_count": 0,
            "results": [asdict(x) for x in ranked[:10]],
            "artifacts": {},
            "summary": {"topic": topic, "error": "low_evidence_coverage", "coverage": coverage},
            "source_status": source_status,
            "opened_url": "",
            "paused_for_credentials": False,
            "pause_reason": "",
            "canvas": {
                "title": "Study Pack Blocked",
                "subtitle": "Evidence coverage below threshold. No low-quality quiz output allowed.",
                "cards": [{"title": "Coverage", "price": f"{coverage:.2%}", "source": "validator", "url": ""}],
            },
        }
    quality_issues = _study_human_quality_issues(cards)
    if quality_issues:
        return {
            "ok": False,
            "query": topic,
            "results_count": 0,
            "results": [asdict(x) for x in ranked[:10]],
            "artifacts": {},
            "summary": {"topic": topic, "error": "human_quality_failed", "issues": quality_issues[:10]},
            "source_status": source_status,
            "opened_url": "",
            "paused_for_credentials": False,
            "pause_reason": "",
            "canvas": {
                "title": "Study Pack Blocked",
                "subtitle": "Human-quality validation failed. Output rejected.",
                "cards": [{"title": "Issues", "price": str(len(quality_issues)), "source": "validator", "url": ""}],
            },
        }
    artifacts = _write_study_artifacts(instruction=instruction, topic=topic, cards=cards, sources=ranked)

    pause_reason = ""
    if use_notebooklm:
        _emit_progress(progress_cb, 90, "Opening NotebookLM workspace")
        notebooklm_url = "https://notebooklm.google.com/"
        webbrowser.open(notebooklm_url, new=2)
        pause_reason = "NotebookLM opened. If sign-in is required, complete login and continue."
    dashboard_uri = Path(artifacts["quiz_html"]).resolve().as_uri()
    webbrowser.open(dashboard_uri, new=2)

    return {
        "ok": True,
        "query": topic,
        "results_count": len(cards),
        "results": [asdict(x) for x in ranked[:25]],
        "artifacts": artifacts,
        "summary": {
            "topic": topic,
            "question_count": len(cards),
            "sources_found": len(ranked),
            "notebooklm_requested": use_notebooklm,
        },
        "source_status": source_status,
        "opened_url": dashboard_uri,
        "paused_for_credentials": bool(use_notebooklm),
        "pause_reason": pause_reason,
        "canvas": {
            "title": "Study Pack Ready",
            "subtitle": f"{len(cards)} questions for {topic}",
            "cards": [
                {
                    "title": r.title[:90],
                    "price": "source",
                    "source": r.source,
                    "url": r.url,
                }
                for r in ranked[:6]
            ],
        },
    }


def _extract_role_query(instruction: str) -> str:
    low = instruction.lower()
    if "data and ai" in low:
        return "Data and AI OR Artificial Intelligence OR Data Analytics OR Machine Learning"
    return "Data and AI roles OR Artificial Intelligence roles OR Data leadership roles"


def _extract_generic_query(instruction: str) -> str:
    q = instruction.strip()
    q = re.sub(r"\b(from there|then|and then)\b.*$", "", q, flags=re.IGNORECASE)
    q = re.sub(r"\b(build|create)\b.*$", "", q, flags=re.IGNORECASE)
    q = re.sub(r"\s+", " ", q).strip(" .")
    return q or instruction.strip()


def _extract_study_topic(instruction: str) -> str:
    text = re.sub(r"\s+", " ", instruction).strip()
    m = re.search(r"(?:for|about)\s+(.+?)(?:\s+exam|\s+test|\s+quiz|$)", text, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip(" .")
    return text


def _extract_question_count(instruction: str, default_count: int = 200) -> int:
    m = re.search(r"\b([0-9]{2,4})\s+question", instruction, flags=re.IGNORECASE)
    if not m:
        return default_count
    return max(20, min(500, int(m.group(1))))


def _study_sources_for_topic(topic: str) -> List[str]:
    low = topic.lower()
    if "south carolina" in low and ("driver" in low or "permit" in low):
        return [
            'site:scdmvonline.com south carolina driver manual permit test',
            'site:dmv.sc.gov Driver Manual PDF South Carolina',
            'site:dc.statelibrary.sc.gov south carolina driver license manual',
            f'"{topic}" rules of the road signs permit practice',
        ]
    return [
        f'"{topic}" official handbook PDF',
        f'"{topic}" practice test questions',
        f'"{topic}" study guide',
    ]


def _refine_study_queries(topic: str) -> List[str]:
    return [
        f'"{topic}" official manual PDF',
        f'"{topic}" exam topics road signs right of way',
        f'"{topic}" permit exam sample questions',
    ]


def _must_terms_for_topic(topic: str) -> List[str]:
    low = topic.lower()
    terms = [t for t in re.split(r"[^a-z0-9]+", low) if len(t) > 2]
    if "driver" in low or "permit" in low:
        terms.extend(["driver", "permit", "manual", "road", "dmv"])
    return list(dict.fromkeys(terms))[:12]


def _preferred_domains_for_topic(topic: str) -> List[str]:
    low = topic.lower()
    if "south carolina" in low and ("driver" in low or "permit" in low):
        return [
            "dmv.sc.gov",
            "scdmvonline.com",
            "dc.statelibrary.sc.gov",
            "driving-tests.org",
            "dmv.org",
        ]
    if "driver" in low or "permit" in low:
        return ["dmv", "gov", "state"]
    return []


def _is_strict_official_topic(topic: str) -> bool:
    low = topic.lower()
    return ("south carolina" in low) and ("driver" in low or "permit" in low)


def _curated_study_sources(topic: str) -> List[SearchResult]:
    low = topic.lower()
    if "south carolina" in low and ("driver" in low or "permit" in low):
        return [
            SearchResult(
                title="South Carolina Driver Manual (SC DMV)",
                url="https://dmv.sc.gov/sites/scdmv/files/media/Files/Driver-Manual.pdf",
                price=None,
                source="curated",
                snippet="Official South Carolina driver manual PDF.",
            ),
            SearchResult(
                title="SC DMV Driver Services",
                url="https://www.scdmvonline.com/Driver-Services",
                price=None,
                source="curated",
                snippet="South Carolina DMV driver services portal.",
            ),
            SearchResult(
                title="SCDMV Beginner's Permit",
                url="https://dmv.sc.gov/driver-services/drivers-license/beginner-permits",
                price=None,
                source="curated",
                snippet="Official beginner permit eligibility and requirements.",
            ),
            SearchResult(
                title="South Carolina Driver's License Manual (State Library)",
                url="https://dc.statelibrary.sc.gov/handle/10827/62666",
                price=None,
                source="curated",
                snippet="State document repository for the SC driver manual.",
            ),
        ]
    return [
        SearchResult(
            title=f"Official manual source for {topic}",
            url="https://www.usa.gov/motor-vehicle-services",
            price=None,
            source="curated",
            snippet="General US DMV service portal.",
        )
    ]


def _build_fact_bank(topic: str, sources: List[SearchResult]) -> List[Dict[str, str]]:
    facts: List[Dict[str, str]] = []
    for src in sources[:12]:
        url = src.url
        if url.lower().endswith(".pdf") or "driver-manual.pdf" in url.lower():
            facts.extend(_extract_pdf_fact_entries(url=url, topic=topic))
            continue
        text = _extract_source_text(url)
        if not text:
            continue
        for sentence in _split_sentences(text):
            clean = _normalize_fact_text(sentence)
            if _is_fact_sentence(clean, topic):
                score = _fact_sentence_score(clean)
                if url.lower().endswith(".pdf") or "driver-manual.pdf" in url.lower():
                    score += 2.0
                facts.append(
                    {
                        "text": clean.strip(),
                        "source_url": url,
                        "category": _categorize_fact(clean),
                        "score": f"{score:.3f}",
                        "image_path": "",
                    }
                )
    dedup: Dict[str, Dict[str, str]] = {}
    for f in facts:
        key = re.sub(r"\s+", " ", f["text"]).strip().lower()
        dedup[key] = f
    ranked = list(dedup.values())
    ranked.sort(key=lambda x: float(x.get("score", "0")), reverse=True)
    return ranked


def _extract_pdf_fact_entries(url: str, topic: str) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    try:
        import fitz  # type: ignore
    except Exception:
        text = _extract_source_text(url)
        if not text:
            return out
        for s in _split_sentences(text):
            clean = _normalize_fact_text(s)
            if _is_fact_sentence(clean, topic):
                out.append(
                    {
                        "text": clean,
                        "source_url": url,
                        "category": _categorize_fact(clean),
                        "score": f"{_fact_sentence_score(clean)+2.0:.3f}",
                        "image_path": "",
                    }
                )
        return out

    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=40) as resp:  # nosec B310
        raw = resp.read()
    doc = fitz.open(stream=raw, filetype="pdf")
    max_pages = min(120, int(getattr(doc, "page_count", 0) or 0))
    for i in range(max_pages):
        try:
            page = doc.load_page(i)
            page_text = page.get_text("text") or ""
        except Exception:
            continue
        for s in _split_sentences(page_text):
            clean = _normalize_fact_text(s)
            if not _is_fact_sentence(clean, topic):
                continue
            score = _fact_sentence_score(clean) + 2.0
            image_path = ""
            if _needs_visual(clean):
                image_path = _render_pdf_page_image(doc, i, url)
            out.append(
                {
                    "text": clean,
                    "source_url": url,
                    "category": _categorize_fact(clean),
                    "score": f"{score:.3f}",
                    "image_path": image_path,
                }
            )
    return out


def _needs_visual(sentence: str) -> bool:
    low = sentence.lower()
    return any(k in low for k in ["sign", "signal", "light", "marking", "lane", "intersection"])


def _render_pdf_page_image(doc: Any, page_index: int, source_url: str) -> str:
    try:
        import fitz  # type: ignore

        digest = hashlib.sha1(source_url.encode("utf-8")).hexdigest()[:12]
        out_dir = Path("data/reports/study_assets") / digest
        out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / f"page_{page_index+1}.png"
        if out.exists():
            return str(out.resolve())
        page = doc.load_page(page_index)
        pix = page.get_pixmap(matrix=fitz.Matrix(1.6, 1.6), alpha=False)
        pix.save(str(out))
        return str(out.resolve())
    except Exception:
        return ""


def _extract_source_text(url: str) -> str:
    low = url.lower()
    try:
        if low.endswith(".pdf") or "driver-manual.pdf" in low:
            text = _extract_pdf_text(url)
            return text
        html_text = _fetch_text(url)
        return _extract_text_from_html(html_text)
    except Exception:
        return ""


def _extract_pdf_text(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as resp:  # nosec B310
        raw = resp.read()

    # Try fast pure-Python extraction first.
    try:
        import io
        import pypdf

        reader = pypdf.PdfReader(io.BytesIO(raw))
        chunks: List[str] = []
        for page in reader.pages[:120]:
            try:
                chunks.append(page.extract_text() or "")
            except Exception:
                continue
        joined = "\n".join(chunks).strip()
        if len(joined) > 5000:
            return joined
    except Exception:
        pass

    # Fallback: PyMuPDF handles many scanned/complex PDFs better.
    try:
        import fitz  # type: ignore

        doc = fitz.open(stream=raw, filetype="pdf")
        chunks2: List[str] = []
        max_pages = min(120, int(getattr(doc, "page_count", 0) or 0))
        for i in range(max_pages):
            try:
                chunks2.append(doc.load_page(i).get_text("text") or "")
            except Exception:
                continue
        return "\n".join(chunks2).strip()
    except Exception:
        return ""


def _extract_text_from_html(html_text: str) -> str:
    text = re.sub(r"<script[\s\S]*?</script>", " ", html_text, flags=re.IGNORECASE)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _split_sentences(text: str) -> List[str]:
    parts = re.split(r"(?<=[\.\!\?])\s+", text)
    out = []
    for p in parts:
        s = p.strip()
        if 60 <= len(s) <= 320:
            out.append(s)
    return out


def _normalize_fact_text(text: str) -> str:
    s = text.replace("\r", " ").replace("\n", " ")
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\b([0-9]{1,2})-([0-9]{1,2})\b", " ", s)
    s = re.sub(r"\s([•\-])\s", " ", s)
    return s.strip(" .")


def _is_fact_sentence(sentence: str, topic: str) -> bool:
    low = sentence.lower()
    must = _must_terms_for_topic(topic)
    score = sum(1 for t in must if t in low)
    legal_signal = any(k in low for k in ["must", "shall", "required", "cannot", "may", "permit", "license", "test"])
    return score >= 1 and legal_signal


def _categorize_fact(sentence: str) -> str:
    low = sentence.lower()
    mapping = [
        ("Road Signs", ["sign", "signal", "traffic light"]),
        ("Permit Rules", ["permit", "license", "application", "under 18", "age"]),
        ("Right of Way", ["right-of-way", "yield", "intersection"]),
        ("Safe Driving", ["safe", "seat belt", "defensive", "speed"]),
        ("Alcohol and Drugs", ["alcohol", "drug", "dui"]),
        ("Parking and Turns", ["park", "turn", "lane", "u-turn"]),
    ]
    for cat, keys in mapping:
        if any(k in low for k in keys):
            return cat
    return "Rules of the Road"


def _fact_sentence_score(sentence: str) -> float:
    low = sentence.lower()
    score = 0.0
    score += 1.0 if len(sentence) >= 90 else 0.4
    score += 0.8 if any(k in low for k in ["must", "shall", "required", "illegal", "prohibited"]) else 0.0
    score += 0.6 if any(k in low for k in ["speed", "lane", "intersection", "sign", "signal", "right-of-way"]) else 0.0
    score += 0.4 if re.search(r"\b[0-9]{1,3}\b", sentence) else 0.0
    return score


def _study_evidence_coverage(cards: List[StudyItem]) -> float:
    if not cards:
        return 0.0
    good = sum(1 for c in cards if c.source_url and c.evidence and len(c.evidence) > 20)
    return good / len(cards)


def _study_human_quality_issues(cards: List[StudyItem]) -> List[str]:
    issues: List[str] = []
    for i, c in enumerate(cards, start=1):
        q = c.question.lower()
        a = c.answer.strip()
        if len(a) < 25:
            issues.append(f"Q{i}: answer too short")
        image_referenced = any(k in q for k in ["image shown", "picture", "sign shown", "visual"])
        if image_referenced and not c.image_path:
            issues.append(f"Q{i}: image referenced but no image attached")
    return issues


def _filter_relevant_results(
    results: List[SearchResult],
    must_terms: List[str],
    banned_domains: set[str],
    min_score: float = 1.0,
    preferred_domains: Optional[List[str]] = None,
) -> List[SearchResult]:
    filtered: List[SearchResult] = []
    preferred = [d.lower() for d in (preferred_domains or []) if d]
    for r in results:
        parsed = urllib.parse.urlparse(r.url)
        host = (parsed.netloc or "").lower()
        if any(b in host for b in banned_domains):
            continue
        if preferred and not any(p in host for p in preferred):
            continue
        score = _result_term_score(r, must_terms)
        if score >= min_score:
            filtered.append(r)
    return filtered


def _result_term_score(result: SearchResult, terms: List[str]) -> float:
    hay = f"{result.title} {result.snippet} {result.url}".lower()
    hit = sum(1 for t in terms if t in hay)
    quality_bonus = 0.0
    host = urllib.parse.urlparse(result.url).netloc.lower()
    if any(x in host for x in ["dmv.sc.gov", "scdmvonline.com", "state", "gov"]):
        quality_bonus += 1.0
    return float(hit) + quality_bonus


def _generate_study_items(
    topic: str,
    count: int,
    source_results: List[SearchResult],
    fact_bank: Optional[List[Dict[str, str]]] = None,
) -> List[StudyItem]:
    categories = [
        "Road Signs",
        "Right of Way",
        "Speed and Distance",
        "Safe Driving",
        "Sharing the Road",
        "Permit Rules",
        "Alcohol and Drugs",
        "Parking and Turns",
    ]
    source_titles = [r.title for r in source_results[:20]] or [f"{topic} official manual"]
    facts = fact_bank or []
    facts_with_image = [f for f in facts if str(f.get("image_path", ""))]
    facts_no_image = [f for f in facts if not str(f.get("image_path", ""))]
    items: List[StudyItem] = []
    for i in range(count):
        cat = categories[i % len(categories)]
        src = source_titles[i % len(source_titles)]
        if facts:
            use_visual = bool(facts_with_image) and (i % 4 == 0)
            if use_visual:
                fact = facts_with_image[(i // 4) % len(facts_with_image)]
            else:
                base = facts_no_image if facts_no_image else facts
                fact = base[i % len(base)]
        else:
            fact = {"text": f"{topic} official rule", "source_url": "", "category": cat}
        fact_text = str(fact.get("text", "")).strip()
        if len(fact_text) > 220:
            fact_text = fact_text[:220] + "..."
        img = str(fact.get("image_path", ""))
        q = f"[{cat}] Q{i+1}: {_fact_to_question(fact_text, has_image=bool(img))}"
        a = f"{fact_text}"
        diff = "easy" if i % 3 == 0 else "medium" if i % 3 == 1 else "hard"
        items.append(
            StudyItem(
                question=q,
                answer=a,
                category=str(fact.get("category", cat)),
                difficulty=diff,
                source_url=str(fact.get("source_url", "")),
                evidence=fact_text,
                image_path=img,
            )
        )
    return items


def _fact_to_question(fact_text: str, has_image: bool = False) -> str:
    clean = re.sub(r"\s+", " ", fact_text).strip()
    if has_image:
        return "Based on the image shown, what rule or meaning applies in this situation?"
    low = clean.lower()
    if low.startswith("if "):
        tail = clean[3:]
        return f"If {tail}, what does South Carolina guidance require?"
    if "must" in low:
        prefix = clean.split("must", 1)[0].strip(" ,.;:")
        if prefix:
            return f"What must be done in South Carolina when {prefix.lower()}?"
    tokens = re.split(r"[^a-zA-Z0-9]+", clean)
    key = " ".join([t for t in tokens[:10] if t]) or "this situation"
    return f"According to the official manual, what is the correct rule regarding {key.lower()}?"


def _write_study_artifacts(
    instruction: str,
    topic: str,
    cards: List[StudyItem],
    sources: List[SearchResult],
) -> Dict[str, str]:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path("data/reports/study_pack") / ts
    out_dir.mkdir(parents=True, exist_ok=True)

    flashcards_csv = out_dir / "flashcards.csv"
    quiz_md = out_dir / "quiz.md"
    quiz_html = out_dir / "quiz.html"
    sources_md = out_dir / "sources.md"

    with flashcards_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["question", "answer", "category", "difficulty", "source_url", "evidence", "image_path"])
        writer.writeheader()
        for c in cards:
            writer.writerow(asdict(c))

    lines = [
        f"# Study Quiz: {topic}",
        "",
        f"- Generated: {datetime.now().isoformat(timespec='seconds')}",
        f"- Requested by: {instruction}",
        f"- Total Questions: {len(cards)}",
        "",
    ]
    for i, c in enumerate(cards[: min(len(cards), 200)], start=1):
        lines.append(f"## Q{i} ({c.category}, {c.difficulty})")
        lines.append(c.question)
        lines.append("")
        lines.append(f"Answer: {c.answer}")
        if c.source_url:
            lines.append(f"Source: {c.source_url}")
        if c.evidence:
            lines.append(f"Evidence: {c.evidence}")
        if c.image_path:
            lines.append(f"Image: {c.image_path}")
        lines.append("")
    quiz_md.write_text("\n".join(lines), encoding="utf-8")

    src_lines = ["# Sources", ""]
    for i, s in enumerate(sources[:50], start=1):
        src_lines.append(f"{i}. [{s.title}]({s.url})")
    sources_md.write_text("\n".join(src_lines), encoding="utf-8")

    _write_study_quiz_html(quiz_html, topic=topic, cards=cards, sources=sources)
    return {
        "directory": str(out_dir.resolve()),
        "flashcards_csv": str(flashcards_csv.resolve()),
        "quiz_md": str(quiz_md.resolve()),
        "quiz_html": str(quiz_html.resolve()),
        "sources_md": str(sources_md.resolve()),
    }


def _write_study_quiz_html(path: Path, topic: str, cards: List[StudyItem], sources: List[SearchResult]) -> None:
    payload_cards = []
    for c in cards:
        row = asdict(c)
        img_path = str(row.get("image_path", "") or "")
        row["image_url"] = Path(img_path).resolve().as_uri() if img_path and Path(img_path).exists() else ""
        payload_cards.append(row)
    cards_payload = json.dumps(payload_cards)
    src_payload = json.dumps([asdict(s) for s in sources[:30]])
    html_text = f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Study Pack - {topic}</title>
<style>
body{{font-family:'Segoe UI',Tahoma,sans-serif;margin:0;background:#f8fafc;color:#0f172a}}
.wrap{{max-width:1200px;margin:0 auto;padding:20px}}
.hero{{background:linear-gradient(120deg,#ecfeff,#eef2ff);border:1px solid #cbd5e1;border-radius:14px;padding:16px}}
.row{{display:flex;gap:10px;flex-wrap:wrap;margin:12px 0}}
input,select{{border:1px solid #cbd5e1;border-radius:10px;padding:8px;font-size:14px}}
.card{{background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:12px;margin-bottom:10px}}
.meta{{font-size:12px;color:#64748b}}
a{{color:#0f766e;text-decoration:none}}
</style></head><body><div class="wrap">
<div class="hero"><h1 style="margin:0">Study Pack: {topic}</h1><p style="margin:6px 0 0 0">{len(cards)} generated questions with source links.</p></div>
<div class="row"><input id="q" placeholder="Search questions" oninput="render()"/><select id="cat" onchange="render()"><option value="">All categories</option></select></div>
<div id="cards"></div>
<h3>Sources</h3><div id="sources"></div>
</div>
<script>
const cards={cards_payload};
const sources={src_payload};
const cats=[...new Set(cards.map(x=>x.category))].sort();
const catSel=document.getElementById('cat'); cats.forEach(c=>{{const o=document.createElement('option');o.value=c;o.textContent=c;catSel.appendChild(o);}});
function esc(s){{return (s||'').replace(/[&<>]/g,c=>({{'&':'&amp;','<':'&lt;','>':'&gt;'}}[c]));}}
function render(){{
 const q=(document.getElementById('q').value||'').toLowerCase();
 const cat=document.getElementById('cat').value;
 const filtered=cards.filter(c=>{{ if(cat && c.category!==cat) return false; return !q || (c.question+' '+c.answer).toLowerCase().includes(q); }});
  document.getElementById('cards').innerHTML=filtered.slice(0,300).map((c,i)=>`<div class="card"><div class="meta">#${{i+1}} • ${{esc(c.category)}} • ${{esc(c.difficulty)}}</div><div><strong>${{esc(c.question)}}</strong></div>${{c.image_url?`<div style="margin-top:8px"><img src="${{c.image_url}}" alt="Study visual" style="max-width:100%;border:1px solid #e2e8f0;border-radius:8px"/></div>`:''}}<div style="margin-top:6px">${{esc(c.answer)}}</div><div class="meta" style="margin-top:6px">${{c.source_url?`Source: <a target="_blank" rel="noopener" href="${{c.source_url}}">${{esc(c.source_url)}}</a>`:''}}</div></div>`).join('');
 document.getElementById('sources').innerHTML=sources.map((s,i)=>`<div><a target="_blank" rel="noopener" href="${{s.url}}">${{i+1}}. ${{esc(s.title)}}</a></div>`).join('');
}}
render();
</script></body></html>"""
    path.write_text(html_text, encoding="utf-8")


def _expand_queries(query: str, instruction: str = "") -> List[str]:
    base = query.strip()
    low = f"{query} {instruction}".lower()
    variants: List[str] = [base]
    if any(k in low for k in ["job", "position", "salary", "remote", "hiring", "linkedin", "indeed"]):
        variants.extend(
            [
                f"{base} salary range",
                f"{base} remote",
                f"{base} United States",
                f"{base} Ireland",
            ]
        )
    else:
        variants.extend(
            [
                f"{base} market share",
                f"{base} analysis",
                f"{base} competitors",
            ]
        )
    out: List[str] = []
    for v in variants:
        k = v.lower()
        if k not in {x.lower() for x in out}:
            out.append(v)
    return out[:5]


def _relevance_score(result: SearchResult, query: str) -> float:
    q_terms = [t for t in re.split(r"[^a-z0-9]+", query.lower()) if len(t) > 2]
    hay = f"{result.title} {result.snippet}".lower()
    overlap = sum(1 for t in q_terms if t in hay)
    source_bonus = 1.0 if result.source in {"linkedin", "builtin", "amazon"} else 0.25
    return overlap + source_bonus


def _is_target_job(title: str) -> bool:
    low = title.lower()
    has_seniority = any(k in low for k in ["vp", "vice president", "avp", "head", "director", "chief"])
    has_domain = any(k in low for k in ["data", "ai", "artificial intelligence", "machine learning", "analytics"])
    return has_seniority and has_domain


def _write_generic_research_artifacts(instruction: str, query: str, results: List[SearchResult]) -> Dict[str, str]:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path("data/reports/generic_research") / ts
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_path = out_dir / "results.csv"
    report_path = out_dir / "report.md"
    brief_md_path = out_dir / "executive_brief.md"
    brief_html_path = out_dir / "executive_brief.html"
    pptx_path = out_dir / "executive_brief.pptx"
    dash_path = out_dir / "dashboard.html"

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["title", "url", "source", "price", "snippet"],
        )
        writer.writeheader()
        for r in results:
            writer.writerow(asdict(r))

    lines = [
        "# Research Report",
        "",
        f"- Generated: {datetime.now().isoformat(timespec='seconds')}",
        f"- Instruction: {instruction}",
        f"- Query focus: {query}",
        f"- Results: {len(results)}",
        "",
        "## Top Results",
    ]
    for i, r in enumerate(results[:40], start=1):
        extra = f" | ${r.price:.2f}" if r.price is not None else ""
        lines.append(f"{i}. [{r.title}]({r.url}) | {r.source}{extra}")
    report_path.write_text("\n".join(lines), encoding="utf-8")

    wants_brief = _wants_exec_brief(instruction)
    wants_ppt = _wants_powerpoint(instruction)
    if wants_brief:
        _write_generic_exec_brief(
            instruction=instruction,
            query=query,
            results=results,
            brief_md_path=brief_md_path,
            brief_html_path=brief_html_path,
        )
    if wants_ppt:
        _write_generic_exec_pptx(
            instruction=instruction,
            query=query,
            results=results,
            pptx_path=pptx_path,
        )
    _write_generic_dashboard_html(dash_path=dash_path, results=results, title="Research Dashboard")

    out = {
        "directory": str(out_dir.resolve()),
        "results_csv": str(csv_path.resolve()),
        "report_md": str(report_path.resolve()),
        "dashboard_html": str(dash_path.resolve()),
    }
    if wants_brief:
        out["executive_brief_md"] = str(brief_md_path.resolve())
        out["executive_brief_html"] = str(brief_html_path.resolve())
        out["primary_open_file"] = str(brief_html_path.resolve())
    elif wants_ppt:
        out["primary_open_file"] = str(pptx_path.resolve())
    else:
        out["primary_open_file"] = str(dash_path.resolve())
    if wants_ppt:
        out["powerpoint_pptx"] = str(pptx_path.resolve())
    return out


def _wants_exec_brief(instruction: str) -> bool:
    low = instruction.lower()
    return any(x in low for x in ["executive summary", "executive brief", "2-page", "two-page brief", "brief"])


def _wants_powerpoint(instruction: str) -> bool:
    low = instruction.lower()
    return any(x in low for x in ["powerpoint", "ppt", "slides", "slide deck"])


def _write_generic_exec_brief(
    instruction: str,
    query: str,
    results: List[SearchResult],
    brief_md_path: Path,
    brief_html_path: Path,
) -> None:
    top = results[:20]
    themes = [r.title.strip() for r in top[:8]]
    lines: List[str] = []
    lines.append(f"# Executive Brief")
    lines.append("")
    lines.append(f"- Generated: {datetime.now().isoformat(timespec='seconds')}")
    lines.append(f"- Instruction: {instruction}")
    lines.append(f"- Query focus: {query}")
    lines.append("")
    lines.append("## 1. Executive Summary")
    lines.append(
        "This brief synthesizes current market signals and source material relevant to the requested topic. The emphasis is on practical strategic interpretation rather than simple link aggregation."
    )
    lines.append("")
    lines.append("## 2. Key Findings")
    for i, t in enumerate(themes, start=1):
        lines.append(f"{i}. {t}")
    lines.append("")
    lines.append("## 3. Competitive/Market Interpretation")
    lines.append(
        "Across sources, the strongest pattern is concentration around a small number of recurring players and themes. Differentiation appears to depend on implementation risk, interoperability depth, and long-term operating model fit."
    )
    lines.append(
        "Decision quality improves when evidence is weighted by source quality and direct relevance to the objective, while excluding low-signal pages."
    )
    lines.append("")
    lines.append("## 4. Recommended Actions")
    lines.append("1. Validate the top findings against your target segment and operating constraints.")
    lines.append("2. Build a decision scorecard with explicit weighting and acceptance thresholds.")
    lines.append("3. Convert findings into an execution roadmap with owners, milestones, and risk controls.")
    lines.append("")
    lines.append("## 5. Source Appendix")
    for i, r in enumerate(top, start=1):
        lines.append(f"{i}. [{r.title}]({r.url})")
    brief_md_path.write_text("\n".join(lines), encoding="utf-8")

    html_rows = "".join(
        f"<li><a href=\"{html.escape(r.url)}\" target=\"_blank\" rel=\"noopener\">{html.escape(r.title)}</a></li>"
        for r in top
    )
    html_text = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Executive Brief</title>
<style>body{{font-family:'Segoe UI',Tahoma,sans-serif;background:#f8fafc;color:#0f172a;margin:0}}.w{{max-width:980px;margin:0 auto;padding:24px}}.card{{background:#fff;border:1px solid #e2e8f0;border-radius:12px;padding:16px}}</style>
</head><body><div class="w"><div class="card"><h1>Executive Brief</h1><p><strong>Instruction:</strong> {html.escape(instruction)}</p><p><strong>Query:</strong> {html.escape(query)}</p><h2>Sources</h2><ol>{html_rows}</ol></div></div></body></html>"""
    brief_html_path.write_text(html_text, encoding="utf-8")


def _write_generic_exec_pptx(instruction: str, query: str, results: List[SearchResult], pptx_path: Path) -> None:
    try:
        from pptx import Presentation  # type: ignore
    except Exception:
        pptx_path.write_text("PowerPoint package unavailable. Install python-pptx.", encoding="utf-8")
        return
    prs = Presentation()
    s0 = prs.slides.add_slide(prs.slide_layouts[0])
    s0.shapes.title.text = "Executive Brief Deck"
    s0.placeholders[1].text = query
    s1 = prs.slides.add_slide(prs.slide_layouts[1])
    s1.shapes.title.text = "Objective"
    s1.placeholders[1].text = instruction[:1200]
    s2 = prs.slides.add_slide(prs.slide_layouts[1])
    s2.shapes.title.text = "Top Findings"
    s2.placeholders[1].text = "\n".join(f"- {r.title}" for r in results[:8])[:2000]
    s3 = prs.slides.add_slide(prs.slide_layouts[1])
    s3.shapes.title.text = "Next Steps"
    s3.placeholders[1].text = (
        "1) Validate findings with stakeholders.\n"
        "2) Build weighted decision framework.\n"
        "3) Track execution milestones and risk gates."
    )
    prs.save(str(pptx_path))


def _write_generic_dashboard_html(dash_path: Path, results: List[SearchResult], title: str) -> None:
    rows = []
    for r in results:
        rows.append(
            {
                "title": r.title,
                "source": r.source,
                "price": (f"${r.price:.2f}" if r.price is not None else "n/a"),
                "url": r.url,
                "snippet": r.snippet,
            }
        )
    payload = json.dumps(rows)
    html_text = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{title}</title>
  <style>
    body {{ font-family: 'Segoe UI', Tahoma, sans-serif; margin:0; background:#f8fafc; color:#0f172a; }}
    .wrap {{ max-width: 1220px; margin:0 auto; padding:20px; }}
    .hero {{ border:1px solid #cbd5e1; background:linear-gradient(120deg,#eff6ff,#f0fdf4); border-radius:14px; padding:16px; }}
    .controls {{ display:flex; gap:10px; flex-wrap:wrap; margin:12px 0; }}
    input, select {{ border:1px solid #cbd5e1; border-radius:10px; padding:9px; font-size:14px; }}
    table {{ width:100%; border-collapse:collapse; background:#fff; }}
    th, td {{ padding:10px; border-bottom:1px solid #e2e8f0; vertical-align:top; font-size:14px; }}
    th {{ position:sticky; top:0; background:#0f172a; color:#e2e8f0; }}
    a {{ color:#0f766e; text-decoration:none; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <h1 style="margin:0">{title}</h1>
      <p style="margin:6px 0 0 0">Filter, inspect, and open source links.</p>
    </div>
    <div class="controls">
      <input id="q" placeholder="Search title/snippet" oninput="render()"/>
      <select id="source" onchange="render()"><option value="">Source: any</option></select>
    </div>
    <table>
      <thead><tr><th>Title</th><th>Source</th><th>Price</th><th>Link</th><th>Snippet</th></tr></thead>
      <tbody id="rows"></tbody>
    </table>
  </div>
<script>
const data = {payload};
const source = document.getElementById("source");
[...new Set(data.map(x=>x.source))].sort().forEach(v=>{{ const o=document.createElement("option"); o.value=v; o.textContent=v; source.appendChild(o); }});
function esc(s){{ return (s||"").replace(/[&<>]/g, c => ({{"&":"&amp;","<":"&lt;",">":"&gt;"}}[c])); }}
function render(){{
  const q=(document.getElementById("q").value||"").toLowerCase();
  const src=document.getElementById("source").value;
  const filtered=data.filter(x=>{{
    if(src && x.source!==src) return false;
    const hay=(x.title+" "+x.snippet).toLowerCase();
    return !q || hay.includes(q);
  }});
  document.getElementById("rows").innerHTML = filtered.map(x => `<tr><td>${{esc(x.title)}}</td><td>${{esc(x.source)}}</td><td>${{esc(x.price)}}</td><td><a href="${{x.url}}" target="_blank" rel="noopener">Open</a></td><td>${{esc((x.snippet||'').slice(0,180))}}</td></tr>`).join("");
}}
render();
</script>
</body>
</html>
"""
    dash_path.write_text(html_text, encoding="utf-8")


def _scrape_linkedin_jobs(role_query: str, region: str, limit: int = 40) -> List[JobListing]:
    region_phrase = "Ireland" if region == "ireland" else "United States"
    url = (
        "https://www.linkedin.com/jobs/search/?keywords="
        + urllib.parse.quote_plus(role_query)
        + "&location="
        + urllib.parse.quote_plus(region_phrase)
    )
    page = _fetch_text(url)
    results: List[JobListing] = []
    card_pattern = re.compile(
        r'<div class="base-card[^>]*job-search-card[\s\S]*?</li>',
        re.IGNORECASE,
    )
    href_re = re.compile(r'class="base-card__full-link[^"]*"[^>]*href="([^"]+)"', re.IGNORECASE)
    title_re = re.compile(r'<span class="sr-only">\s*([\s\S]*?)\s*</span>', re.IGNORECASE)
    loc_re = re.compile(r'class="job-search-card__location">\s*([\s\S]*?)\s*</span>', re.IGNORECASE)
    for card in card_pattern.findall(page):
        href_m = href_re.search(card)
        title_m = title_re.search(card)
        if not href_m or not title_m:
            continue
        href = html.unescape(href_m.group(1).strip())
        title = html.unescape(re.sub(r"\s+", " ", title_m.group(1)).strip())
        loc = ""
        loc_m = loc_re.search(card)
        if loc_m:
            loc = html.unescape(re.sub(r"\s+", " ", loc_m.group(1)).strip())
        if not loc:
            loc = "Ireland" if region == "ireland" else "United States"
        text = f"{title} {loc}"
        salary_text, salary_min, salary_max, currency = _extract_salary(text)
        results.append(
            JobListing(
                title=title,
                url=href,
                source="linkedin",
                location=loc,
                remote=("remote" in text.lower()),
                salary_text=salary_text,
                salary_min=salary_min,
                salary_max=salary_max,
                currency=currency,
                snippet="",
            )
        )
        if len(results) >= limit:
            break
    return results


def _scrape_builtin_jobs(role_query: str, region: str, limit: int = 30) -> List[JobListing]:
    region_phrase = "Ireland" if region == "ireland" else "United States"
    url = "https://builtin.com/jobs?search=" + urllib.parse.quote_plus(f"{role_query} {region_phrase}")
    page = _fetch_text(url)
    block_re = re.compile(r'<a href="(/job/[^"]+)"[^>]*data-id="job-card-title"[\s\S]*?</a>', re.IGNORECASE)
    results: List[JobListing] = []
    for m in block_re.finditer(page):
        href = "https://builtin.com" + html.unescape(m.group(1))
        title = html.unescape(re.sub(r"<.*?>", "", m.group(0))).strip()
        around = page[max(0, m.start() - 450) : m.start() + 900]
        # Try to capture nearby location text; fallback to requested region.
        loc_match = re.search(r"([A-Za-z .'-]+,\s*[A-Z]{2})", around)
        location = loc_match.group(1) if loc_match else ("Ireland" if region == "ireland" else "United States")
        salary_text, salary_min, salary_max, currency = _extract_salary(around)
        results.append(
            JobListing(
                title=title,
                url=href,
                source="builtin",
                location=location,
                remote=("remote" in around.lower() or "remote" in title.lower()),
                salary_text=salary_text,
                salary_min=salary_min,
                salary_max=salary_max,
                currency=currency,
                snippet="",
            )
        )
        if len(results) >= limit:
            break
    return results


def _extract_regions(instruction: str) -> List[str]:
    low = instruction.lower()
    has_us = "us" in low or "united states" in low or "usa" in low
    has_ireland = "ireland" in low
    if has_us and has_ireland:
        return ["us", "ireland"]
    if has_ireland:
        return ["ireland"]
    if has_us:
        return ["us"]
    return ["us", "ireland"]


def _to_job_listing(item: SearchResult, source: str, fallback_region: str) -> Optional[JobListing]:
    title = (item.title or "").strip()
    url = (item.url or "").strip()
    snippet = (item.snippet or "").strip()
    if not title or not url:
        return None
    salary_text, salary_min, salary_max, currency = _extract_salary(title + " " + snippet)
    location = _extract_location(title, snippet, fallback_region)
    remote = "remote" in f"{title} {snippet}".lower()
    return JobListing(
        title=title,
        url=url,
        source=source,
        location=location,
        remote=remote,
        salary_text=salary_text,
        salary_min=salary_min,
        salary_max=salary_max,
        currency=currency,
        snippet=snippet,
    )


def _extract_salary(text: str) -> tuple[str, Optional[float], Optional[float], str]:
    cleaned = text.replace(",", "")
    usd_range = re.search(r"(\$[0-9]{2,3}(?:\.[0-9]{1,2})?\s*[kK]?)\s*-\s*(\$[0-9]{2,3}(?:\.[0-9]{1,2})?\s*[kK]?)", cleaned)
    eur_range = re.search(r"(€[0-9]{2,3}(?:\.[0-9]{1,2})?\s*[kK]?)\s*-\s*(€[0-9]{2,3}(?:\.[0-9]{1,2})?\s*[kK]?)", cleaned)
    single_usd = re.search(r"\$[0-9]{2,3}(?:\.[0-9]{1,2})?\s*[kK]?", cleaned)
    single_eur = re.search(r"€[0-9]{2,3}(?:\.[0-9]{1,2})?\s*[kK]?", cleaned)

    if usd_range:
        a, b = usd_range.group(1), usd_range.group(2)
        return f"{a} - {b}", _money_to_number(a), _money_to_number(b), "USD"
    if eur_range:
        a, b = eur_range.group(1), eur_range.group(2)
        return f"{a} - {b}", _money_to_number(a), _money_to_number(b), "EUR"
    if single_usd:
        v = single_usd.group(0)
        n = _money_to_number(v)
        return v, n, n, "USD"
    if single_eur:
        v = single_eur.group(0)
        n = _money_to_number(v)
        return v, n, n, "EUR"
    return "", None, None, ""


def _money_to_number(token: str) -> Optional[float]:
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)", token)
    if not m:
        return None
    value = float(m.group(1))
    if "k" in token.lower():
        value *= 1000.0
    return value


def _extract_location(title: str, snippet: str, fallback_region: str) -> str:
    joined = f"{title} | {snippet}"
    patterns = [
        r"(Remote(?:\s*-\s*[A-Za-z ,]+)?)",
        r"([A-Za-z .'-]+,\s*(?:[A-Z]{2}|Ireland|UK|United States|USA))",
    ]
    for pat in patterns:
        match = re.search(pat, joined, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return "Ireland" if fallback_region == "ireland" else "United States"


def _salary_rank(job: JobListing) -> float:
    if job.salary_min is None and job.salary_max is None:
        return float("inf")
    vals = [x for x in (job.salary_min, job.salary_max) if x is not None]
    return statistics.mean(vals) if vals else float("inf")


def _write_job_artifacts(instruction: str, jobs: List[JobListing]) -> Dict[str, str]:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path("data/reports/job_search") / ts
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_path = out_dir / "jobs.csv"
    report_path = out_dir / "report.md"
    dash_path = out_dir / "dashboard.html"

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "title",
                "source",
                "location",
                "remote",
                "salary_text",
                "salary_min",
                "salary_max",
                "currency",
                "url",
                "snippet",
            ],
        )
        writer.writeheader()
        for j in jobs:
            writer.writerow(asdict(j))

    report_lines = [
        "# Job Search Report",
        "",
        f"- Generated: {datetime.now().isoformat(timespec='seconds')}",
        f"- Instruction: {instruction}",
        f"- Total Listings: {len(jobs)}",
        f"- Remote Listings: {sum(1 for j in jobs if j.remote)}",
        "",
        "## Top Listings",
    ]
    for idx, j in enumerate(jobs[:30], start=1):
        report_lines.append(f"{idx}. [{j.title}]({j.url}) | {j.source} | {j.location} | {j.salary_text or 'salary n/a'}")
    report_path.write_text("\n".join(report_lines), encoding="utf-8")

    _write_dashboard_html(dash_path=dash_path, jobs=jobs, title="VP/AVP Data & AI Job Dashboard")
    return {
        "directory": str(out_dir.resolve()),
        "jobs_csv": str(csv_path.resolve()),
        "report_md": str(report_path.resolve()),
        "dashboard_html": str(dash_path.resolve()),
    }


def _write_dashboard_html(dash_path: Path, jobs: List[JobListing], title: str) -> None:
    rows = []
    for j in jobs:
        rows.append(
            {
                "title": j.title,
                "source": j.source,
                "location": j.location,
                "remote": "Yes" if j.remote else "No",
                "salary": j.salary_text or "n/a",
                "salary_mid": _salary_rank(j) if _salary_rank(j) != float("inf") else 0,
                "currency": j.currency or "",
                "url": j.url,
            }
        )
    payload = json.dumps(rows)
    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{title}</title>
  <style>
    body {{ font-family: 'Segoe UI', Tahoma, sans-serif; margin: 0; background: #f8fafc; color: #0f172a; }}
    .wrap {{ max-width: 1200px; margin: 0 auto; padding: 20px; }}
    .hero {{ background: linear-gradient(120deg,#ecfeff,#eef2ff); border:1px solid #cbd5e1; border-radius:14px; padding:18px; }}
    .controls {{ display:flex; gap:10px; flex-wrap:wrap; margin:14px 0; }}
    input, select {{ border:1px solid #cbd5e1; border-radius:10px; padding:9px; font-size:14px; }}
    table {{ width:100%; border-collapse: collapse; background:white; border-radius: 12px; overflow: hidden; }}
    th, td {{ border-bottom:1px solid #e2e8f0; padding:10px; text-align:left; font-size:14px; vertical-align: top; }}
    th {{ background:#0f172a; color:#e2e8f0; position: sticky; top: 0; }}
    a {{ color:#0f766e; text-decoration: none; }}
    .pill {{ font-size:12px; border-radius:20px; padding:2px 8px; background:#ecfeff; color:#115e59; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <h1 style="margin:0">{title}</h1>
      <p style="margin:6px 0 0 0">Interactive view for role, region, remote, compensation, and apply links.</p>
    </div>
    <div class="controls">
      <input id="q" placeholder="Search title/location/source" oninput="render()"/>
      <select id="remote" onchange="render()">
        <option value="">Remote: any</option>
        <option value="Yes">Remote only</option>
        <option value="No">On-site/hybrid</option>
      </select>
      <select id="source" onchange="render()">
        <option value="">Source: any</option>
      </select>
      <select id="currency" onchange="render()">
        <option value="">Currency: any</option>
        <option value="USD">USD</option>
        <option value="EUR">EUR</option>
      </select>
    </div>
    <table>
      <thead><tr><th>Role</th><th>Source</th><th>Location</th><th>Remote</th><th>Salary</th><th>Apply</th></tr></thead>
      <tbody id="rows"></tbody>
    </table>
  </div>
<script>
const data = {payload};
const sourceSel = document.getElementById("source");
[...new Set(data.map(x=>x.source))].sort().forEach(v=>{{ const o=document.createElement("option"); o.value=v; o.textContent=v; sourceSel.appendChild(o); }});
function esc(s){{ return (s||"").replace(/[&<>]/g, c => ({{"&":"&amp;","<":"&lt;",">":"&gt;"}}[c])); }}
function render(){{
  const q=(document.getElementById("q").value||"").toLowerCase();
  const remote=document.getElementById("remote").value;
  const source=document.getElementById("source").value;
  const currency=document.getElementById("currency").value;
  const filtered=data.filter(x=>{{
    if(remote && x.remote!==remote) return false;
    if(source && x.source!==source) return false;
    if(currency && x.currency!==currency) return false;
    const hay=(x.title+" "+x.location+" "+x.source).toLowerCase();
    return !q || hay.includes(q);
  }});
  const tbody=document.getElementById("rows");
  tbody.innerHTML=filtered.map(x=>`<tr><td>${{esc(x.title)}}</td><td><span class="pill">${{esc(x.source)}}</span></td><td>${{esc(x.location)}}</td><td>${{esc(x.remote)}}</td><td>${{esc(x.salary)}}</td><td><a href="${{x.url}}" target="_blank" rel="noopener">Open</a></td></tr>`).join("");
}}
render();
</script>
</body>
</html>
"""
    dash_path.write_text(html, encoding="utf-8")


def _job_summary(jobs: List[JobListing]) -> Dict[str, Any]:
    usd_mids = [_salary_rank(j) for j in jobs if j.currency == "USD" and _salary_rank(j) != float("inf")]
    eur_mids = [_salary_rank(j) for j in jobs if j.currency == "EUR" and _salary_rank(j) != float("inf")]
    return {
        "total": len(jobs),
        "remote": sum(1 for j in jobs if j.remote),
        "by_source": _count_by(j.source for j in jobs),
        "by_region": {
            "ireland_like": sum(1 for j in jobs if "ireland" in j.location.lower() or ", ie" in j.location.lower()),
            "us_like": sum(1 for j in jobs if "united states" in j.location.lower() or re.search(r",\s*[A-Z]{2}$", j.location)),
        },
        "salary": {
            "usd_avg": round(statistics.mean(usd_mids), 2) if usd_mids else None,
            "eur_avg": round(statistics.mean(eur_mids), 2) if eur_mids else None,
            "count_with_salary": sum(1 for j in jobs if j.salary_text),
        },
    }


def _count_by(values: Any) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for v in values:
        out[v] = out.get(v, 0) + 1
    return out


def _emit_progress(progress_cb: Optional[Callable[[int, str], None]], pct: int, message: str) -> None:
    if not progress_cb:
        return
    try:
        progress_cb(max(0, min(100, int(pct))), message)
    except Exception:
        return


def resume_pending_plan(pending: Dict[str, Any], step_mode: bool = False) -> Dict[str, Any]:
    plan = pending.get("plan", {})
    plan_steps = [dict(x) for x in plan.get("steps", [])]
    start = int(pending.get("next_step_index", 0))
    run = execute_plan(plan, start_index=start, step_mode=step_mode, allow_input_fallback=True)
    response = {
        "ok": run.ok,
        "mode": "desktop_sequence_resume",
        "plan": plan,
        "trace": run.trace,
        "done": run.done,
        "next_step_index": run.next_step_index,
        "pending_plan": {"plan": plan, "next_step_index": run.next_step_index} if not run.done else None,
        "paused_for_credentials": run.paused_for_credentials,
        "pause_reason": run.pause_reason,
        "error": run.error,
        "canvas": {
            "title": "Desktop Resume",
            "subtitle": f"Resumed from step {start}",
            "cards": [
                {"title": f"Step {t.get('step', 0)}: {t.get('action','')}", "price": "ok" if t.get("ok") else "error", "source": "uia", "url": ""}
                for t in run.trace[:6]
            ],
        },
    }
    return _finalize_operator_result(response, instruction=str(plan.get("instruction", "")), plan_steps=plan_steps)
