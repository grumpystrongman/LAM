from __future__ import annotations

import json
import csv
import html
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
from typing import Any, Dict, List, Optional

from lam.interface.ai_backend import backend_metadata, normalize_backend
from lam.interface.app_launcher import normalize_app_name, open_installed_app
from lam.interface.app_learner import get_guidance
from lam.interface.desktop_sequence import assess_risk, build_plan, execute_plan
from lam.interface.local_vector_store import LocalVectorStore

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"


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
    results = _parse_duckduckgo(query, limit=limit)
    if len(results) >= max(3, limit // 2):
        return results
    fallback = _parse_bing_rss(query, limit=limit)
    out: Dict[str, SearchResult] = {}
    for r in results + fallback:
        out[r.url] = r
    return list(out.values())[:limit]


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


def execute_instruction(
    instruction: str,
    control_granted: bool,
    step_mode: bool = False,
    confirm_risky: bool = False,
    ai_backend: str = "deterministic-local",
) -> Dict[str, Any]:
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

    if _is_native_planning_intent(normalized):
        plan = _build_native_plan(normalized)
        execution = _execute_native_plan(plan=plan, instruction=instruction)
        execution["ai"] = ai_meta
        return execution

    if _is_job_research_intent(normalized):
        findings = _run_job_market_research(normalized)
        return {
            "ok": findings["ok"],
            "mode": "job_market_research",
            "instruction": instruction,
            "ai": ai_meta,
            "query": findings["query"],
            "results_count": findings["results_count"],
            "artifacts": findings["artifacts"],
            "summary": findings["summary"],
            "results": findings["results"],
            "source_status": findings.get("source_status", {}),
            "opened_url": findings.get("opened_url", ""),
            "canvas": findings["canvas"],
            "paused_for_credentials": False,
            "pause_reason": "",
        }

    if _is_desktop_sequence_intent(normalized):
        plan = build_plan(normalized)
        risk = assess_risk(plan)
        if risk["requires_confirmation"] and not confirm_risky:
            return {
                "ok": False,
                "mode": "desktop_sequence_preview",
                "instruction": instruction,
                "requires_confirmation": True,
                "message": "Risky actions detected. Confirm to execute.",
                "risk": risk,
                "plan": plan,
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
        store = LocalVectorStore()
        app_name = plan.get("app_name", "") or "desktop"
        guidance = get_guidance(app_name=app_name, user_goal=normalized, store=store) if app_name else {"guidance": []}
        return {
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

    open_match = re.search(r"\bopen\s+(.+?)(?:\s+app)?\b", normalized, flags=re.IGNORECASE)
    if open_match and ("search" not in normalized.lower()):
        target = open_match.group(1).strip()
        ok, launched = open_installed_app(target)
        app_name = normalize_app_name(target)
        store = LocalVectorStore()
        guidance = get_guidance(app_name=app_name, user_goal=normalized, store=store)
        if ok:
            return {
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
        return {
            "ok": False,
            "mode": "desktop_app_open",
            "instruction": instruction,
            "ai": ai_meta,
            "error": f"Could not locate installed app '{target}'.",
            "app_name": app_name,
            "guidance": guidance,
        }

    query = normalized
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
    return json.loads(json.dumps(response))


def preview_instruction(instruction: str) -> Dict[str, Any]:
    normalized = instruction.strip()
    if not normalized:
        return {"ok": False, "error": "Instruction is empty."}
    if _is_native_planning_intent(normalized):
        plan = _build_native_plan(normalized)
        return {
            "ok": True,
            "mode": "preview_native_plan",
            "instruction": instruction,
            "plan": plan,
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
        risk = assess_risk(plan)
        return {
            "ok": True,
            "mode": "preview_desktop_sequence",
            "instruction": instruction,
            "plan": plan,
            "risk": risk,
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
    ]
    complexity = sum(1 for s in signals if s in low)
    return complexity >= 2 or (len(instruction) > 180 and any(k in low for k in ["find", "research", "search"]))


def _build_native_plan(instruction: str) -> Dict[str, Any]:
    domain = "job_market" if _is_job_research_intent(instruction) else "web_research"
    deliverables: List[str] = []
    low = instruction.lower()
    if "spreadsheet" in low or "csv" in low:
        deliverables.append("spreadsheet")
    if "report" in low:
        deliverables.append("report")
    if "dashboard" in low:
        deliverables.append("dashboard")
    if "link" in low:
        deliverables.append("apply_links")
    if not deliverables:
        deliverables = ["report", "dashboard"]

    sources = (
        ["linkedin", "indeed", "ziprecruiter", "glassdoor", "builtin"]
        if domain == "job_market"
        else ["web_search", "source_pages"]
    )
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


def _execute_native_plan(plan: Dict[str, Any], instruction: str) -> Dict[str, Any]:
    if plan.get("domain") == "job_market":
        findings = _run_job_market_research(instruction)
        return {
            "ok": findings["ok"],
            "mode": "autonomous_plan_execute",
            "plan": plan,
            "instruction": instruction,
            "decision_log": [
                "Detected multi-step research + artifact request.",
                "Selected job_market strategy and commercial job-board sources.",
                "Executed structured extraction and artifact generation.",
            ],
            "query": findings["query"],
            "results_count": findings["results_count"],
            "artifacts": findings["artifacts"],
            "summary": findings["summary"],
            "results": findings["results"],
            "source_status": findings.get("source_status", {}),
            "opened_url": findings.get("opened_url", ""),
            "canvas": findings["canvas"],
            "paused_for_credentials": False,
            "pause_reason": "",
        }

    findings = _run_generic_research(instruction)
    return {
        "ok": findings["ok"],
        "mode": "autonomous_plan_execute",
        "plan": plan,
        "instruction": instruction,
        "decision_log": [
            "Detected generic multi-step research task.",
            "Built diversified web queries and ranked results by relevance.",
            "Generated reusable spreadsheet/report/dashboard artifacts.",
        ],
        "query": findings["query"],
        "results_count": findings["results_count"],
        "artifacts": findings["artifacts"],
        "summary": findings["summary"],
        "results": findings["results"],
        "source_status": findings.get("source_status", {}),
        "opened_url": findings.get("opened_url", ""),
        "canvas": findings["canvas"],
        "paused_for_credentials": False,
        "pause_reason": "",
    }


def _is_job_research_intent(instruction: str) -> bool:
    low = instruction.lower()
    job_terms = ["job", "position", "vp", "avp", "linkedin", "indeed", "salary", "remote"]
    analysis_terms = ["spreadsheet", "dashboard", "report", "analysis"]
    return sum(1 for t in job_terms if t in low) >= 3 and any(t in low for t in analysis_terms)


def _run_job_market_research(instruction: str) -> Dict[str, Any]:
    role_query = _extract_role_query(instruction)
    region_labels = _extract_regions(instruction)
    source_status: Dict[str, str] = {}
    collected: List[JobListing] = []

    for region in region_labels:
        try:
            li = _scrape_linkedin_jobs(role_query=role_query, region=region, limit=40)
            collected.extend(li)
            source_status[f"linkedin_{region}"] = f"ok:{len(li)}"
        except Exception as exc:
            source_status[f"linkedin_{region}"] = f"error:{type(exc).__name__}"

        try:
            bi = _scrape_builtin_jobs(role_query=role_query, region=region, limit=30)
            collected.extend(bi)
            source_status[f"builtin_{region}"] = f"ok:{len(bi)}"
        except Exception as exc:
            source_status[f"builtin_{region}"] = f"error:{type(exc).__name__}"

        # Best-effort site-search fallback for other commercial boards.
        site_queries = [
            ("indeed", "site:indeed.com/jobs"),
            ("ziprecruiter", "site:ziprecruiter.com/jobs"),
            ("glassdoor", "site:glassdoor.com/job-listing"),
        ]
        region_phrase = "Ireland" if region == "ireland" else "United States"
        for source, site_prefix in site_queries:
            query = f'{site_prefix} "{role_query}" {region_phrase} salary remote'
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

    dedup: Dict[str, JobListing] = {}
    for job in collected:
        dedup[job.url] = job
    jobs_all = list(dedup.values())
    jobs = [j for j in jobs_all if _is_target_job(j.title)]
    if len(jobs) < 12:
        jobs = jobs_all
    jobs.sort(key=lambda j: (_salary_rank(j), not j.remote, j.source, j.title))
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


def _run_generic_research(instruction: str) -> Dict[str, Any]:
    query = _extract_generic_query(instruction)
    queries = _expand_queries(query)
    collected: List[SearchResult] = []
    source_status: Dict[str, str] = {}
    for q in queries:
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
    ranked.sort(key=lambda x: _relevance_score(x, query), reverse=True)
    artifacts = _write_generic_research_artifacts(instruction=instruction, query=query, results=ranked)
    summary = {
        "total": len(ranked),
        "sources": _count_by(r.source for r in ranked),
    }
    dashboard_uri = Path(artifacts["dashboard_html"]).resolve().as_uri() if artifacts.get("dashboard_html") else ""
    if dashboard_uri:
        webbrowser.open(dashboard_uri, new=2)
    return {
        "ok": True,
        "query": query,
        "results_count": len(ranked),
        "results": [asdict(x) for x in ranked[:50]],
        "artifacts": artifacts,
        "summary": summary,
        "source_status": source_status,
        "opened_url": dashboard_uri,
        "canvas": {
            "title": "Research Dashboard Generated",
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


def _expand_queries(query: str) -> List[str]:
    base = query.strip()
    variants = [
        base,
        f"{base} salary range",
        f"{base} remote",
        f"{base} United States",
        f"{base} Ireland",
    ]
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
    _write_generic_dashboard_html(dash_path=dash_path, results=results, title="Research Dashboard")

    return {
        "directory": str(out_dir.resolve()),
        "results_csv": str(csv_path.resolve()),
        "report_md": str(report_path.resolve()),
        "dashboard_html": str(dash_path.resolve()),
    }


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


def resume_pending_plan(pending: Dict[str, Any], step_mode: bool = False) -> Dict[str, Any]:
    plan = pending.get("plan", {})
    start = int(pending.get("next_step_index", 0))
    run = execute_plan(plan, start_index=start, step_mode=step_mode, allow_input_fallback=True)
    return {
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
