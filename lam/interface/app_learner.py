from __future__ import annotations

import re
import urllib.parse
import urllib.request
from typing import Dict, List

from lam.interface.local_vector_store import LocalVectorStore


USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"


def _fetch(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=20) as resp:  # nosec B310 - controlled URLs
        return resp.read().decode("utf-8", errors="ignore")


def _duckduckgo_search(query: str, limit: int = 5) -> List[Dict[str, str]]:
    q = urllib.parse.quote_plus(query)
    html = _fetch(f"https://duckduckgo.com/html/?q={q}")
    pattern = re.compile(
        r'<a[^>]*class="result__a"[^>]*href="(?P<href>[^"]+)"[^>]*>(?P<title>.*?)</a>',
        re.IGNORECASE | re.DOTALL,
    )
    snippet_pattern = re.compile(r'<a[^>]*class="result__snippet"[^>]*>(.*?)</a>', re.IGNORECASE | re.DOTALL)
    snippets = snippet_pattern.findall(html)

    out: List[Dict[str, str]] = []
    for idx, m in enumerate(pattern.finditer(html)):
        href = m.group("href")
        title = re.sub("<.*?>", "", m.group("title")).strip()
        parsed = urllib.parse.urlparse(href)
        if parsed.netloc.endswith("duckduckgo.com"):
            qs = urllib.parse.parse_qs(parsed.query)
            if "uddg" in qs:
                href = urllib.parse.unquote(qs["uddg"][0])
            elif "u3" in qs:
                href = urllib.parse.unquote(qs["u3"][0])
        snippet = re.sub("<.*?>", "", snippets[idx]).strip() if idx < len(snippets) else ""
        if href.startswith("http"):
            out.append({"title": title, "url": href, "snippet": snippet})
        if len(out) >= limit:
            break
    return out


def learn_app_usage(app_name: str, store: LocalVectorStore) -> Dict:
    query = f"how to use {app_name} windows app tutorial keyboard shortcuts"
    hits = _duckduckgo_search(query, limit=6)
    added = 0
    for hit in hits:
        content = (hit.get("title", "") + ". " + hit.get("snippet", "")).strip()
        if len(content) < 20:
            continue
        store.add_document(
            app_name=app_name,
            source_url=hit["url"],
            title=hit["title"][:220],
            content=content,
        )
        added += 1
    return {"app_name": app_name, "query": query, "sources_added": added}


def get_guidance(app_name: str, user_goal: str, store: LocalVectorStore) -> Dict:
    docs = store.search(app_name, user_goal, top_k=4)
    if not docs:
        learn_app_usage(app_name, store)
        docs = store.search(app_name, user_goal, top_k=4)
    guidance = []
    for d in docs:
        guidance.append(
            {
                "title": d["title"],
                "source_url": d["source_url"],
                "tip": d["content"][:260],
                "score": round(float(d["score"]), 4),
            }
        )
    return {"app_name": app_name, "guidance": guidance}

