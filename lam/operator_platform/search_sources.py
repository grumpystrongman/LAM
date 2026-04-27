from __future__ import annotations

import re
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from typing import List, Optional

from .research_constants import USER_AGENT
from .research_types import SearchResult


def extract_price(text: str) -> Optional[float]:
    price_match = re.search(r"\$([0-9]{1,4}(?:\.[0-9]{2})?)", str(text or "").replace(",", ""))
    if not price_match:
        return None
    try:
        return float(price_match.group(1))
    except ValueError:
        return None


def fetch_text(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=20) as resp:  # nosec B310 - controlled URLs
        return resp.read().decode("utf-8", errors="ignore")


def parse_duckduckgo(query: str, limit: int = 8) -> List[SearchResult]:
    q = urllib.parse.quote_plus(query)
    url = f"https://duckduckgo.com/html/?q={q}"
    html = fetch_text(url)
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
        results.append(
            SearchResult(
                title=title,
                url=url_value,
                price=extract_price(title + " " + snippet),
                source="duckduckgo",
                snippet=snippet,
            )
        )
        if len(results) >= limit:
            break
    return results


def parse_bing_rss(query: str, limit: int = 8) -> List[SearchResult]:
    q = urllib.parse.quote_plus(query)
    url = f"https://www.bing.com/search?q={q}&format=rss"
    xml_text = fetch_text(url)
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
                price=extract_price(title + " " + snippet),
                source="bing_rss",
                snippet=snippet,
            )
        )
        if len(results) >= limit:
            break
    return results


def safe_search_web(query: str, limit: int = 10) -> List[SearchResult]:
    try:
        return parse_duckduckgo(query, limit=limit)
    except Exception:
        return []


def search_web(query: str, limit: int = 10) -> List[SearchResult]:
    results = safe_search_web(query, limit=limit)
    if len(results) >= max(3, limit // 2):
        return results
    try:
        fallback = parse_bing_rss(query, limit=limit)
    except Exception:
        fallback = []
    out = {row.url: row for row in results + fallback}
    return list(out.values())[:limit]
