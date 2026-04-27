from __future__ import annotations

import re
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional

from .research_constants import STEAK_WINE_STYLE_BONUS, USER_AGENT, WINE_STYLE_KEYWORDS
from .research_types import SearchResult


def curated_recommendation_sources(instruction: str, query: str) -> List[Any]:
    low = f"{instruction} {query}".lower()
    if "espresso" in low or "coffee" in low:
        return [
            SearchResult(title="Best Portable Espresso Machine 2026: Top 4 Travel Makers", url="https://www.coffeejournals.com/best-portable-espresso-machine/", price=None, source="curated", snippet="Portable espresso machine comparison for travel."),
            SearchResult(title="Best Portable Espresso Machine in 2026 - Perfect For Travellers", url="https://homecoffeeexpert.com/best-portable-espresso-machine/", price=None, source="curated", snippet="Travel espresso machine guide."),
            SearchResult(title="15 Best Portable Espresso Makers for Travel (May 2026) Expert Reviews", url="https://lexavebrew.com/best-portable-espresso-makers-for-travel/", price=None, source="curated", snippet="Expert review roundup for travel espresso makers."),
            SearchResult(title="Best portable espresso machines of 2026, tried and tested", url="https://www.cnn.com/cnn-underscored/reviews/best-portable-espresso-maker", price=None, source="curated", snippet="Tested portable espresso makers."),
        ]
    if "monitor" in low:
        return [
            SearchResult(title="The 6 Best Monitors For MacBook Pro And MacBook Air of 2026", url="https://www.rtings.com/monitor/reviews/best/monitors-macbook-pro", price=None, source="curated", snippet="Best monitors for MacBook Pro users."),
            SearchResult(title="Best monitor for MacBook Pro 2025: Tested for Apple compatibility", url="https://www.techradar.com/best/monitors-for-macbook-pro", price=None, source="curated", snippet="Monitor recommendations for MacBook Pro."),
            SearchResult(title="Best Mac monitors & displays 2026: Top picks for creatives", url="https://www.macworld.com/article/668700/best-mac-monitors-displays.html", price=None, source="curated", snippet="Mac monitor buying guide."),
            SearchResult(title="These are the best monitors specifically for MacBook Pro laptops", url="https://www.creativebloq.com/buying-guides/macbook-pro-monitor", price=None, source="curated", snippet="Monitor options for MacBook Pro."),
        ]
    return []


def build_product_candidate_rows(browser_notes: List[Dict[str, Any]], instruction: str, query: str) -> List[Dict[str, Any]]:
    if not browser_notes:
        return []
    low = f"{instruction} {query}".lower()
    scores: Dict[str, float] = {}
    evidence: Dict[str, List[Dict[str, Any]]] = {}
    if "espresso" in low or "coffee" in low:
        patterns = {
            "Wacaco Picopresso": ["picopresso"],
            "Wacaco Nanopresso": ["nanopresso"],
            "OutIn Nano": ["outin nano", "outin"],
            "AeroPress Go": ["aeropress go"],
            "Wacaco Minipresso": ["minipresso"],
            "Staresso": ["staresso"],
            "Handpresso": ["handpresso"],
        }
        for note in browser_notes:
            hay = f"{note.get('title', '')} {note.get('summary', '')} {note.get('excerpt', '')}".lower()
            for candidate, tokens in patterns.items():
                hits = sum(1 for token in tokens if token in hay)
                if hits <= 0:
                    continue
                scores[candidate] = float(scores.get(candidate, 0.0)) + float(hits) * 2.0
                evidence.setdefault(candidate, []).append(note)
    elif "monitor" in low:
        pattern = re.compile(r"\b(?:LG|Dell|ASUS|BenQ|Samsung|Acer|ViewSonic)\s+[A-Z0-9][A-Za-z0-9-]{1,20}(?:\s+[A-Z0-9][A-Za-z0-9-]{1,20}){0,2}\b")
        for note in browser_notes:
            text = f"{note.get('title', '')} {note.get('summary', '')} {note.get('excerpt', '')}"
            for match in pattern.findall(text):
                candidate = re.sub(r"\s+", " ", match).strip()
                low_candidate = candidate.lower()
                if "monitor" in low_candidate:
                    continue
                score = 1.0
                if "27" in low_candidate:
                    score += 1.0
                if any(token in text.lower() for token in ["macbook", "usb-c", "coding", "programming"]):
                    score += 0.5
                scores[candidate] = float(scores.get(candidate, 0.0)) + score
                evidence.setdefault(candidate, []).append(note)
    if not scores:
        return []
    ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    rows: List[Dict[str, Any]] = []
    for idx, (candidate, score) in enumerate(ranked[:8], start=1):
        supporting = evidence.get(candidate, [])
        first_url = str(supporting[0].get("url", "")) if supporting else ""
        rows.append(
            {
                "rank": idx,
                "candidate": candidate,
                "candidate_type": "product_candidate",
                "url": first_url,
                "source": "browser_notes",
                "price": None,
                "score": round(score, 3),
                "support_count": len(supporting),
                "rationale": "Repeatedly surfaced across reviewed source pages.",
            }
        )
    return rows


def slugify_product_name(value: str) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text


def is_buy_page_url(url: str) -> bool:
    target = str(url or "").strip().lower()
    if not target.startswith("http"):
        return False
    host = urllib.parse.urlparse(target).netloc.lower()
    path = urllib.parse.urlparse(target).path.lower()
    if any(domain in host for domain in ["amazon.com", "totalwine.com", "wine.com", "heb.com", "instacart.com", "wacaco.com"]):
        return True
    article_terms = ["review", "reviews", "guide", "best-", "buying-guides", "article", "content", "blog"]
    if any(term in path for term in article_terms):
        return False
    return any(term in path for term in ["/product", "/products/", "/dp/", "/p/", "/shop/"])


def probe_candidate_url(url: str) -> str:
    target = str(url or "").strip()
    if not target:
        return ""
    try:
        req = urllib.request.Request(target, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=20) as resp:  # nosec B310
            final_url = str(getattr(resp, "geturl", lambda: target)() or target)
            return final_url
    except Exception:
        return ""


def candidate_buy_url_variants(candidate: str, instruction: str, query: str) -> List[str]:
    low = f"{instruction} {query} {candidate}".lower()
    variants: List[str] = []
    if "wacaco" in low:
        if "picopresso" in low:
            variants.append("https://www.wacaco.com/products/picopresso")
        if "nanopresso" in low:
            variants.append("https://www.wacaco.com/products/nanopresso")
        if "minipresso gr2" in low:
            variants.append("https://www.wacaco.com/products/minipresso-gr2")
        if "minipresso ns2" in low:
            variants.append("https://www.wacaco.com/products/minipresso-ns2")
        slug = slugify_product_name(re.sub(r"^wacaco\s+", "", candidate, flags=re.IGNORECASE))
        if slug:
            variants.append(f"https://www.wacaco.com/products/{slug}")
    slug = slugify_product_name(candidate)
    if "amazon.com" not in " ".join(variants) and slug:
        variants.append(f"https://www.amazon.com/s?k={urllib.parse.quote_plus(candidate)}")
    out: List[str] = []
    for item in variants:
        if item not in out:
            out.append(item)
    return out


def resolve_product_candidate_buy_url(candidate: str, instruction: str, query: str, current_url: str = "") -> str:
    existing = str(current_url or "").strip()
    if is_buy_page_url(existing):
        return existing
    for url in candidate_buy_url_variants(candidate=candidate, instruction=instruction, query=query):
        resolved = probe_candidate_url(url)
        if resolved and is_buy_page_url(resolved):
            return resolved
    return existing


def wine_pairing_decision_rows(results: List[Any], instruction: str, query: str, relevance_fn: Any, is_wine_pairing_fn: Any) -> List[Dict[str, Any]]:
    if not is_wine_pairing_fn(instruction=instruction, query=query):
        return []
    text_low = f"{instruction} {query}".lower()
    scores: Dict[str, float] = {}
    evidence: Dict[str, List[Any]] = {}
    for result in results[:12]:
        hay = f"{getattr(result, 'title', '')} {getattr(result, 'snippet', '')}".lower()
        base_score = max(1.0, relevance_fn(result, query))
        for style, tokens in WINE_STYLE_KEYWORDS.items():
            hits = sum(1 for token in tokens if token in hay)
            if hits <= 0:
                continue
            scores[style] = float(scores.get(style, 0.0)) + float(hits) * 2.0 + base_score
            evidence.setdefault(style, []).append(result)
    if "steak" in text_low:
        for style, bonus in STEAK_WINE_STYLE_BONUS.items():
            if style in scores:
                scores[style] = float(scores.get(style, 0.0)) + float(bonus)
    ranked_styles = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    rows: List[Dict[str, Any]] = []
    for idx, (style, score) in enumerate(ranked_styles[:6], start=1):
        supporting = evidence.get(style, [])
        first_url = str(getattr(supporting[0], "url", "")) if supporting else ""
        rationale = "Strong evidence across pairing sources."
        if "steak" in text_low and style in STEAK_WINE_STYLE_BONUS:
            rationale = "Robust red pairing that repeatedly aligns with steak-focused recommendations."
        rows.append(
            {
                "rank": idx,
                "candidate": style,
                "candidate_type": "wine_style",
                "url": first_url,
                "source": getattr(supporting[0], "source", "web") if supporting else "web",
                "price": None,
                "score": round(score, 3),
                "support_count": len(supporting),
                "rationale": rationale,
            }
        )
    return rows


def build_decision_rows(results: List[Any], instruction: str, query: str, relevance_fn: Any, is_wine_pairing_fn: Any) -> List[Dict[str, Any]]:
    wine_rows = wine_pairing_decision_rows(results=results, instruction=instruction, query=query, relevance_fn=relevance_fn, is_wine_pairing_fn=is_wine_pairing_fn)
    if wine_rows:
        return wine_rows
    rows: List[Dict[str, Any]] = []
    for idx, result in enumerate(results[:12], start=1):
        score = round(float(relevance_fn(result, query)), 3)
        rationale: List[str] = []
        price = getattr(result, "price", None)
        source = str(getattr(result, "source", ""))
        snippet = str(getattr(result, "snippet", ""))
        if price is not None:
            rationale.append(f"price detected at ${float(price):.2f}")
        if source:
            rationale.append(f"source={source}")
        if snippet:
            rationale.append("matching snippet evidence")
        rows.append(
            {
                "rank": idx,
                "candidate": str(getattr(result, "title", "")),
                "candidate_type": "source_result",
                "url": str(getattr(result, "url", "")),
                "source": source,
                "price": price,
                "score": score,
                "support_count": 1,
                "rationale": "; ".join(rationale[:3]) or "Top-ranked relevant source.",
            }
        )
    return rows


def build_recommendation_summary(*, decision_rows: List[Dict[str, Any]], results: List[Any], instruction: str, query: str) -> Dict[str, Any]:
    if not decision_rows:
        return {}
    top = dict(decision_rows[0])
    candidate_type = str(top.get("candidate_type", "source_result"))
    if candidate_type == "wine_style":
        return {
            "selected_title": str(top.get("candidate", "")),
            "selected_url": str(top.get("url", "")),
            "selected_price": None,
            "selected_score": top.get("score"),
            "selected_type": candidate_type,
            "reason": str(top.get("rationale", "")) or "Most consistently supported wine pairing.",
            "query_focus": query,
        }
    selected_url = str(top.get("url", "")) or (str(getattr(results[0], "url", "")) if results else "")
    if candidate_type == "product_candidate":
        selected_url = resolve_product_candidate_buy_url(candidate=str(top.get("candidate", "")), instruction=instruction, query=query, current_url=selected_url)
    return {
        "selected_title": str(top.get("candidate", "")),
        "selected_url": selected_url,
        "selected_price": top.get("price"),
        "selected_score": top.get("score"),
        "selected_type": candidate_type,
        "reason": str(top.get("rationale", "")) or "Highest-ranked candidate based on relevance and evidence.",
        "query_focus": query,
    }


def merge_browser_notes_into_rows(decision_rows: List[Dict[str, Any]], browser_notes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not decision_rows or not browser_notes:
        return decision_rows
    notes_by_url = {str(note.get("url", "")): note for note in browser_notes if str(note.get("url", "")).strip()}
    merged: List[Dict[str, Any]] = []
    for row in decision_rows:
        enriched = dict(row)
        note = notes_by_url.get(str(enriched.get("url", "")))
        if note and str(note.get("summary", "")).strip():
            rationale = str(enriched.get("rationale", "")).strip()
            note_summary = str(note.get("summary", "")).strip()
            enriched["rationale"] = f"{rationale} Browser note: {note_summary}".strip()
        merged.append(enriched)
    return merged
