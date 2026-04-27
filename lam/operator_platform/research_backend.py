from __future__ import annotations

import re
from typing import Any, Dict, List

from lam.interface.human_judgment import assess_result_quality

from .research_constants import QUERY_NOISE_TERMS, RECOMMENDATION_RESEARCH_TOKENS, WINE_STYLE_KEYWORDS
from .search_sources import search_web as platform_search_web


def _is_price_recommendation_intent(instruction: str) -> bool:
    low = str(instruction or "").lower()
    strong = [
        "best price",
        "lowest price",
        "cheapest",
        "recommend me the one to buy",
        "which one to buy",
    ]
    if any(token in low for token in strong):
        return True
    return "ebay" in low and "price" in low


def is_recommendation_research_intent(instruction: str) -> bool:
    low = str(instruction or "").lower()
    has_decision_signal = any(token in low for token in RECOMMENDATION_RESEARCH_TOKENS) or _is_price_recommendation_intent(low)
    has_research_signal = any(token in low for token in ["research", "find", "search", "look up", "analy", "buy", "find me"])
    return bool(has_decision_signal and has_research_signal)


def is_wine_pairing_intent(instruction: str, query: str = "") -> bool:
    low = f"{instruction} {query}".lower()
    has_wine = "wine" in low
    has_food = any(token in low for token in ["dinner", "steak", "potatoes", "pairing", "pair with", "meal"])
    return has_wine and has_food


def query_focus_terms(text: str) -> List[str]:
    normalized = re.sub(r"(\d+)\s+inch\b", r"\1-inch", str(text or "").lower())
    terms = [term for term in re.split(r"[^a-z0-9-]+", normalized) if len(term) > 2]
    filtered = [term for term in terms if term not in QUERY_NOISE_TERMS]
    return filtered or terms


def topic_overlap_count(result: Any, query: str) -> int:
    focus_terms = query_focus_terms(query)
    hay = f"{str(getattr(result, 'title', '')).lower()} {str(getattr(result, 'snippet', '')).lower()} {str(getattr(result, 'url', '')).lower()}"
    return sum(1 for term in focus_terms if term and term in hay)


def minimum_topic_overlap(query: str) -> int:
    focus_terms = query_focus_terms(query)
    if len(focus_terms) >= 3:
        return 2
    return 1


def passes_topic_gate(result: Any, instruction: str, query: str) -> bool:
    hay = f"{str(getattr(result, 'title', '')).lower()} {str(getattr(result, 'snippet', '')).lower()} {str(getattr(result, 'url', '')).lower()}"
    low = f"{instruction} {query}".lower()
    style_terms = {token for tokens in WINE_STYLE_KEYWORDS.values() for token in tokens}
    if is_wine_pairing_intent(instruction=instruction, query=query):
        has_wine_signal = "wine" in hay or any(token in hay for token in style_terms)
        has_pairing_signal = any(token in hay for token in ["steak", "pair", "pairing", "potato", "potatoes", "ribeye", "filet"])
        has_style_signal = any(token in hay for token in style_terms)
        return has_wine_signal and (has_pairing_signal or has_style_signal)
    if "espresso" in low or "coffee" in low:
        has_core = any(token in hay for token in ["espresso", "coffee"])
        has_portability = any(token in hay for token in ["portable", "travel", "maker", "machine"])
        return has_core and has_portability
    if "monitor" in low:
        has_monitor = "monitor" in hay or "display" in hay
        has_context = any(token in hay for token in ["macbook", "coding", "programming", "27", "27-inch", "usb-c", "usb c"])
        return has_monitor and has_context
    return topic_overlap_count(result, query) >= minimum_topic_overlap(query)


def build_recommendation_focus_query(query: str, instruction: str) -> str:
    low = f"{instruction} {query}".lower()
    if is_wine_pairing_intent(instruction=instruction, query=query):
        return "wine steak potatoes pairing" if "steak" in low and "potato" in low else "wine pairing"
    if "espresso" in low or "coffee" in low:
        return "portable espresso maker travel review"
    if "monitor" in low:
        size = "27-inch" if "27 inch" in low or "27-inch" in low else "monitor"
        budget_match = re.search(r"\bunder\s+\$?([0-9]{2,4})", low)
        budget = f"under {budget_match.group(1)}" if budget_match else ""
        context_terms: List[str] = []
        if "coding" in low or "programming" in low:
            context_terms.append("coding")
        if "macbook pro" in low:
            context_terms.append("macbook pro")
        elif "macbook" in low:
            context_terms.append("macbook")
        parts = [size, "monitor"]
        if budget:
            parts.append(budget)
        parts.extend(context_terms)
        return re.sub(r"\s+", " ", " ".join(part for part in parts if part)).strip()
    seen: set[str] = set()
    ordered_terms: List[str] = []
    for term in query_focus_terms(query):
        if term in seen:
            continue
        seen.add(term)
        ordered_terms.append(term)
    return " ".join(ordered_terms[:8]).strip()


def search_web(query: str, limit: int = 10) -> List[Any]:
    return list(platform_search_web(query=query, limit=limit))


def relevance_score(result: Any, query: str) -> float:
    q_terms = query_focus_terms(query)
    hay = f"{str(getattr(result, 'title', ''))} {str(getattr(result, 'snippet', ''))}".lower()
    overlap = sum(1 for term in q_terms if term in hay)
    source = str(getattr(result, "source", ""))
    source_bonus = 1.0 if source in {"linkedin", "builtin", "amazon"} else 0.25
    return overlap + source_bonus


def apply_human_judgment_quality_gate(*, ranked: List[Any], instruction: str, query: str, constraints: Dict[str, Any]) -> List[Any]:
    locality_terms = [str(x).lower() for x in (constraints.get("locality_terms") or []) if str(x).strip()]
    scored: List[tuple[Any, int]] = []
    for result in ranked:
        quality = assess_result_quality(
            title=str(getattr(result, "title", "")),
            url=str(getattr(result, "url", "")),
            snippet=str(getattr(result, "snippet", "")),
            query=query,
            locality_terms=locality_terms,
        )
        score = int(quality.score)
        if score <= 0 or not passes_topic_gate(result, instruction, query):
            continue
        scored.append((result, score))
    scored.sort(key=lambda item: (item[1], relevance_score(item[0], query)), reverse=True)
    return [result for result, _score in scored]


def count_locality_matches(ranked: List[Any], locality_terms: List[str]) -> int:
    if not locality_terms:
        return 0
    count = 0
    for result in ranked:
        hay = f"{str(getattr(result, 'title', '')).lower()} {str(getattr(result, 'snippet', '')).lower()} {str(getattr(result, 'url', '')).lower()}"
        if any(str(term).lower() in hay for term in locality_terms):
            count += 1
    return count


def browser_research_walk(*, query: str, candidates: List[Any], browser_worker_mode: str, human_like_interaction: bool, progress_cb: Any = None, max_pages: int = 4) -> Dict[str, Any]:
    from lam.interface import search_agent as search_agent_mod

    return dict(
        search_agent_mod._browser_research_walk(  # type: ignore[attr-defined]
            query=query,
            candidates=candidates,
            browser_worker_mode=browser_worker_mode,
            human_like_interaction=human_like_interaction,
            progress_cb=progress_cb,
            max_pages=max_pages,
        )
    )
