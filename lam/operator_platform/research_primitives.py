from __future__ import annotations

import json
import re
from dataclasses import asdict
from typing import Any, Dict, List

from lam.interface.browser_worker import ensure_browser_worker, normalize_browser_worker_mode
from lam.interface.human_judgment import EleganceBudget, NegativeMemory, QualityCritic

from .browser_research import browser_query_url
from .recommendation_helpers import (
    build_decision_rows,
    build_product_candidate_rows,
    build_recommendation_summary,
    curated_recommendation_sources,
    merge_browser_notes_into_rows,
)
from .research_backend import (
    apply_human_judgment_quality_gate,
    build_recommendation_focus_query,
    count_locality_matches,
    is_recommendation_research_intent,
    is_wine_pairing_intent,
    relevance_score,
    browser_research_walk,
    search_web,
)

HUMAN_JUDGMENT_SUPERLATIVE_TOKENS = {
    "best",
    "cheapest",
    "lowest",
    "top",
    "recommended",
    "recommend",
    "which one to buy",
    "should i buy",
}

HUMAN_JUDGMENT_LOCALITY_TOKENS = {
    "near me",
    "local",
    "nearby",
    "in durham",
    "in fairfax",
}


def extract_generic_query(instruction: str) -> str:
    raw = re.sub(r"\s+", " ", str(instruction or "")).strip(" .")
    if not raw:
        return ""
    if is_wine_pairing_intent(instruction=raw, query=raw):
        meal_match = re.search(r"\bi am having ([^.?!]+)", raw, flags=re.IGNORECASE)
        if meal_match:
            meal = re.sub(r"\s+", " ", meal_match.group(1)).strip(" .")
            if meal:
                return f"best wine for {meal}"
        foods: List[str] = []
        for token in ["steak", "potatoes", "lamb", "salmon", "pasta", "pizza", "burger"]:
            if re.search(rf"\b{re.escape(token)}\b", raw, flags=re.IGNORECASE):
                foods.append(token)
        if foods:
            if len(foods) == 1:
                return f"best wine for {foods[0]}"
            return f"best wine for {' and '.join(foods[:3])}"
        return "best wine pairing"

    first_sentence = re.split(r"(?<=[.!?])\s+", raw, maxsplit=1)[0]
    q = first_sentence.strip(" .")
    q = re.sub(
        r"^\s*(?:go\s+)?(?:please\s+)?(?:research|find|search(?:\s+for)?|look(?:\s+up)?|help me find|tell me)\s+",
        "",
        q,
        flags=re.IGNORECASE,
    )
    q = re.sub(
        r"\b(?:and\s+)?recommend(?:\s+me)?(?:\s+which\s+one\s+to\s+buy|\s+one\s+to\s+buy|\s+the\s+best\s+one|\s+the\s+best)?\b.*$",
        "",
        q,
        flags=re.IGNORECASE,
    )
    q = re.sub(
        r"\b(?:build|create|make|save|write|draft|summari[sz]e|return|produce|generate|prepare|package|export)\b.*$",
        "",
        q,
        flags=re.IGNORECASE,
    )
    q = re.sub(r"\b(?:and|then)\s+(?:produce|generate|prepare|package|export)\b.*$", "", q, flags=re.IGNORECASE)
    q = re.sub(r"\b(from there|then|and then)\b.*$", "", q, flags=re.IGNORECASE)
    q = re.sub(r"\s+", " ", q).strip(" .,:;")
    return q or raw


def human_judgment_constraints(instruction: str, query: str) -> Dict[str, Any]:
    low = f"{instruction} {query}".lower()
    compare_required = any(token in low for token in HUMAN_JUDGMENT_SUPERLATIVE_TOKENS)
    locality_required = any(token in low for token in HUMAN_JUDGMENT_LOCALITY_TOKENS)
    locality_terms: List[str] = []
    match = re.search(r"\bin\s+([A-Za-z0-9 ,.-]{2,60})", instruction, flags=re.IGNORECASE)
    if match:
        locality_terms.append(re.sub(r"\s+", " ", match.group(1)).strip().lower())
    if "near me" in low:
        locality_terms.append("near me")
    return {
        "compare_required": compare_required,
        "locality_required": locality_required,
        "locality_terms": locality_terms[:5],
    }


def human_judgment_refine_queries(query: str, constraints: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    if constraints.get("compare_required"):
        out.append(f"top options compare {query}")
        out.append(f"price comparison {query}")
    locality_terms = [str(x).strip() for x in (constraints.get("locality_terms") or []) if str(x).strip()]
    for loc in locality_terms[:2]:
        out.append(f"{query} {loc} inventory")
        out.append(f"{query} {loc} price")
    return list(dict.fromkeys([x for x in out if x]))


def expand_queries(query: str, instruction: str = "") -> List[str]:
    base = query.strip()
    low = f"{query} {instruction}".lower()
    variants: List[str] = [base]
    if any(k in low for k in ["job", "position", "salary", "remote", "hiring", "linkedin", "indeed"]):
        variants.extend([f"{base} salary range", f"{base} remote", f"{base} United States", f"{base} Ireland"])
    elif is_recommendation_research_intent(low):
        focus_query = build_recommendation_focus_query(query=query, instruction=instruction)
        if focus_query and focus_query.lower() != base.lower():
            variants.insert(0, focus_query)
        if is_wine_pairing_intent(instruction=instruction, query=query):
            variants.extend(
                [
                    f"{focus_query or base} wine pairing",
                    "cabernet sauvignon steak potatoes",
                    "malbec steak potatoes pairing",
                ]
            )
        variants.extend([f"{focus_query or base} best options", f"{focus_query or base} reviews", f"{focus_query or base} buying guide"])
    else:
        variants.extend([f"{base} market share", f"{base} analysis", f"{base} competitors"])
    out: List[str] = []
    for variant in variants:
        if variant.lower() not in {item.lower() for item in out}:
            out.append(variant)
    return out[:5]


def _count_by(values: Any) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for value in values:
        key = str(value or "").strip() or "unknown"
        counts[key] = counts.get(key, 0) + 1
    return counts


def collect_generic_research(
    *,
    instruction: str,
    browser_worker_mode: str = "local",
    human_like_interaction: bool = False,
) -> Dict[str, Any]:
    query = extract_generic_query(instruction)
    queries = expand_queries(query, instruction=instruction)
    constraints = human_judgment_constraints(instruction=instruction, query=query)
    recommendation_intent = is_recommendation_research_intent(instruction)
    memory = NegativeMemory()
    quality_critic = QualityCritic()
    elegance = EleganceBudget(total=100)
    collected: List[Any] = []
    source_status: Dict[str, str] = {}
    skipped_known_bad = 0
    rejected_low_quality = 0
    opened_url = ""
    browser_notes: List[Dict[str, Any]] = []
    browser_status = ensure_browser_worker(mode=browser_worker_mode)
    source_status["browser_worker"] = (
        f"ok:{browser_status.get('mode', 'local')}"
        if bool(browser_status.get("ok"))
        else f"error:{browser_status.get('error', 'unavailable')}"
    )
    if recommendation_intent and human_like_interaction:
        opened_url = browser_query_url(query)
    for current_query in queries:
        if current_query != query:
            elegance.consume(3, "additional_query_refinement")
        try:
            rows = search_web(current_query, limit=12)
            keep_rows: List[Any] = []
            for row in rows:
                if memory.is_bad_url(row.url):
                    skipped_known_bad += 1
                    elegance.consume(1, "skip_known_bad_url")
                    continue
                keep_rows.append(row)
            collected.extend(keep_rows)
            source_status[current_query] = f"ok:{len(keep_rows)}"
        except Exception as exc:  # pylint: disable=broad-exception-caught
            source_status[current_query] = f"error:{type(exc).__name__}"
    ranked = list({result.url: result for result in collected}.values())
    ranked.sort(key=lambda result: relevance_score(result, query), reverse=True)
    ranked = apply_human_judgment_quality_gate(
        ranked=ranked,
        instruction=instruction,
        query=query,
        constraints=constraints,
    )
    if constraints.get("compare_required") and len(ranked) < 2:
        elegance.consume(6, "quality_refinement_compare_required")
        extra_queries = human_judgment_refine_queries(query=query, constraints=constraints)
        for current_query in extra_queries:
            try:
                rows = search_web(current_query, limit=8)
                collected.extend(rows)
                source_status[current_query] = f"ok:{len(rows)}"
            except Exception as exc:  # pylint: disable=broad-exception-caught
                source_status[current_query] = f"error:{type(exc).__name__}"
        ranked = list({result.url: result for result in collected}.values())
        ranked.sort(key=lambda result: relevance_score(result, query), reverse=True)
        ranked = apply_human_judgment_quality_gate(
            ranked=ranked,
            instruction=instruction,
            query=query,
            constraints=constraints,
        )
    if recommendation_intent and len(ranked) < 2:
        curated = curated_recommendation_sources(instruction=instruction, query=query)
        if curated:
            for row in curated:
                source_status[f"curated:{row.title}"] = "seeded"
            ranked = apply_human_judgment_quality_gate(
                ranked=list({result.url: result for result in ranked + curated}.values()),
                instruction=instruction,
                query=query,
                constraints=constraints,
            )
    ranked_preview = [asdict(x) for x in ranked[:10]]
    if constraints.get("locality_required"):
        local_hits = count_locality_matches(ranked=ranked, locality_terms=constraints.get("locality_terms", []))
        if local_hits <= 0:
            return {
                "ok": False,
                "error": "locality_not_satisfied",
                "query": query,
                "results_count": len(ranked),
                "results": ranked_preview,
                "source_status": source_status,
                "summary": {
                    "detail": "Request requires local relevance, but no strong locality-matching results were found.",
                    "constraints": constraints,
                    "elegance_budget": elegance.snapshot(),
                },
            }
    if constraints.get("compare_required") and len(ranked) < 2:
        return {
            "ok": False,
            "error": "decision_quality_insufficient",
            "query": query,
            "results_count": len(ranked),
            "results": ranked_preview,
            "source_status": source_status,
            "summary": {
                "detail": "Superlative request requires candidate comparison, but too few high-quality candidates were found.",
                "constraints": constraints,
                "elegance_budget": elegance.snapshot(),
            },
        }
    quality_samples: List[float] = []
    for row in ranked[:8]:
        quality = quality_critic.evaluate(
            title=row.title,
            url=row.url,
            snippet=row.snippet,
            query=query,
            locality_terms=constraints.get("locality_terms", []),
            evidence_count=1,
        )
        quality_samples.append(float(quality.score))
    avg_quality = (sum(quality_samples) / len(quality_samples)) if quality_samples else 0.0
    if avg_quality < 1.2:
        elegance.consume(4, "quality_threshold_not_met")
        return {
            "ok": False,
            "error": "quality_threshold_not_met",
            "query": query,
            "results_count": len(ranked),
            "results": ranked_preview,
            "source_status": source_status,
            "summary": {
                "avg_quality": round(avg_quality, 3),
                "constraints": constraints,
                "elegance_budget": elegance.snapshot(),
            },
        }
    top_score = relevance_score(ranked[0], query) if ranked else 0.0
    if top_score < 1.25:
        return {
            "ok": False,
            "error": "low_relevance",
            "query": query,
            "results_count": 0,
            "results": ranked_preview,
            "source_status": source_status,
            "summary": {
                "top_score": round(top_score, 3),
                "query": query,
                "elegance_budget": elegance.snapshot(),
            },
        }
    for row in ranked[10:]:
        quality = quality_critic.evaluate(
            title=row.title,
            url=row.url,
            snippet=row.snippet,
            query=query,
            locality_terms=constraints.get("locality_terms", []),
            evidence_count=0,
        )
        if quality.level == "low":
            memory.mark_bad_url(row.url, ",".join(quality.reasons[:4]) or "low_quality")
            rejected_low_quality += 1
            elegance.consume(1, "reject_low_quality_candidate")
    decision_rows: List[Dict[str, Any]] = []
    recommendation: Dict[str, Any] = {}
    if recommendation_intent or constraints.get("compare_required"):
        if human_like_interaction:
            browser_walk = browser_research_walk(
                query=query,
                candidates=ranked,
                browser_worker_mode=browser_worker_mode,
                human_like_interaction=human_like_interaction,
                progress_cb=None,
                max_pages=4,
            )
            browser_notes = [dict(x) for x in (browser_walk.get("notes") or []) if isinstance(x, dict)]
            if browser_walk.get("opened_url"):
                opened_url = str(browser_walk.get("opened_url"))
            if browser_walk.get("worker_status"):
                source_status["browser_walk"] = str(browser_walk.get("worker_status"))
        decision_rows = build_product_candidate_rows(browser_notes=browser_notes, instruction=instruction, query=query)
        if not decision_rows and recommendation_intent and human_like_interaction:
            curated_sources = curated_recommendation_sources(instruction=instruction, query=query)
            if curated_sources:
                curated_walk = browser_research_walk(
                    query=query,
                    candidates=curated_sources,
                    browser_worker_mode=browser_worker_mode,
                    human_like_interaction=human_like_interaction,
                    progress_cb=None,
                    max_pages=4,
                )
                curated_notes = [dict(x) for x in (curated_walk.get("notes") or []) if isinstance(x, dict)]
                if curated_notes:
                    seen_urls = {str(note.get("url", "")) for note in browser_notes}
                    for note in curated_notes:
                        if str(note.get("url", "")) not in seen_urls:
                            browser_notes.append(note)
                    decision_rows = build_product_candidate_rows(browser_notes=browser_notes, instruction=instruction, query=query)
                if curated_walk.get("opened_url"):
                    opened_url = str(curated_walk.get("opened_url"))
                if curated_walk.get("worker_status"):
                    source_status["browser_walk_curated"] = str(curated_walk.get("worker_status"))
        if not decision_rows:
            decision_rows = build_decision_rows(results=ranked, instruction=instruction, query=query, relevance_fn=relevance_score, is_wine_pairing_fn=is_wine_pairing_intent)
            decision_rows = merge_browser_notes_into_rows(decision_rows=decision_rows, browser_notes=browser_notes)
        recommendation = build_recommendation_summary(
            decision_rows=decision_rows,
            results=ranked,
            instruction=instruction,
            query=query,
        )
        if recommendation.get("selected_url"):
            opened_url = str(recommendation.get("selected_url"))
    summary = {
        "total": len(ranked),
        "sources": _count_by(row.source for row in ranked),
        "constraints": constraints,
        "judgment": {
            "skipped_known_bad": skipped_known_bad,
            "rejected_low_quality": rejected_low_quality,
            "candidate_count": len(ranked),
        },
        "decision_candidate_count": len(decision_rows),
        "browser_pages_reviewed": len(browser_notes),
        "human_like_interaction": bool(human_like_interaction),
        "browser_worker_mode": normalize_browser_worker_mode(browser_worker_mode),
        "elegance_budget": elegance.snapshot(),
    }
    results = [asdict(x) for x in ranked[:50]]
    sources = [
        {
            "name": str(row.get("title", "")),
            "url": str(row.get("url", "")),
            "source_type": str(row.get("source", "")),
            "snippet": str(row.get("snippet", "")),
        }
        for row in results[:20]
    ]
    notes = {
        "objective": instruction,
        "audience": "operator",
        "domain": "generic_research",
        "summary": f"Collected {len(results)} research result(s) for {query}.",
        "findings": [str(recommendation.get("reason", ""))] if recommendation else [f"Collected {len(results)} result(s)."],
        "query": query,
    }
    return {
        "ok": True,
        "query": query,
        "search_results": results,
        "decision_rows": decision_rows,
        "recommendation": recommendation,
        "browser_notes": browser_notes,
        "opened_url": opened_url,
        "research_summary": summary,
        "research_notes": notes,
        "sources": sources,
        "source_status": source_status,
        "error": "",
        "evidence": [notes["summary"], *(["recommendation=" + str(recommendation.get("selected_title", ""))] if recommendation.get("selected_title") else [])],
        "logs": [f"queries={len(queries)}", f"ranked_results={len(results)}"],
    }
