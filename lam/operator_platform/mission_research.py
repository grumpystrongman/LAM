from __future__ import annotations

import re
from typing import Any, Callable, Dict, List

from .mission_contract import MissionContract
from .research_strategist import ResearchStrategist


def normalize_mission_collected_sources(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [dict(item) for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    sources: List[Dict[str, Any]] = []
    raw_sources = payload.get("sources", [])
    if isinstance(raw_sources, list):
        for row in raw_sources:
            if isinstance(row, dict):
                normalized = dict(row)
                normalized["source"] = row.get("source") or row.get("source_name") or row.get("company") or row.get("name") or "source"
                normalized["source_type"] = row.get("source_type") or row.get("type") or "reference"
                normalized["url"] = row.get("url") or row.get("job_url") or row.get("url_or_path") or ""
                normalized["title"] = row.get("title") or row.get("role_title") or row.get("name") or ""
                normalized["snippet"] = row.get("snippet") or row.get("summary") or ""
                sources.append(normalized)
    raw_results = payload.get("search_results", [])
    if isinstance(raw_results, list):
        for row in raw_results[:12]:
            if isinstance(row, dict):
                sources.append(
                    {
                        "source": row.get("source", "search_result"),
                        "source_type": row.get("source", "search_result"),
                        "url": row.get("url", ""),
                        "title": row.get("title", ""),
                        "snippet": row.get("snippet", ""),
                    }
                )
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for row in sources:
        key = f"{row.get('url', '')}|{row.get('title', '')}".lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _mission_query_plan(mission: MissionContract, strategist: ResearchStrategist) -> List[str]:
    base = strategist.candidate_queries(mission, limit=6)
    goal = str(mission.user_goal or "").strip()
    location = str(mission.scope_dimensions.get("location", "") or "").strip()
    extra: List[str] = []
    if mission.mission_type == "job_search_package":
        role_hint = "senior data ai leadership"
        extra.extend(
            [
                f"site:linkedin.com/jobs {role_hint} {location}".strip(),
                f"site:indeed.com {role_hint} {location}".strip(),
                f"site:greenhouse.io {role_hint}".strip(),
                f"site:lever.co {role_hint}".strip(),
                f"{role_hint} company careers {location}".strip(),
            ]
        )
    elif mission.mission_type == "grant_application_package":
        domain_hint = str(mission.scope_dimensions.get("domain", mission.domain)).replace("_", " ")
        extra.extend(
            [
                f"site:grants.gov {domain_hint} grant",
                f"{domain_hint} foundation grant eligibility",
                f"{domain_hint} grant deadline",
                f"{domain_hint} prior award examples",
            ]
        )
    elif mission.mission_type == "executive_research_brief":
        extra.extend(
            [
                f"{goal} official source",
                f"{goal} annual report",
                f"{goal} market size public data",
                f"{goal} company strategy",
            ]
        )
    elif mission.mission_type == "data_storytelling":
        extra.extend(
            [
                f"{goal} dataset dictionary",
                f"{goal} data quality notes",
                f"{goal} methodology",
            ]
        )
    out: List[str] = []
    seen: set[str] = set()
    for item in [*base, *extra]:
        value = " ".join(str(item or "").split()).strip()
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out[:8]


def _specialize_source_type(source: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(source)
    url = str(row.get("url", row.get("url_or_path", "")) or "").lower()
    title = str(row.get("title", row.get("source", "")) or "").lower()
    raw_type = str(row.get("source_type", row.get("type", "")) or "").lower()
    if any(token in url for token in ["linkedin.com/jobs", "indeed.com", "greenhouse.io", "lever.co"]) or "job" in raw_type:
        row["source_type"] = "job_board"
    elif any(token in url for token in ["careers.", "/careers", "jobs."]) or "career" in title:
        row["source_type"] = "company_site"
    elif "grants.gov" in url:
        row["source_type"] = "grant_portal"
    elif any(token in url for token in [".gov", ".org", "foundation"]) and "grant" in f"{url} {title}":
        row["source_type"] = "official"
    elif any(token in url for token in ["sec.gov", "annual", "investor", "earnings"]) or "annual report" in title:
        row["source_type"] = "official"
    elif any(token in url for token in ["data.", "dataset", "data.gov"]):
        row["source_type"] = "public_dataset"
    elif raw_type:
        row["source_type"] = raw_type
    return row


def _source_matches_mission(mission: MissionContract, source: Dict[str, Any]) -> bool:
    row = _specialize_source_type(source)
    text = " ".join(
        [
            str(row.get("source", "")),
            str(row.get("source_type", "")),
            str(row.get("url", row.get("url_or_path", ""))),
            str(row.get("title", "")),
            str(row.get("snippet", "")),
        ]
    ).lower()
    if mission.mission_type == "job_search_package":
        return any(token in text for token in ["job", "career", "hiring", "salary", "recruit", "linkedin", "indeed", "greenhouse", "lever"])
    if mission.mission_type == "grant_application_package":
        return any(token in text for token in ["grant", "funder", "foundation", "eligibility", "award", "grants.gov", "deadline"])
    if mission.mission_type == "executive_research_brief":
        return any(token in text for token in ["market", "company", "official", "annual", "investor", "public", "dataset", "strategy", "report"])
    if mission.mission_type == "data_storytelling":
        return any(token in text for token in ["data", "dataset", "dictionary", "method", "quality", "analysis", "stat"])
    return True


def _post_filter_sources(mission: MissionContract, sources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    filtered: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for item in sources:
        row = _specialize_source_type(item)
        if not _source_matches_mission(mission, row):
            continue
        key = f"{row.get('url', '')}|{row.get('title', '')}".lower()
        if not key or key in seen:
            continue
        seen.add(key)
        filtered.append(row)
    return filtered


def collect_mission_research(
    *,
    mission: MissionContract,
    strategy: Dict[str, Any],
    context: Dict[str, Any],
    collector: Callable[..., Dict[str, Any]],
    strategist: ResearchStrategist | None = None,
) -> Dict[str, Any]:
    strategist = strategist or ResearchStrategist()
    queries = _mission_query_plan(mission, strategist)
    sources: List[Dict[str, Any]] = []
    errors: List[str] = []
    notes: List[Dict[str, Any]] = []
    for query in queries:
        try:
            payload = collector(query=query, instruction=mission.user_goal, contract=mission, strategy=strategy, context=context)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            errors.append(f"{query}:{type(exc).__name__}")
            continue
        if isinstance(payload, dict):
            payload_error = str(payload.get("error", "") or "").strip()
            if payload_error and not bool(payload.get("ok", False)):
                errors.append(f"{query}:{payload_error}")
            notes.append(
                {
                    "query": str(payload.get("query", query)),
                    "ok": bool(payload.get("ok", False)),
                    "error": payload_error,
                    "source_count": len(normalize_mission_collected_sources(payload)),
                }
            )
        sources.extend(normalize_mission_collected_sources(payload))
    filtered_sources = _post_filter_sources(mission, sources)
    return {
        "attempted": True,
        "queries": queries,
        "sources": filtered_sources,
        "errors": errors,
        "notes": notes,
    }
