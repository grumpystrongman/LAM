from __future__ import annotations

from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse


_ADAPTER_RULES: List[Tuple[str, Dict[str, Any]]] = [
    (
        "youtube.com",
        {
            "adapter_id": "youtube_tutorial",
            "platform": "youtube",
            "authority_level": "community_video",
            "transcript_supported": True,
            "visual_supported": True,
            "refresh_supported": True,
            "version_sensitive": True,
            "trust_tier": 0.72,
            "recency_priority": "high",
            "preferred_refresh_window_days": 30,
            "live_integration": "web_video_search",
            "extraction_modes": ["metadata", "captions", "audio_fallback", "visual_sampling"],
        },
    ),
    (
        "youtu.be",
        {
            "adapter_id": "youtube_tutorial",
            "platform": "youtube",
            "authority_level": "community_video",
            "transcript_supported": True,
            "visual_supported": True,
            "refresh_supported": True,
            "version_sensitive": True,
            "trust_tier": 0.72,
            "recency_priority": "high",
            "preferred_refresh_window_days": 30,
            "live_integration": "web_video_search",
            "extraction_modes": ["metadata", "captions", "audio_fallback", "visual_sampling"],
        },
    ),
    (
        "vimeo.com",
        {
            "adapter_id": "vimeo_tutorial",
            "platform": "vimeo",
            "authority_level": "community_video",
            "transcript_supported": True,
            "visual_supported": True,
            "refresh_supported": True,
            "version_sensitive": True,
            "trust_tier": 0.68,
            "recency_priority": "high",
            "preferred_refresh_window_days": 45,
            "live_integration": "web_video_search",
            "extraction_modes": ["metadata", "captions", "audio_fallback", "visual_sampling"],
        },
    ),
    (
        "loom.com",
        {
            "adapter_id": "loom_demo",
            "platform": "loom",
            "authority_level": "guided_demo",
            "transcript_supported": True,
            "visual_supported": True,
            "refresh_supported": True,
            "version_sensitive": True,
            "trust_tier": 0.64,
            "recency_priority": "medium",
            "preferred_refresh_window_days": 21,
            "live_integration": "browser_capture",
            "extraction_modes": ["metadata", "captions", "visual_sampling"],
        },
    ),
    (
        "learn.microsoft.com",
        {
            "adapter_id": "microsoft_learn",
            "platform": "microsoft_docs",
            "authority_level": "official_docs",
            "transcript_supported": False,
            "visual_supported": False,
            "refresh_supported": True,
            "version_sensitive": True,
            "trust_tier": 0.97,
            "recency_priority": "high",
            "preferred_refresh_window_days": 21,
            "live_integration": "official_docs_search",
            "extraction_modes": ["metadata", "article_sections"],
        },
    ),
    (
        "react.dev",
        {
            "adapter_id": "react_docs",
            "platform": "react_docs",
            "authority_level": "official_docs",
            "transcript_supported": False,
            "visual_supported": False,
            "refresh_supported": True,
            "version_sensitive": True,
            "trust_tier": 0.97,
            "recency_priority": "high",
            "preferred_refresh_window_days": 30,
            "live_integration": "official_docs_search",
            "extraction_modes": ["metadata", "article_sections", "code_examples"],
        },
    ),
    (
        "github.com",
        {
            "adapter_id": "github_reference",
            "platform": "github",
            "authority_level": "reference_repo",
            "transcript_supported": False,
            "visual_supported": False,
            "refresh_supported": True,
            "version_sensitive": True,
            "trust_tier": 0.88,
            "recency_priority": "high",
            "preferred_refresh_window_days": 14,
            "live_integration": "repo_search",
            "extraction_modes": ["metadata", "readme", "source_examples"],
        },
    ),
    (
        "grants.gov",
        {
            "adapter_id": "grants_gov",
            "platform": "grants_gov",
            "authority_level": "official_grants",
            "transcript_supported": False,
            "visual_supported": False,
            "refresh_supported": True,
            "version_sensitive": True,
            "trust_tier": 0.99,
            "recency_priority": "high",
            "preferred_refresh_window_days": 7,
            "live_integration": "official_search",
            "extraction_modes": ["metadata", "opportunity_summary", "eligibility"],
        },
    ),
]


def adapt_source(source: Dict[str, object]) -> Dict[str, object]:
    item = dict(source)
    url = str(item.get("source_url", "") or "").strip()
    stype = str(item.get("source_type", "other") or "other").lower()
    host = urlparse(url).netloc.lower()
    adapter = _adapter_for(url, stype, host)
    item["canonical_url"] = _canonical_url(url)
    item["adapter"] = adapter
    item["platform"] = str(adapter.get("platform", "generic"))
    item["authority_level"] = str(adapter.get("authority_level", "generic"))
    item["adapter_capabilities"] = {
        "metadata": True,
        "transcript": bool(adapter.get("transcript_supported", False)),
        "visual_sampling": bool(adapter.get("visual_supported", False)),
        "refresh_supported": bool(adapter.get("refresh_supported", True)),
        "live_integration": str(adapter.get("live_integration", "none")),
        "extraction_modes": list(adapter.get("extraction_modes", []) or []),
    }
    item["version_sensitive"] = bool(adapter.get("version_sensitive", False))
    item["discovery_mode"] = str(item.get("discovery_mode", "") or ("live" if bool(item.get("live_collected")) else "seeded"))
    item["freshness_window_days"] = int(adapter.get("preferred_refresh_window_days", 30) or 30)
    return item


def adapt_sources(sources: List[Dict[str, object]]) -> List[Dict[str, object]]:
    return [adapt_source(source) for source in sources]


def adapter_summary(sources: List[Dict[str, object]]) -> Dict[str, object]:
    adapted = adapt_sources(sources)
    platforms = sorted({str(item.get("platform", "generic")) for item in adapted if str(item.get("platform", "")).strip()})
    transcript_ready = sum(1 for item in adapted if bool((item.get("adapter_capabilities", {}) or {}).get("transcript", False)))
    visual_ready = sum(1 for item in adapted if bool((item.get("adapter_capabilities", {}) or {}).get("visual_sampling", False)))
    version_sensitive = sum(1 for item in adapted if bool(item.get("version_sensitive", False)))
    official_count = sum(1 for item in adapted if str(item.get("authority_level", "")).startswith("official"))
    live_count = sum(1 for item in adapted if str(item.get("discovery_mode", "")) == "live")
    avg_trust = round(sum(float((item.get("adapter", {}) or {}).get("trust_tier", 0.5) or 0.5) for item in adapted) / max(1, len(adapted)), 3)
    return {
        "platforms": platforms,
        "transcript_ready_count": transcript_ready,
        "visual_ready_count": visual_ready,
        "version_sensitive_count": version_sensitive,
        "official_source_count": official_count,
        "live_source_count": live_count,
        "avg_trust_tier": avg_trust,
        "runtime_quality": _runtime_quality(live_count=live_count, official_count=official_count, transcript_ready=transcript_ready, total=len(adapted)),
    }


def _runtime_quality(*, live_count: int, official_count: int, transcript_ready: int, total: int) -> str:
    if total == 0:
        return "empty"
    if live_count >= 2 and official_count >= 1 and transcript_ready >= 1:
        return "strong_live_mix"
    if live_count >= 1 and transcript_ready >= 1:
        return "usable_live_mix"
    if official_count >= 1:
        return "official_heavy_offline_mix"
    return "heuristic_only"


def _adapter_for(url: str, stype: str, host: str) -> Dict[str, object]:
    for domain, adapter in _ADAPTER_RULES:
        if domain in host:
            return dict(adapter)
    if stype == "docs":
        return {
            "adapter_id": "official_docs",
            "platform": "docs",
            "authority_level": "official_docs",
            "transcript_supported": False,
            "visual_supported": False,
            "refresh_supported": True,
            "version_sensitive": True,
            "trust_tier": 0.92,
            "recency_priority": "high",
            "preferred_refresh_window_days": 30,
            "live_integration": "docs_search",
            "extraction_modes": ["metadata", "article_sections"],
        }
    if stype == "github":
        return {
            "adapter_id": "generic_repo",
            "platform": "github",
            "authority_level": "reference_repo",
            "transcript_supported": False,
            "visual_supported": False,
            "refresh_supported": True,
            "version_sensitive": True,
            "trust_tier": 0.84,
            "recency_priority": "high",
            "preferred_refresh_window_days": 21,
            "live_integration": "repo_search",
            "extraction_modes": ["metadata", "readme", "source_examples"],
        }
    if stype == "video":
        return {
            "adapter_id": "generic_video",
            "platform": "video",
            "authority_level": "community_video",
            "transcript_supported": True,
            "visual_supported": True,
            "refresh_supported": True,
            "version_sensitive": True,
            "trust_tier": 0.66,
            "recency_priority": "high",
            "preferred_refresh_window_days": 45,
            "live_integration": "web_video_search",
            "extraction_modes": ["metadata", "captions", "audio_fallback", "visual_sampling"],
        }
    if stype in {"blog", "forum"}:
        return {
            "adapter_id": f"{stype}_text",
            "platform": stype,
            "authority_level": "community_text",
            "transcript_supported": False,
            "visual_supported": False,
            "refresh_supported": True,
            "version_sensitive": False,
            "trust_tier": 0.52 if stype == "forum" else 0.62,
            "recency_priority": "medium",
            "preferred_refresh_window_days": 60,
            "live_integration": "web_text_search",
            "extraction_modes": ["metadata", "article_sections"],
        }
    return {
        "adapter_id": "generic_source",
        "platform": stype or "other",
        "authority_level": "generic",
        "transcript_supported": False,
        "visual_supported": False,
        "refresh_supported": True,
        "version_sensitive": False,
        "trust_tier": 0.5,
        "recency_priority": "medium",
        "preferred_refresh_window_days": 60,
        "live_integration": "web_search",
        "extraction_modes": ["metadata"],
    }


def _canonical_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return url.strip()
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")
