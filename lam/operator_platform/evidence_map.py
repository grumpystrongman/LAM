from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List

from .mission_contract import MissionContract


@dataclass(slots=True)
class EvidenceEntry:
    source: str
    source_type: str
    url_or_path: str
    credibility: float
    relevance: float
    freshness: float
    claim_support: List[str] = field(default_factory=list)
    confidence: float = 0.0
    limitations: List[str] = field(default_factory=list)
    allowed_as_evidence: bool = True
    allowed_as_context: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class SourceQualityScorer:
    def score(self, contract: MissionContract, source: Dict[str, Any]) -> Dict[str, Any]:
        source_type = str(source.get("source_type", source.get("type", "reference"))).lower()
        text = " ".join(
            [
                str(source.get("source", "")),
                source_type,
                str(source.get("url", source.get("url_or_path", ""))),
                str(source.get("title", "")),
                str(source.get("snippet", "")),
            ]
        ).lower()
        scope = contract.scope_dimensions
        relevance = 0.55
        if contract.domain.replace("_", " ")[:8] in text or contract.domain in text:
            relevance += 0.15
        if contract.subdomain and contract.subdomain.replace("_", " ") in text:
            relevance += 0.1
        if scope.get("location") and str(scope.get("location", "")).lower().split(",")[0] in text:
            relevance += 0.1
        if scope.get("service_category") and str(scope.get("service_category", "")).lower() in text:
            relevance += 0.1
        if scope.get("product_category") and str(scope.get("product_category", "")).lower() in text:
            relevance += 0.1

        authority = 0.45
        url = str(source.get("url", source.get("url_or_path", ""))).lower()
        if url.startswith(("https://", "http://", "file://", "user://", "sample://")):
            authority += 0.15
        if any(token in url for token in [".gov", ".org", "official", "grants.gov", "foundation", "careers", "linkedin", "indeed"]):
            authority += 0.15
        if source_type in {"instruction", "user_context", "memory"}:
            authority = min(authority, 0.35)
        freshness = 0.7 if contract.scope_dimensions.get("timeframe") not in {"", "current"} else 0.8
        specificity = 0.55 + min(0.25, 0.05 * len([x for x in [scope.get("location"), scope.get("service_category"), scope.get("product_category"), scope.get("account"), scope.get("file_target")] if x]))
        primary = 0.9 if any(token in url for token in [".gov", "careers", "foundation", "grants.gov", "official"]) or source_type in {"primary", "official", "job_board", "company_site", "grant_portal", "dataset", "public_dataset"} else 0.6
        hallucination_risk = max(0.05, 1.0 - ((relevance + authority + specificity) / 3.0))
        overall = round((relevance * 0.35) + (authority * 0.25) + (freshness * 0.1) + (specificity * 0.2) + (primary * 0.1), 4)
        allowed_as_evidence = overall >= 0.58
        allowed_as_context = overall >= 0.42
        if source_type in {"instruction", "user_context", "memory"}:
            allowed_as_evidence = False
            allowed_as_context = True
        return {
            "overall_score": overall,
            "relevance": round(relevance, 4),
            "authority": round(authority, 4),
            "freshness": round(freshness, 4),
            "specificity": round(specificity, 4),
            "primary_strength": round(primary, 4),
            "hallucination_risk": round(hallucination_risk, 4),
            "allowed_as_evidence": allowed_as_evidence,
            "allowed_as_context": allowed_as_context,
        }


class EvidenceMap:
    def __init__(self) -> None:
        self.entries: List[EvidenceEntry] = []

    def add(self, entry: EvidenceEntry) -> None:
        self.entries.append(entry)

    def add_scored_source(
        self,
        *,
        contract: MissionContract,
        source: Dict[str, Any],
        supported_claims: List[str],
        scorer: SourceQualityScorer | None = None,
    ) -> EvidenceEntry:
        scorer = scorer or SourceQualityScorer()
        scored = scorer.score(contract, source)
        entry = EvidenceEntry(
            source=str(source.get("source", source.get("title", source.get("name", "source")))),
            source_type=str(source.get("source_type", source.get("type", "reference"))),
            url_or_path=str(source.get("url", source.get("url_or_path", ""))),
            credibility=float(scored["authority"]),
            relevance=float(scored["relevance"]),
            freshness=float(scored["freshness"]),
            claim_support=list(supported_claims),
            confidence=float(scored["overall_score"]),
            limitations=list(source.get("limitations", []) or []),
            allowed_as_evidence=bool(scored["allowed_as_evidence"]),
            allowed_as_context=bool(scored["allowed_as_context"]),
        )
        self.add(entry)
        return entry

    def supported_claims(self) -> Dict[str, List[Dict[str, Any]]]:
        coverage: Dict[str, List[Dict[str, Any]]] = {}
        for entry in self.entries:
            for claim in entry.claim_support:
                coverage.setdefault(claim, []).append(entry.to_dict())
        return coverage

    def accepted_sources(self) -> List[Dict[str, Any]]:
        return [entry.to_dict() for entry in self.entries if entry.allowed_as_evidence]

    def rejected_sources(self) -> List[Dict[str, Any]]:
        return [entry.to_dict() for entry in self.entries if not entry.allowed_as_evidence]

    def summary(self) -> Dict[str, Any]:
        accepted = self.accepted_sources()
        rejected = self.rejected_sources()
        context_only = [entry.to_dict() for entry in self.entries if entry.allowed_as_context and not entry.allowed_as_evidence]
        accepted_external = [item for item in accepted if str(item.get("source_type", "")).lower() not in {"instruction", "user_context", "memory"}]
        primary_types = {"primary", "official", "job_board", "company_site", "grant_portal", "dataset", "public_dataset"}
        accepted_primary = [item for item in accepted if str(item.get("source_type", "")).lower() in primary_types or ".gov" in str(item.get("url_or_path", "")).lower()]
        return {
            "source_count": len(self.entries),
            "accepted_count": len(accepted),
            "rejected_count": len(rejected),
            "context_only_count": len(context_only),
            "accepted_external_count": len(accepted_external),
            "accepted_primary_count": len(accepted_primary),
            "claims_supported": len(self.supported_claims()),
            "average_confidence": round(sum(entry.confidence for entry in self.entries) / max(1, len(self.entries)), 4),
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entries": [entry.to_dict() for entry in self.entries],
            "summary": self.summary(),
            "claims": self.supported_claims(),
        }
