from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List


@dataclass(slots=True)
class TaskContract:
    user_goal: str
    audience: str
    domain: str
    geography: str
    timeframe: str
    constraints: List[str] = field(default_factory=list)
    requested_outputs: List[str] = field(default_factory=list)
    source_rules: List[str] = field(default_factory=list)
    safety_rules: List[str] = field(default_factory=list)
    completion_criteria: List[str] = field(default_factory=list)
    invalidation_keys: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class TaskContractEngine:
    def extract(self, instruction: str, context: Dict[str, Any] | None = None) -> TaskContract:
        text = re.sub(r"\s+", " ", str(instruction or "")).strip()
        low = text.lower()
        geography = self._extract_geography(text)
        timeframe = self._extract_timeframe(text)
        audience = self._extract_audience(low)
        domain = self._extract_domain(low)
        requested_outputs = self._extract_outputs(low)
        constraints = self._extract_constraints(low)
        source_rules = self._extract_source_rules(low)
        safety_rules = self._extract_safety_rules(low)
        completion_criteria = self._extract_completion(low, requested_outputs)
        invalidation_keys = {
            "geography": geography,
            "domain": domain,
            "timeframe": timeframe,
            "audience": audience,
            "outputs": "|".join(sorted(requested_outputs)),
        }
        if context and isinstance(context, dict):
            for key in ["account", "file", "payer_market", "risk_level"]:
                value = str(context.get(key, "")).strip()
                if value:
                    invalidation_keys[key] = value
        return TaskContract(
            user_goal=text,
            audience=audience,
            domain=domain,
            geography=geography,
            timeframe=timeframe,
            constraints=constraints,
            requested_outputs=requested_outputs,
            source_rules=source_rules,
            safety_rules=safety_rules,
            completion_criteria=completion_criteria,
            invalidation_keys=invalidation_keys,
        )

    def artifact_matches_contract(self, artifact_metadata: Dict[str, Any], contract: TaskContract) -> bool:
        if not isinstance(artifact_metadata, dict):
            return False
        meta = artifact_metadata.get("task_contract", {}) if isinstance(artifact_metadata.get("task_contract"), dict) else artifact_metadata
        for key, value in contract.invalidation_keys.items():
            existing = str(meta.get("invalidation_keys", {}).get(key, meta.get(key, ""))).strip().lower()
            if existing and existing != str(value).strip().lower():
                return False
        return True

    def write_contract(self, path: str | Path, contract: TaskContract) -> Path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(contract.to_dict(), indent=2), encoding="utf-8")
        return target

    def _extract_domain(self, low: str) -> str:
        if any(token in low for token in ["payer", "insurance", "plan pricing", "transparency in coverage"]):
            return "payer_pricing_review"
        if any(token in low for token in ["inbox", "gmail", "draft replies", "email triage"]):
            return "email_triage"
        if any(
            token in low
            for token in [
                "rag",
                "vector store",
                "retriever",
                "code",
                "test",
                "analysis app",
                "data science",
                "statistical analysis",
                "deep analysis",
                "write and test",
                "fix failures",
            ]
        ):
            return "deep_analysis"
        if any(token in low for token in ["presentation", "slide", "powerpoint", "deck"]):
            return "presentation_build"
        if any(token in low for token in ["ui", "frontend", "dashboard", "component", "app shell"]):
            return "ui_build"
        return "web_research"

    def _extract_geography(self, text: str) -> str:
        match = re.search(r"\b(?:for|in)\s+([A-Z][A-Za-z .'-]+,\s*[A-Z]{2})\b", text)
        if match:
            return match.group(1).strip()
        match = re.search(r"\b([A-Z][A-Za-z .'-]+,\s*[A-Z]{2})\b", text)
        if match:
            candidate = match.group(1).strip()
            if len(candidate.split()) <= 4:
                return candidate
        low = text.lower()
        if "fairfax" in low:
            return "Fairfax, VA"
        if "durham" in low:
            return "Durham, NC"
        return ""

    def _extract_timeframe(self, text: str) -> str:
        match = re.search(r"(last\s+\d+\s*(?:hours|days|weeks|months))", text, flags=re.IGNORECASE)
        if match:
            return match.group(1).lower()
        if re.search(r"\b(today|this week|this month|current)\b", text, flags=re.IGNORECASE):
            return re.search(r"\b(today|this week|this month|current)\b", text, flags=re.IGNORECASE).group(1).lower()
        return "current"

    def _extract_audience(self, low: str) -> str:
        if any(token in low for token in ["stakeholder", "executive", "leadership", "board", "client"]):
            return "stakeholder"
        if any(token in low for token in ["engineer", "developer", "repo", "code review"]):
            return "technical"
        return "operator"

    def _extract_outputs(self, low: str) -> List[str]:
        outputs: List[str] = []
        def _matches(token: str) -> bool:
            escaped = re.escape(token)
            if re.fullmatch(r"[a-z0-9][a-z0-9 -]*", token):
                return re.search(rf"\b{escaped}\b", low) is not None
            return token in low

        output_map = {
            "spreadsheet": ["spreadsheet", "csv", "xlsx", "workbook"],
            "report": ["report", "summary", "brief", "memo"],
            "dashboard": ["dashboard", "html report"],
            "presentation": ["presentation", "slide", "slides", "powerpoint", "deck"],
            "rag_index": ["rag", "vector store", "retriever", "index"],
            "code": ["code", "script", "cli", "web app", "analysis app"],
            "ui": ["ui", "frontend", "component", "app shell"],
        }
        for name, tokens in output_map.items():
            if any(_matches(token) for token in tokens):
                outputs.append(name)
        return outputs or ["report"]

    def _extract_constraints(self, low: str) -> List[str]:
        constraints: List[str] = []
        if "public" in low:
            constraints.append("prefer public sources")
        if "no phi" in low:
            constraints.append("no PHI")
        if "do not" in low or "don't" in low:
            constraints.append("explicit negative constraints present")
        if "stale" in low:
            constraints.append("reject stale outputs")
        return constraints

    def _extract_source_rules(self, low: str) -> List[str]:
        rules = ["preserve source references"]
        if "public" in low:
            rules.append("public-source only")
        if "synthetic" in low:
            rules.append("allow synthetic fallback when labeled")
        return rules

    def _extract_safety_rules(self, low: str) -> List[str]:
        rules = ["avoid unsupported claims", "respect approval gates for destructive actions"]
        if "no phi" in low:
            rules.append("no PHI")
        if "no patient-level" in low:
            rules.append("no patient-level data")
        return rules

    def _extract_completion(self, low: str, outputs: List[str]) -> List[str]:
        criteria = ["requested deliverables created", "outputs verifiable and source-backed"]
        if "test" in low:
            criteria.append("tests executed")
        if "fix" in low:
            criteria.append("failures corrected before completion")
        if "presentation" in outputs:
            criteria.append("presentation outline included")
        return criteria
