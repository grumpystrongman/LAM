from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List


@dataclass(slots=True)
class TaskContract:
    user_goal: str
    task_type: str
    domain: str
    subdomain: str
    audience: str
    geography: str
    timeframe: str
    constraints: List[str] = field(default_factory=list)
    requested_outputs: List[str] = field(default_factory=list)
    required_artifacts: List[str] = field(default_factory=list)
    scope_dimensions: Dict[str, Any] = field(default_factory=dict)
    source_rules: List[str] = field(default_factory=list)
    source_requirements: Dict[str, Any] = field(default_factory=dict)
    evidence_requirements: Dict[str, Any] = field(default_factory=dict)
    data_sufficiency_requirements: Dict[str, Any] = field(default_factory=dict)
    safety_rules: List[str] = field(default_factory=list)
    safety_requirements: Dict[str, Any] = field(default_factory=dict)
    allowed_fallbacks: List[str] = field(default_factory=list)
    completion_criteria: List[str] = field(default_factory=list)
    success_criteria: List[str] = field(default_factory=list)
    invalidation_keys: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload.setdefault("scope_dimensions", {})
        payload["scope_dimensions"].setdefault("location", self.geography)
        payload["scope_dimensions"].setdefault("timeframe", self.timeframe)
        payload["scope_dimensions"].setdefault("domain", self.domain)
        payload["scope_dimensions"].setdefault("audience", self.audience)
        return payload


class TaskContractEngine:
    def extract(self, instruction: str, context: Dict[str, Any] | None = None) -> TaskContract:
        text = re.sub(r"\s+", " ", str(instruction or "")).strip()
        low = text.lower()
        context = context or {}
        domain = self._extract_domain(low)
        task_type = self._extract_task_type(low, domain)
        subdomain = self._extract_subdomain(low, domain)
        geography = self._extract_geography(text)
        timeframe = self._extract_timeframe(text)
        audience = self._extract_audience(low)
        requested_outputs = self._extract_outputs(low)
        constraints = self._extract_constraints(low)
        source_rules = self._extract_source_rules(low, domain)
        safety_rules = self._extract_safety_rules(low)
        scope_dimensions = self._extract_scope_dimensions(
            text=text,
            low=low,
            domain=domain,
            audience=audience,
            geography=geography,
            timeframe=timeframe,
            requested_outputs=requested_outputs,
            context=context,
        )
        required_artifacts = self._required_artifacts_for_outputs(requested_outputs, scope_dimensions)
        source_requirements = self._extract_source_requirements(low, domain, scope_dimensions)
        evidence_requirements = self._extract_evidence_requirements(domain, scope_dimensions, source_requirements)
        data_sufficiency_requirements = self._extract_data_sufficiency_requirements(domain, scope_dimensions, requested_outputs)
        safety_requirements = self._extract_safety_requirements(low, domain, scope_dimensions)
        allowed_fallbacks = self._extract_allowed_fallbacks(low, domain, requested_outputs)
        completion_criteria = self._extract_completion(low, requested_outputs)
        success_criteria = self._extract_success_criteria(requested_outputs, scope_dimensions, domain)
        invalidation_keys = self._build_invalidation_keys(
            domain=domain,
            audience=audience,
            timeframe=timeframe,
            requested_outputs=requested_outputs,
            scope_dimensions=scope_dimensions,
            context=context,
        )
        return TaskContract(
            user_goal=text,
            task_type=task_type,
            domain=domain,
            subdomain=subdomain,
            audience=audience,
            geography=geography,
            timeframe=timeframe,
            constraints=constraints,
            requested_outputs=requested_outputs,
            required_artifacts=required_artifacts,
            scope_dimensions=scope_dimensions,
            source_rules=source_rules,
            source_requirements=source_requirements,
            evidence_requirements=evidence_requirements,
            data_sufficiency_requirements=data_sufficiency_requirements,
            safety_rules=safety_rules,
            safety_requirements=safety_requirements,
            allowed_fallbacks=allowed_fallbacks,
            completion_criteria=completion_criteria,
            success_criteria=success_criteria,
            invalidation_keys=invalidation_keys,
        )

    def artifact_matches_contract(self, artifact_metadata: Dict[str, Any], contract: TaskContract) -> bool:
        if not isinstance(artifact_metadata, dict):
            return False
        meta = artifact_metadata.get("task_contract", {}) if isinstance(artifact_metadata.get("task_contract"), dict) else artifact_metadata
        existing_scope = meta.get("scope_dimensions", {}) if isinstance(meta.get("scope_dimensions"), dict) else {}
        for key, value in contract.invalidation_keys.items():
            if not str(value).strip():
                continue
            existing = str(meta.get("invalidation_keys", {}).get(key, meta.get(key, existing_scope.get(key, "")))).strip().lower()
            if existing and existing != str(value).strip().lower():
                return False
        return True

    def write_contract(self, path: str | Path, contract: TaskContract) -> Path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(contract.to_dict(), indent=2), encoding="utf-8")
        return target

    def _extract_domain(self, low: str) -> str:
        if any(token in low for token in ["payer", "insurance", "plan pricing", "transparency in coverage", "negotiated rate"]):
            return "payer_pricing_review"
        if any(token in low for token in ["inbox", "gmail", "draft replies", "email triage"]):
            return "email_triage"
        if any(token in low for token in ["linkedin", "indeed", "job board", "job boards", "salary", "vp of data", "vp data", "avp data"]):
            return "job_market"
        if any(token in low for token in ["competitor", "competitors", "oracle health", "epic systems", "market landscape"]):
            return "competitor_analysis"
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
        if any(token in low for token in ["spreadsheet cleanup", "clean spreadsheet", "csv cleanup", "workbook cleanup"]):
            return "spreadsheet_cleanup"
        if any(token in low for token in ["whiskey", "wine", "ebay", "best price", "buy", "shopping", "local purchase"]):
            return "retail_product_research"
        return "web_research"

    def _extract_task_type(self, low: str, domain: str) -> str:
        if domain in {"payer_pricing_review", "deep_analysis", "competitor_analysis", "job_market"}:
            return "analysis"
        if domain == "email_triage":
            return "operations"
        if domain in {"presentation_build", "ui_build"}:
            return "build"
        if domain == "spreadsheet_cleanup":
            return "file_operation"
        if domain == "retail_product_research":
            return "research"
        return "research"

    def _extract_subdomain(self, low: str, domain: str) -> str:
        if domain == "payer_pricing_review" and "imaging" in low:
            return "outpatient_imaging"
        if domain == "retail_product_research" and "whiskey" in low:
            return "local_whiskey"
        if domain == "email_triage":
            return "gmail"
        if domain == "spreadsheet_cleanup":
            return "tabular_cleanup"
        return ""

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
        for city, state in [("fairfax", "VA"), ("durham", "NC"), ("houston", "TX")]:
            if city in low:
                return f"{city.title()}, {state}"
        return ""

    def _extract_timeframe(self, text: str) -> str:
        match = re.search(r"(last\s+\d+\s*(?:hours|days|weeks|months))", text, flags=re.IGNORECASE)
        if match:
            return match.group(1).lower()
        match = re.search(r"\b(today|tonight|this week|this month|current)\b", text, flags=re.IGNORECASE)
        if match:
            return match.group(1).lower()
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
            "spreadsheet": ["spreadsheet", "csv", "xlsx", "workbook", "sheet"],
            "report": ["report", "summary", "brief", "memo"],
            "dashboard": ["dashboard", "html report"],
            "presentation": ["presentation", "slide", "slides", "powerpoint", "deck"],
            "rag_index": ["rag", "vector store", "retriever", "index"],
            "code": ["code", "script", "cli", "web app", "analysis app"],
            "ui": ["ui", "frontend", "component", "app shell"],
            "draft_replies": ["draft replies", "drafts"],
            "task_spreadsheet": ["task spreadsheet", "task list"],
            "recommendation": ["recommendation", "recommend"],
            "evidence": ["evidence", "citations", "sources"],
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
        if "no patient-level" in low:
            constraints.append("no patient-level data")
        if "do not" in low or "don't" in low:
            constraints.append("explicit negative constraints present")
        if "stale" in low:
            constraints.append("reject stale outputs")
        return constraints

    def _extract_source_rules(self, low: str, domain: str) -> List[str]:
        rules = ["preserve source references"]
        if "public" in low:
            rules.append("public-source only")
        if "synthetic" in low:
            rules.append("allow synthetic fallback when labeled")
        if domain == "email_triage":
            rules.append("use authenticated inbox sources, not public web sources")
        return rules

    def _extract_safety_rules(self, low: str) -> List[str]:
        rules = ["avoid unsupported claims", "respect approval gates for destructive actions"]
        if "no phi" in low:
            rules.append("no PHI")
        if "no patient-level" in low:
            rules.append("no patient-level data")
        return rules

    def _extract_scope_dimensions(
        self,
        *,
        text: str,
        low: str,
        domain: str,
        audience: str,
        geography: str,
        timeframe: str,
        requested_outputs: List[str],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        account = self._extract_account(text, context)
        file_target = self._extract_file_target(text, context)
        service_category = self._extract_service_category(low, domain)
        product_category = self._extract_product_category(low, domain)
        availability = "local purchase" if any(token in low for token in ["near me", "local", "locally", "today", "tonight"]) else ""
        scope = {
            "location": geography,
            "timeframe": timeframe,
            "domain": domain,
            "service_category": service_category,
            "product_category": product_category,
            "availability": availability,
            "audience": audience,
            "account": account,
            "file_target": file_target,
            "data_type": self._extract_data_type(low, domain),
            "output_type": requested_outputs,
        }
        if context.get("project_id"):
            scope["project_id"] = str(context.get("project_id"))
        return {
            key: value
            for key, value in scope.items()
            if value is not None and value != "" and value != [] and value != {}
        }

    def _extract_source_requirements(self, low: str, domain: str, scope_dimensions: Dict[str, Any]) -> Dict[str, Any]:
        allowed_source_types = ["public_url", "local_file", "user_file", "browser_result"]
        if domain == "email_triage":
            allowed_source_types = ["authenticated_account_session", "email_message"]
        elif domain == "payer_pricing_review":
            allowed_source_types = ["public_provider_pricing", "public_payer_reference", "local_transparency_file", "synthetic_fixture"]
        elif domain == "spreadsheet_cleanup":
            allowed_source_types = ["target_spreadsheet_file"]
        return {
            "allowed_source_types": allowed_source_types,
            "location_required": bool(scope_dimensions.get("location")),
            "account_required": bool(scope_dimensions.get("account")),
            "file_target_required": bool(scope_dimensions.get("file_target")),
            "freshness_required": bool(scope_dimensions.get("timeframe") not in {"", "current"}),
        }

    def _extract_evidence_requirements(self, domain: str, scope_dimensions: Dict[str, Any], source_requirements: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "direct_scope_match_required": True,
            "source_type_fit_required": True,
            "min_credible_sources": 1 if domain in {"email_triage", "spreadsheet_cleanup"} else 2,
            "scope_dimensions_checked": sorted(scope_dimensions.keys()),
            "source_requirements": dict(source_requirements),
        }

    def _extract_data_sufficiency_requirements(self, domain: str, scope_dimensions: Dict[str, Any], outputs: List[str]) -> Dict[str, Any]:
        if domain == "payer_pricing_review":
            return {"min_valid_records": 1, "min_usable_sources": 1, "needs_comparable_entities": True}
        if domain == "email_triage":
            return {"min_messages": 1, "timeframe_required": bool(scope_dimensions.get("timeframe"))}
        if domain == "spreadsheet_cleanup":
            return {"min_rows": 1, "target_file_required": True}
        if domain == "retail_product_research":
            return {"min_candidates": 2, "needs_local_availability": bool(scope_dimensions.get("availability"))}
        return {"min_sources": 2 if "report" in outputs or "recommendation" in outputs else 1}

    def _extract_safety_requirements(self, low: str, domain: str, scope_dimensions: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "approval_required": any(token in low for token in ["send", "delete", "purchase", "submit"]),
            "credential_sensitive": domain == "email_triage" or bool(scope_dimensions.get("account")),
            "no_private_data": "no phi" in low or domain == "payer_pricing_review",
        }

    def _extract_allowed_fallbacks(self, low: str, domain: str, outputs: List[str]) -> List[str]:
        fallbacks: List[str] = []
        if any(token in low for token in ["synthetic", "demo", "fallback"]):
            fallbacks.append("demo_complete")
        if domain in {"payer_pricing_review", "deep_analysis"}:
            fallbacks.append("template_complete")
            fallbacks.append("real_partial")
        if "report" in outputs or "presentation" in outputs:
            fallbacks.append("no_result_found_with_sufficient_search")
        return list(dict.fromkeys(fallbacks))

    def _extract_completion(self, low: str, outputs: List[str]) -> List[str]:
        criteria = ["requested deliverables created", "outputs verifiable and source-backed"]
        if "test" in low:
            criteria.append("tests executed")
        if "fix" in low:
            criteria.append("failures corrected before completion")
        if "presentation" in outputs:
            criteria.append("presentation outline included")
        return criteria

    def _extract_success_criteria(self, outputs: List[str], scope_dimensions: Dict[str, Any], domain: str) -> List[str]:
        criteria = ["final output is truthful about evidence quality and completeness"]
        if outputs:
            criteria.append(f"requested outputs delivered: {', '.join(outputs)}")
        if scope_dimensions:
            criteria.append(f"scope respected: {', '.join(sorted(scope_dimensions.keys()))}")
        if domain == "email_triage":
            criteria.append("message evidence comes from the target account and timeframe")
        return criteria

    def _build_invalidation_keys(
        self,
        *,
        domain: str,
        audience: str,
        timeframe: str,
        requested_outputs: List[str],
        scope_dimensions: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Dict[str, str]:
        invalidation_keys = {
            "domain": domain,
            "timeframe": timeframe,
            "audience": audience,
            "outputs": "|".join(sorted(requested_outputs)),
        }
        for key, value in scope_dimensions.items():
            if isinstance(value, list):
                invalidation_keys[key] = "|".join(str(item) for item in value)
            elif value not in {"", None}:
                invalidation_keys[key] = str(value)
        for key in ["account", "file_target", "project_id", "risk_level"]:
            value = str(context.get(key, "")).strip()
            if value:
                invalidation_keys[key] = value
        return invalidation_keys

    def _required_artifacts_for_outputs(self, outputs: List[str], scope_dimensions: Dict[str, Any]) -> List[str]:
        mapping = {
            "spreadsheet": "workbook_xlsx",
            "task_spreadsheet": "workbook_xlsx",
            "report": "summary_report_md",
            "dashboard": "dashboard_html",
            "presentation": "powerpoint_pptx",
            "rag_index": "rag_index_db",
            "code": "analysis_script_py",
            "ui": "ui_spec_json",
            "draft_replies": "drafts_json",
        }
        required = [mapping[item] for item in outputs if item in mapping]
        if scope_dimensions.get("location") and "spreadsheet" in outputs:
            slug = re.sub(r"[^a-z0-9]+", "_", str(scope_dimensions.get("location", "")).lower()).strip("_")
            if slug:
                required.append(f"{slug}_stakeholder_package")
        return list(dict.fromkeys(required))

    def _extract_account(self, text: str, context: Dict[str, Any]) -> str:
        match = re.search(r"([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})", text, flags=re.IGNORECASE)
        if match:
            return match.group(1)
        value = str(context.get("account", "")).strip()
        return value

    def _extract_file_target(self, text: str, context: Dict[str, Any]) -> str:
        match = re.search(r"([A-Za-z]:\\[^\"'\s]+(?:\.[A-Za-z0-9]+)?)", text)
        if match:
            return match.group(1)
        match = re.search(r"\bfile\s+([A-Za-z0-9._-]+\.(?:csv|xlsx|xls|json|md|txt))\b", text, flags=re.IGNORECASE)
        if match:
            return match.group(1)
        value = str(context.get("file", context.get("file_target", ""))).strip()
        return value

    def _extract_service_category(self, low: str, domain: str) -> str:
        if domain == "payer_pricing_review" and any(token in low for token in ["outpatient imaging", "mri", "ct", "radiology", "ultrasound", "diagnostic imaging"]):
            return "outpatient imaging"
        return ""

    def _extract_product_category(self, low: str, domain: str) -> str:
        if domain == "retail_product_research":
            for token in ["whiskey", "wine", "monitor", "espresso", "cyberdeck"]:
                if token in low:
                    return token
        return ""

    def _extract_data_type(self, low: str, domain: str) -> str:
        if domain == "payer_pricing_review":
            return "public payer/provider pricing"
        if domain == "email_triage":
            return "email messages"
        if domain == "spreadsheet_cleanup":
            return "tabular file"
        if domain == "retail_product_research":
            return "retail product evidence"
        return "mixed evidence"
