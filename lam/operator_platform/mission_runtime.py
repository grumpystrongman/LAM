from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Dict, List

from .artifact_specific_critics import (
    CompletionCritic,
    CoverLetterCritic,
    DataStoryCritic,
    ExecutiveBriefCritic,
    GrantProposalCritic,
    JobFitCritic,
    PresentationCritic,
    ResearchQualityCritic,
    ResumeCritic,
    SourceCredibilityCritic,
    StatisticalAnalysisCritic,
    UIUXCritic,
)
from .evidence_map import EvidenceMap
from .mission_contract import MissionContract, MissionContractEngine
from .mission_research import collect_mission_research, normalize_mission_collected_sources
from .research_strategist import ResearchStrategist
from .revision_runtime import RevisionRuntime
from .task_contract_engine import TaskContractEngine
from .user_project_memory import UserProjectMemory
from .work_product_engine import WorkProductEngine


class MissionRuntime:
    def __init__(
        self,
        *,
        mission_contract_engine: MissionContractEngine | None = None,
        task_contract_engine: TaskContractEngine | None = None,
        strategist: ResearchStrategist | None = None,
        engine: WorkProductEngine | None = None,
        revision_runtime: RevisionRuntime | None = None,
        memory: UserProjectMemory | None = None,
        source_collector: Callable[..., Dict[str, Any]] | None = None,
    ) -> None:
        self.mission_contract_engine = mission_contract_engine or MissionContractEngine()
        self.task_contract_engine = task_contract_engine or TaskContractEngine()
        self.strategist = strategist or ResearchStrategist()
        self.engine = engine or WorkProductEngine()
        self.revision_runtime = revision_runtime or RevisionRuntime(engine=self.engine)
        self.memory = memory or UserProjectMemory()
        self.source_collector = source_collector

    def run(self, instruction: str, context: Dict[str, Any] | None = None) -> Dict[str, Any]:
        context = dict(context or {})
        mission = self.mission_contract_engine.extract(instruction, context=context)
        task_contract = self.task_contract_engine.extract(instruction, context=context).to_dict()
        task_contract.update(self.mission_contract_engine.to_task_contract_patch(mission))
        workspace = Path(str(context.get("workspace_dir") or Path("data/mission_runs") / self._slug(mission.user_goal)))
        workspace.mkdir(parents=True, exist_ok=True)
        strategy = self.strategist.build(mission)
        memory_context = self.memory.retrieve_for_mission(
            mission_contract=mission,
            query=mission.user_goal,
            project_id=str(context.get("project_id", "") or ""),
            allow_sensitive=bool(context.get("allow_sensitive_memory", False)),
            limit=8,
        )
        evidence_map, evidence_diagnostics = self._build_evidence_map(
            mission,
            strategy=strategy,
            context=context,
            memory_context=memory_context,
        )
        artifacts, artifact_metadata = self.engine.build(
            contract=mission,
            strategy=strategy,
            evidence_map=evidence_map.to_dict(),
            memory_context=memory_context,
            workspace_dir=workspace,
            source_records=list(evidence_diagnostics.get("normalized_sources", []) or []),
            extra_context=context,
        )
        critic_results, revisions = self._critique_and_revise(mission, strategy, evidence_map.to_dict(), artifacts)
        completion = CompletionCritic().evaluate(mission.artifact_plan, artifacts).to_dict()
        critic_results["completion"] = completion
        status = self._mission_status(mission, strategy, evidence_map.to_dict(), evidence_diagnostics, critic_results)
        recovery = self._recovery_payload(mission, status, evidence_diagnostics, critic_results)
        final_package = {
            "status": status,
            "summary": self._summary_text(mission, status, evidence_map.to_dict()),
            "next_steps": self._next_steps(mission, status),
        }
        result = {
            "ok": status not in {"failed_validation", "failed_execution"},
            "mode": "mission_runtime",
            "instruction": instruction,
            "mission_contract": mission.to_dict(),
            "task_contract": task_contract,
            "deliverable_mode": mission.deliverable_mode,
            "research_strategy": strategy,
            "evidence_map": evidence_map.to_dict(),
            "evidence_diagnostics": evidence_diagnostics,
            "artifacts": artifacts,
            "artifact_metadata": artifact_metadata,
            "artifact_plan": list(mission.artifact_plan),
            "critics": {"mission": critic_results},
            "revisions_performed": revisions,
            "memory_context": memory_context,
            "mission_status": status,
            "output_truth": {
                "status": status,
                "real_evidence_sources": int((evidence_map.summary() or {}).get("accepted_external_count", 0) or 0),
                "attempted_collection": bool(evidence_diagnostics.get("collection_attempted", False)),
                "reason": recovery.get("reason", ""),
            },
            "recovery": recovery,
            "final_package": final_package,
            "summary": {
                "mission_type": mission.mission_type,
                "deliverable_mode": mission.deliverable_mode,
                "accepted_sources": int((evidence_map.summary() or {}).get("accepted_count", 0) or 0),
                "accepted_external_sources": int((evidence_map.summary() or {}).get("accepted_external_count", 0) or 0),
                "artifacts_created": len(artifacts),
            },
            "opened_url": Path(next(iter(artifacts.values()), str(workspace.resolve()))).resolve().as_uri() if artifacts else "",
        }
        return result

    def _build_evidence_map(
        self,
        mission: MissionContract,
        *,
        strategy: Dict[str, Any],
        context: Dict[str, Any],
        memory_context: Dict[str, Any],
    ) -> tuple[EvidenceMap, Dict[str, Any]]:
        evidence_map = EvidenceMap()
        seed_sources = list(context.get("seed_sources", []) or [])
        expected_claims = list(strategy.get("expected_evidence", [])[:4])
        diagnostics: Dict[str, Any] = {
            "collection_attempted": False,
            "collection_queries": [],
            "collected_source_count": 0,
            "collector_errors": [],
            "seed_source_count": len(seed_sources),
            "normalized_sources": [],
        }
        for row in seed_sources:
            if isinstance(row, dict):
                evidence_map.add_scored_source(contract=mission, source=row, supported_claims=expected_claims)
        collected_sources = self._collect_sources(mission, strategy=strategy, context=context)
        diagnostics["collection_attempted"] = bool(collected_sources.get("attempted", False))
        diagnostics["collection_queries"] = list(collected_sources.get("queries", []) or [])
        diagnostics["collector_errors"] = list(collected_sources.get("errors", []) or [])
        normalized_sources = list(collected_sources.get("sources", []) or [])
        diagnostics["normalized_sources"] = normalized_sources
        diagnostics["collected_source_count"] = len(normalized_sources)
        for row in normalized_sources:
            if isinstance(row, dict):
                evidence_map.add_scored_source(contract=mission, source=row, supported_claims=expected_claims)
        for item in list(memory_context.get("used", []) or [])[:4]:
            evidence_map.add_scored_source(
                contract=mission,
                source={
                    "source": f"memory:{item.get('type', 'context')}",
                    "source_type": "user_context",
                    "url_or_path": f"user://memory/{item.get('memory_id', '')}",
                    "title": str(item.get("type", "")),
                    "snippet": json.dumps(item.get("content", {}))[:220],
                },
                supported_claims=expected_claims[:2] or ["context_support"],
            )
        if not evidence_map.entries:
            evidence_map.add_scored_source(
                contract=mission,
                source={
                    "source": "user_instruction",
                    "source_type": "instruction",
                    "url_or_path": "user://instruction",
                    "title": mission.user_goal,
                    "snippet": mission.user_goal,
                },
                supported_claims=expected_claims[:1] or ["mission_scope"],
            )
        return evidence_map, diagnostics

    def _collect_sources(self, mission: MissionContract, *, strategy: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        collector = context.get("source_collector") or self.source_collector
        if collector is None:
            return {"attempted": False, "queries": [], "sources": [], "errors": []}
        return collect_mission_research(
            mission=mission,
            strategy=strategy,
            context=context,
            collector=collector,
            strategist=self.strategist,
        )

    def _normalize_collected_sources(self, payload: Any) -> List[Dict[str, Any]]:
        return normalize_mission_collected_sources(payload)

    def _critique_and_revise(
        self,
        mission: MissionContract,
        strategy: Dict[str, Any],
        evidence_map: Dict[str, Any],
        artifacts: Dict[str, str],
    ) -> tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
        results: Dict[str, Dict[str, Any]] = {
            "research_quality": ResearchQualityCritic().evaluate(strategy, evidence_map).to_dict(),
            "source_credibility": SourceCredibilityCritic().evaluate(evidence_map).to_dict(),
        }
        revisions: List[Dict[str, Any]] = []
        for key, path in artifacts.items():
            critic_name = ""
            evaluator = None
            if key.startswith("resume"):
                critic_name, evaluator = "resume", ResumeCritic().evaluate
            elif key.startswith("cover_letter"):
                critic_name, evaluator = "cover_letter", CoverLetterCritic().evaluate
            elif key.startswith("proposal"):
                critic_name, evaluator = "grant_proposal", GrantProposalCritic().evaluate
            elif key.startswith("executive_brief"):
                critic_name, evaluator = "executive_brief", ExecutiveBriefCritic().evaluate
            elif key.startswith("data_story"):
                critic_name, evaluator = "data_story", DataStoryCritic().evaluate
            elif key.startswith("presentation"):
                critic_name, evaluator = "presentation", PresentationCritic().evaluate
            elif key.startswith("ui_spec"):
                critic_name, evaluator = "uiux", lambda text: UIUXCritic().evaluate(text)
            elif key.startswith("dashboard"):
                critic_name, evaluator = "uiux", lambda text: UIUXCritic().evaluate(text)
            elif key.startswith("code"):
                critic_name, evaluator = "statistical_analysis", lambda text: StatisticalAnalysisCritic().evaluate(text)
            elif key.startswith("job_tracker") and key.endswith("_csv"):
                rows = self._read_csv_rows(path)
                results["job_fit"] = JobFitCritic().evaluate(rows).to_dict()
                continue
            if evaluator is None:
                continue
            revision = self.revision_runtime.revise_until_pass(
                artifact_key=key,
                artifact_path=path,
                critic_name=critic_name,
                evaluate=lambda text, fn=evaluator: fn(text),
            )
            results[critic_name] = dict(revision.get("final_result", {}))
            revisions.append(revision)
        return results, revisions

    def _read_csv_rows(self, path: str | Path) -> List[Dict[str, Any]]:
        import csv

        rows: List[Dict[str, Any]] = []
        with Path(path).open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                if row:
                    rows.append(dict(row))
        return rows

    def _mission_status(
        self,
        mission: MissionContract,
        strategy: Dict[str, Any],
        evidence_map: Dict[str, Any],
        diagnostics: Dict[str, Any],
        critics: Dict[str, Dict[str, Any]],
    ) -> str:
        summary = dict(evidence_map.get("summary", {}) or {})
        accepted = int(summary.get("accepted_count", 0) or 0)
        accepted_external = int(summary.get("accepted_external_count", 0) or 0)
        accepted_primary = int(summary.get("accepted_primary_count", 0) or 0)
        claims_supported = int(summary.get("claims_supported", 0) or 0)
        failed = [name for name, payload in critics.items() if isinstance(payload, dict) and not bool(payload.get("passed", True))]
        evidence_only_failures = {"research_quality", "source_credibility"}
        hard_failures = [name for name in failed if name not in evidence_only_failures]
        if hard_failures:
            return "failed_validation"
        if mission.deliverable_mode == "demo_package" or bool(diagnostics.get("synthetic_mode", False)):
            return "demo_complete"
        thresholds = dict(strategy.get("minimum_evidence_threshold", {}) or {})
        min_sources = int(thresholds.get("min_sources", 2) or 2)
        min_claims = int(thresholds.get("min_supported_claims", 2) or 2)
        min_primary = int(thresholds.get("min_primary_sources", 0) or 0)
        attempted = bool(diagnostics.get("collection_attempted", False))
        if accepted_external <= 0:
            if attempted:
                return "no_result_found_with_sufficient_search"
            return "template_complete"
        if claims_supported < min_claims or accepted_external < min_sources or accepted_primary < min_primary:
            return "real_partial"
        return "real_complete"

    def _summary_text(self, mission: MissionContract, status: str, evidence_map: Dict[str, Any]) -> str:
        summary = dict(evidence_map.get("summary", {}) or {})
        accepted = int(summary.get("accepted_count", 0) or 0)
        accepted_external = int(summary.get("accepted_external_count", 0) or 0)
        return f"Mission {mission.mission_type} finished with status {status}. Evidence sources accepted: {accepted} total, {accepted_external} external."

    def _next_steps(self, mission: MissionContract, status: str) -> List[str]:
        if status == "real_complete":
            return ["Open the package artifacts.", "Review critic notes before circulation."]
        if status == "real_partial":
            return ["Broaden source collection within the mission contract.", "Re-run artifact revisions after stronger evidence is added."]
        if status == "no_result_found_with_sufficient_search":
            return ["Try a different source set or provide user-owned source files.", "Do not circulate the current package as a fact-backed external deliverable."]
        if status == "template_complete":
            return ["Add mission-specific source files or live evidence.", "Use the package as a structure template, not as a final external deliverable."]
        if status == "demo_complete":
            return ["Replace demo inputs with real mission evidence before circulation.", "Validate all external claims against accepted sources."]
        return ["Review failed critics and unresolved evidence gaps."]

    def _recovery_payload(
        self,
        mission: MissionContract,
        status: str,
        diagnostics: Dict[str, Any],
        critics: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        failed_critics = [name for name, payload in critics.items() if isinstance(payload, dict) and not bool(payload.get("passed", True))]
        if status == "failed_validation":
            return {
                "status": status,
                "reason": f"Artifact critics failed: {', '.join(failed_critics)}",
                "strategy": "revision_runtime",
                "fallback_used": "none",
            }
        if status == "template_complete":
            return {
                "status": status,
                "reason": "No external evidence was accepted for final-use claims.",
                "strategy": "template_only",
                "fallback_used": "template_package",
            }
        if status == "no_result_found_with_sufficient_search":
            return {
                "status": status,
                "reason": "Research collection ran but did not produce enough accepted evidence.",
                "strategy": "search_or_request_sources",
                "fallback_used": "none",
            }
        if status == "real_partial":
            return {
                "status": status,
                "reason": "Some accepted evidence exists, but it does not meet the mission threshold.",
                "strategy": "broaden_within_scope",
                "fallback_used": "partial",
            }
        return {
            "status": status,
            "reason": "Mission package met the current quality and evidence bar.",
            "strategy": "none",
            "fallback_used": "none",
        }

    def _slug(self, text: str) -> str:
        import re

        return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")[:72] or "mission"
