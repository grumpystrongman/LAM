from __future__ import annotations

import csv
import json
import textwrap
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .career_package_builder import build_job_search_package
from .data_storytelling import build_story_package
from .mission_contract import MissionContract
from .presentation_build import build_presentation_outline
from .ui_build import build_ui_delivery


class WorkProductEngine:
    def build(
        self,
        *,
        contract: MissionContract,
        strategy: Dict[str, Any],
        evidence_map: Dict[str, Any],
        memory_context: Dict[str, Any],
        workspace_dir: str | Path,
        source_records: List[Dict[str, Any]] | None = None,
        extra_context: Dict[str, Any] | None = None,
    ) -> Tuple[Dict[str, str], Dict[str, Dict[str, Any]]]:
        extra_context = dict(extra_context or {})
        if contract.mission_type == "job_search_package" and list(source_records or []):
            candidate_profile = dict(extra_context.get("candidate_profile", {}) or {})
            candidate_profile.setdefault("user_goal", contract.user_goal)
            return build_job_search_package(
                contract=contract,
                strategy=strategy,
                evidence_map=evidence_map,
                memory_context=memory_context,
                workspace_dir=workspace_dir,
                source_records=source_records,
                candidate_profile=candidate_profile,
            )
        root = Path(workspace_dir)
        artifacts_dir = root / "mission_artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        artifacts: Dict[str, str] = {}
        metadata: Dict[str, Dict[str, Any]] = {}
        claim_summary = ", ".join(sorted((evidence_map.get("claims", {}) or {}).keys())[:4]) or "limited explicit claims"
        accepted_sources = [item for item in (evidence_map.get("entries", []) or []) if isinstance(item, dict) and bool(item.get("allowed_as_evidence", False))]

        for item in contract.artifact_plan:
            name = str(item.get("name", "artifact"))
            artifact_type = str(item.get("artifact_type", "document"))
            content = self._render_artifact(
                name=name,
                artifact_type=artifact_type,
                contract=contract,
                strategy=strategy,
                evidence_map=evidence_map,
                memory_context=memory_context,
                claim_summary=claim_summary,
                accepted_sources=accepted_sources,
            )
            if artifact_type == "spreadsheet":
                path = artifacts_dir / f"{name}.csv"
                rows = content if isinstance(content, list) else [{"name": name, "note": str(content)}]
                self._write_csv(path, rows)
                key = f"{name}_csv"
            elif artifact_type == "dashboard":
                path = artifacts_dir / f"{name}.html"
                path.write_text(str(content), encoding="utf-8")
                key = f"{name}_html"
            elif artifact_type == "code":
                path = artifacts_dir / f"{name}.py"
                path.write_text(str(content), encoding="utf-8")
                key = f"{name}_py"
            elif artifact_type == "spec":
                path = artifacts_dir / f"{name}.json"
                path.write_text(json.dumps(content, indent=2), encoding="utf-8")
                key = f"{name}_json"
            elif artifact_type == "presentation":
                path = artifacts_dir / f"{name}.md"
                path.write_text(str(content), encoding="utf-8")
                key = f"{name}_md"
            else:
                path = artifacts_dir / f"{name}.md"
                path.write_text(str(content), encoding="utf-8")
                key = f"{name}_md"
            artifacts[key] = str(path.resolve())
            metadata[key] = {
                "key": key,
                "path": str(path.resolve()),
                "type": artifact_type,
                "title": name.replace("_", " ").title(),
                "evidence_summary": f"Built for {contract.mission_type} using {int((evidence_map.get('summary', {}) or {}).get('accepted_count', 0) or 0)} accepted evidence source(s).",
                "validation_state": "ready",
                "created_at": path.stat().st_mtime if path.exists() else 0,
            }
        summary_path = artifacts_dir / "final_package_summary.md"
        summary_path.write_text(self._render_package_summary(contract, strategy, evidence_map, artifacts), encoding="utf-8")
        artifacts["final_package_summary_md"] = str(summary_path.resolve())
        metadata["final_package_summary_md"] = {
            "key": "final_package_summary_md",
            "path": str(summary_path.resolve()),
            "type": "document",
            "title": "Final Package Summary",
            "evidence_summary": "Mission-level package summary and usage notes.",
            "validation_state": "ready",
            "created_at": summary_path.stat().st_mtime if summary_path.exists() else 0,
        }
        return artifacts, metadata

    def revise_artifact(self, *, artifact_path: str | Path, instructions: List[str]) -> str:
        path = Path(artifact_path)
        text = path.read_text(encoding="utf-8") if path.exists() else ""
        if not instructions:
            return text
        revised = text.rstrip() + "\n\n## Revision Notes Applied\n" + "\n".join(f"- {item}" for item in instructions) + "\n"
        path.write_text(revised, encoding="utf-8")
        return revised

    def _write_csv(self, path: Path, rows: List[Dict[str, Any]]) -> None:
        fieldnames: List[str] = []
        for row in rows:
            for key in row.keys():
                if key not in fieldnames:
                    fieldnames.append(str(key))
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames or ["value"])
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    def _render_artifact(
        self,
        *,
        name: str,
        artifact_type: str,
        contract: MissionContract,
        strategy: Dict[str, Any],
        evidence_map: Dict[str, Any],
        memory_context: Dict[str, Any],
        claim_summary: str,
        accepted_sources: List[Dict[str, Any]],
    ) -> Any:
        accepted = int((evidence_map.get("summary", {}) or {}).get("accepted_count", 0) or 0)
        if name == "job_tracker":
            rows = self._rows_from_sources(accepted_sources, default_type="job_board", primary_field="role")
            if rows:
                return rows
            return [
                {"role": "Template role target 1", "company": "Template company", "fit_score": 0.5, "evidence": "Template only; add real accepted role evidence before use."},
                {"role": "Template role target 2", "company": "Template company", "fit_score": 0.49, "evidence": "Template only; add real accepted role evidence before use."},
            ]
        if name == "grant_tracker":
            rows = self._rows_from_sources(accepted_sources, default_type="grant_portal", primary_field="opportunity")
            if rows:
                return rows
            return [
                {"opportunity": "Template grant opportunity", "fit_score": 0.5, "deadline": "TBD", "evidence": "Template only; add real accepted grant evidence before use."},
            ]
        if name == "resume":
            return textwrap.dedent(
                f"""\
                # Tailored Resume

                ## Summary
                Tailored senior operator-level profile aligned to the target leadership roles, with emphasis on analytics transformation, AI operating systems, stakeholder delivery, and execution rigor across technical and business environments.

                ## Experience
                - Led cross-functional analytics, automation, and AI delivery programs.
                - Translated ambiguous business goals into executable operating plans and measurable deliverables.
                - Built systems, dashboards, and artifacts that supported leadership decisions and team execution.

                ## Impact Highlights
                - Drove measurable execution improvements with operator workflows and stakeholder deliverables.
                - Reduced friction across research, analysis, packaging, and stakeholder communication.
                - Turned complex technical work into clear decision-ready outputs for executives and clients.

                ## Role Fit
                Tailored to the strongest current role requirements identified in the mission evidence, especially strategic leadership, cross-functional execution, analytics depth, AI fluency, and high-trust stakeholder communication.

                ## Evidence Alignment
                Supported claims: {claim_summary}
                Evidence-backed sources referenced: {", ".join(self._top_source_titles(accepted_sources)) or "none yet"}
                """
            )
        if name == "cover_letter":
            return textwrap.dedent(
                f"""\
                # Tailored Cover Letter

                ## Why this role
                The role aligns with the user's domain background and leadership scope, especially where the team needs someone who can connect strategy, technical execution, and business communication.

                ## Why me
                The profile shows operating rigor, technical depth, and stakeholder delivery. The candidate has experience turning unclear objectives into structured plans, leading cross-functional execution, and packaging results in ways decision-makers can actually use.

                ## Next step
                Close with a clear request for discussion and interview follow-up. The letter should explicitly invite a conversation about how the candidate can help the organization move faster with data, analytics, AI, and stakeholder alignment.

                ## Evidence Alignment
                Supported claims: {claim_summary}
                Evidence-backed sources referenced: {", ".join(self._top_source_titles(accepted_sources)) or "none yet"}
                """
            )
        if name == "proposal":
            return textwrap.dedent(
                f"""\
                # Grant Proposal Draft

                ## Need
                The problem is framed with measurable operational and analytic impact. The proposed work addresses a concrete gap in healthcare analytics capability, especially where fragmented data workflows and limited AI-enabled decision support create preventable delays, uneven reporting quality, and poor organizational visibility.

                ## Eligibility
                The draft assumes alignment with the identified grant opportunity and notes validation needs. The package should confirm organizational eligibility, geography or beneficiary restrictions, matching-fund requirements, and any collaboration or nonprofit-status conditions before submission.

                ## Approach
                Deliver phased healthcare analytics AI capabilities with clear milestones. Phase one establishes data intake, governance, and analytic baselines. Phase two adds decision-support workflows, stakeholder reporting, and pilot use cases. Phase three validates adoption, quality improvement, and measurable operational value.

                ## Budget
                Budget assumptions are preliminary and require confirmation. Expected cost categories include implementation labor, data engineering, analytics tooling, evaluation support, stakeholder training, and light program management needed to ensure delivery against the funder timeline.

                ## Outcomes
                Measurable adoption, efficiency, and quality outcomes are proposed. The outcome model should track improved reporting timeliness, higher analytic reuse, stronger stakeholder decision support, and clearer evidence that the funded work can scale beyond the pilot window.

                ## Evidence Alignment
                Supported claims: {claim_summary}
                Evidence-backed sources referenced: {", ".join(self._top_source_titles(accepted_sources)) or "none yet"}
                """
            )
        if name == "executive_brief":
            story = build_story_package(
                {
                    "user_goal": contract.user_goal,
                    "audience": contract.audience,
                    "requested_outputs": contract.requested_outputs,
                },
                {
                    "findings": list(strategy.get("research_questions", [])[:3]),
                    "recommended_actions": ["Review the top evidence-backed options.", "Validate remaining assumptions before stakeholder circulation."],
                    "caveats": [f"Accepted evidence sources: {accepted}.", "This brief should not overstate unsupported claims."],
                    "insights": list(strategy.get("expected_evidence", [])[:3]),
                },
            )
            return textwrap.dedent(
                f"""\
                # Executive Brief

                ## Executive Summary
                {story.get('executive_summary', 'Prepared an executive brief for the mission.')}

                ## Key Findings
                {chr(10).join(f"- {item}" for item in story.get('key_findings', []))}

                ## So What
                {story.get('so_what', 'The evidence points to a focused set of actions.')}

                ## Recommendations
                {chr(10).join(f"- {item}" for item in story.get('recommended_actions', []))}

                ## Caveats
                {chr(10).join(f"- {item}" for item in story.get('caveats', []))}

                ## Source Notes
                {chr(10).join(f"- {item}" for item in self._top_source_titles(accepted_sources)) or "- No external evidence-backed sources were accepted yet."}
                """
            )
        if name == "data_story":
            return textwrap.dedent(
                f"""\
                # Data Story

                ## Story
                The strongest story should be the one most supported by the data and evidence.

                ## Evidence
                Supported claims: {claim_summary}

                ## Caveats
                - Evidence count: {accepted}
                - Interpret results within the mission constraints.
                - Evidence-backed sources: {", ".join(self._top_source_titles(accepted_sources)) or "none yet"}

                ## Recommended Actions
                - Validate the central story with stakeholders.
                - Use visuals to clarify the decision implications.
                """
            )
        if name == "presentation":
            outline = build_presentation_outline(
                {"user_goal": contract.user_goal, "audience": contract.audience},
                {
                    "executive_summary": "Mission findings prepared for executive review.",
                    "key_findings": list(strategy.get("research_questions", [])[:3]),
                    "recommended_actions": ["Review the package.", "Decide on next actions."],
                    "caveats": ["Evidence quality is shown in the evidence map."],
                    "so_what": "Leadership can use this package to decide the next move.",
                },
            )
            slides = ["# Presentation Outline", ""]
            slides.extend(
                f"## {slide.get('title', 'Slide')}\n" + "\n".join(f"- {item}" for item in (slide.get("bullets", []) or []))
                for slide in (outline.get("slides", []) or [])
            )
            return "Title Slide\n\n" + "\n\n".join(slides) + "\n\nAppendix\n- Evidence map references"
        if name == "dashboard":
            return f"<html><body><h1>Mission Dashboard</h1><p>Mission: {contract.mission_type}</p><p>Accepted sources: {accepted}</p><p>Claims: {claim_summary}</p></body></html>"
        if name == "ui_spec":
            return build_ui_delivery({"goal": contract.user_goal, "artifact_viewers": True})
        if name == "code":
            return textwrap.dedent(
                f"""\
                from __future__ import annotations

                def run_mission_stub() -> dict:
                    return {{
                        "mission_type": "{contract.mission_type}",
                        "deliverable_mode": "{contract.deliverable_mode}",
                        "accepted_sources": {accepted},
                    }}
                """
            )
        if name in {"application_checklist", "submission_checklist"}:
            return textwrap.dedent(
                f"""\
                # {name.replace('_', ' ').title()}

                - Review evidence map against final claims
                - Validate stakeholder-specific requirements
                - Confirm quality-critic results before circulation
                """
            )
        return textwrap.dedent(
            f"""\
            # {name.replace('_', ' ').title()}

            Mission: {contract.mission_type}
            Audience: {contract.audience}
            Accepted evidence sources: {accepted}
            Claims supported: {claim_summary}
            Memory signals available: {len(memory_context.get('used', []) if isinstance(memory_context, dict) else [])}
            """
        )

    def _render_package_summary(self, contract: MissionContract, strategy: Dict[str, Any], evidence_map: Dict[str, Any], artifacts: Dict[str, str]) -> str:
        return textwrap.dedent(
            f"""\
            # Final Package Summary

            ## Mission
            {contract.user_goal}

            ## Mission Type
            {contract.mission_type}

            ## Deliverable Mode
            {contract.deliverable_mode}

            ## Evidence Summary
            {json.dumps(evidence_map.get("summary", {}), indent=2)}

            ## Research Questions
            {chr(10).join(f"- {item}" for item in strategy.get("research_questions", []))}

            ## Artifacts
            {chr(10).join(f"- {key}: {value}" for key, value in sorted(artifacts.items()))}
            """
        )

    def _rows_from_sources(self, accepted_sources: List[Dict[str, Any]], *, default_type: str, primary_field: str) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for idx, item in enumerate(accepted_sources[:5], start=1):
            title = str(item.get("source", "") or "").strip() or f"Source {idx}"
            row = {
                primary_field: title,
                "source_type": str(item.get("source_type", default_type)),
                "url": str(item.get("url_or_path", "")),
                "fit_score": round(max(0.55, min(0.98, float(item.get("confidence", 0.6) or 0.6))), 2),
                "evidence": ", ".join(list(item.get("claim_support", []) or [])[:3]) or "supported evidence",
            }
            if primary_field == "role":
                row["company"] = self._hostish_label(str(item.get("url_or_path", ""))) or "Evidence-backed company"
            if primary_field == "opportunity":
                row["deadline"] = "See source"
            rows.append(row)
        return rows

    def _top_source_titles(self, accepted_sources: List[Dict[str, Any]], limit: int = 4) -> List[str]:
        out: List[str] = []
        seen: set[str] = set()
        for item in accepted_sources:
            title = str(item.get("source", "") or "").strip()
            if not title:
                title = self._hostish_label(str(item.get("url_or_path", "")))
            if not title:
                continue
            low = title.lower()
            if low in seen:
                continue
            seen.add(low)
            out.append(title)
            if len(out) >= limit:
                break
        return out

    def _hostish_label(self, value: str) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        raw = raw.replace("https://", "").replace("http://", "")
        return raw.split("/", 1)[0]
