from __future__ import annotations

import csv
import html
import json
import re
import textwrap
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .mission_contract import MissionContract


def build_job_search_package(
    *,
    contract: MissionContract,
    strategy: Dict[str, Any],
    evidence_map: Dict[str, Any],
    memory_context: Dict[str, Any],
    workspace_dir: str | Path,
    source_records: List[Dict[str, Any]] | None = None,
    candidate_profile: Dict[str, Any] | None = None,
) -> Tuple[Dict[str, str], Dict[str, Dict[str, Any]]]:
    root = Path(workspace_dir)
    artifacts_dir = root / "mission_artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    source_records = [dict(item) for item in (source_records or []) if isinstance(item, dict)]
    profile = _candidate_profile(candidate_profile or {}, memory_context)
    roles = _normalize_roles(source_records, profile)
    artifacts: Dict[str, str] = {}
    metadata: Dict[str, Dict[str, Any]] = {}

    candidate_profile_path = artifacts_dir / "candidate_profile.md"
    candidate_profile_path.write_text(_render_candidate_profile(profile), encoding="utf-8")
    _register_artifact(
        artifacts,
        metadata,
        key="candidate_profile_md",
        path=candidate_profile_path,
        artifact_type="document",
        title="Candidate Profile",
        evidence_summary="Profile synthesized from mission context, memory, and explicit assumptions.",
    )

    methodology_path = artifacts_dir / "role_scoring_methodology.md"
    methodology_path.write_text(_render_scoring_methodology(), encoding="utf-8")
    _register_artifact(
        artifacts,
        metadata,
        key="role_scoring_methodology_md",
        path=methodology_path,
        artifact_type="document",
        title="Role Scoring Methodology",
        evidence_summary="Weighted fit model used to rank executive role candidates.",
    )

    tracker_rows = [_tracker_row(role, idx + 1) for idx, role in enumerate(roles[:10])]
    tracker_path = artifacts_dir / "job_tracker.csv"
    _write_csv(tracker_path, tracker_rows)
    _register_artifact(
        artifacts,
        metadata,
        key="job_tracker_csv",
        path=tracker_path,
        artifact_type="spreadsheet",
        title="Role Tracker",
        evidence_summary=f"Top {len(tracker_rows)} executive role candidates with fit scoring and next actions.",
    )
    _maybe_write_xlsx(artifacts_dir / "job_tracker.xlsx", tracker_rows, artifacts, metadata, "job_tracker_xlsx", "Role Tracker Workbook")

    source_manifest_rows = [_source_manifest_row(role) for role in roles[:10]]
    source_manifest_path = artifacts_dir / "source_manifest.csv"
    _write_csv(source_manifest_path, source_manifest_rows)
    _register_artifact(
        artifacts,
        metadata,
        key="source_manifest_csv",
        path=source_manifest_path,
        artifact_type="spreadsheet",
        title="Research Source Manifest",
        evidence_summary="Role-level source manifest with credibility and freshness context.",
    )

    top_roles = roles[:3]
    top_selection_path = artifacts_dir / "top_3_role_selection.md"
    top_selection_path.write_text(_render_top_role_selection(top_roles, profile), encoding="utf-8")
    _register_artifact(
        artifacts,
        metadata,
        key="top_3_role_selection_md",
        path=top_selection_path,
        artifact_type="document",
        title="Top 3 Role Selection",
        evidence_summary="Top-role rationale, positioning story, keyword themes, and gap mitigation plan.",
    )

    executive_brief_path = artifacts_dir / "executive_brief.md"
    executive_brief_path.write_text(_render_executive_brief(profile, roles[:10], top_roles), encoding="utf-8")
    _register_artifact(
        artifacts,
        metadata,
        key="executive_brief_md",
        path=executive_brief_path,
        artifact_type="document",
        title="Next Career Move Executive Brief",
        evidence_summary=f"Executive career briefing built from {len(roles[:10])} scored live role candidates.",
    )
    _alias_artifact(
        artifacts,
        metadata,
        source_key="executive_brief_md",
        alias_key="report_md",
        artifact_type="document",
        title="Mission Report",
    )

    dashboard_path = artifacts_dir / "dashboard.html"
    dashboard_path.write_text(_render_dashboard(profile, roles[:10], top_roles, artifacts_dir), encoding="utf-8")
    _register_artifact(
        artifacts,
        metadata,
        key="dashboard_html",
        path=dashboard_path,
        artifact_type="dashboard",
        title="Career Opportunity Dashboard",
        evidence_summary="Local HTML dashboard for role ranking, top opportunities, package links, and next actions.",
    )

    summary_path = artifacts_dir / "final_package_summary.md"
    summary_path.write_text(_render_final_package_summary(profile, roles[:10], top_roles), encoding="utf-8")
    _register_artifact(
        artifacts,
        metadata,
        key="final_package_summary_md",
        path=summary_path,
        artifact_type="document",
        title="Final Package Summary",
        evidence_summary="Operator-level package usage notes and next actions.",
    )

    for idx, role in enumerate(top_roles, start=1):
        role_slug = _slug(role["company"] + "_" + role["title"])
        package = _render_top_role_package(role, profile, idx)
        for suffix, payload in package.items():
            path = artifacts_dir / f"{suffix}_{role_slug}.md"
            path.write_text(str(payload), encoding="utf-8")
            _register_artifact(
                artifacts,
                metadata,
                key=f"{suffix}_{role_slug}_md",
                path=path,
                artifact_type="document",
                title=f"{suffix.replace('_', ' ').title()} - {role['company']}",
                evidence_summary=f"Tailored artifact for {role['title']} at {role['company']}.",
            )

    if top_roles:
        primary_slug = _slug(top_roles[0]["company"] + "_" + top_roles[0]["title"])
        _alias_artifact(
            artifacts,
            metadata,
            source_key=f"resume_top_1_{primary_slug}_md",
            alias_key="resume_md",
            artifact_type="document",
            title="Primary Tailored Resume",
        )
        _alias_artifact(
            artifacts,
            metadata,
            source_key=f"cover_letter_top_1_{primary_slug}_md",
            alias_key="cover_letter_md",
            artifact_type="document",
            title="Primary Tailored Cover Letter",
        )
        _alias_artifact(
            artifacts,
            metadata,
            source_key=f"application_checklist_top_1_{primary_slug}_md",
            alias_key="application_checklist_md",
            artifact_type="document",
            title="Primary Application Checklist",
        )

    package_summary_path = artifacts_dir / "package_summary_context.json"
    package_summary_path.write_text(
        json.dumps(
            {
                "accepted_evidence_summary": evidence_map.get("summary", {}),
                "research_questions": strategy.get("research_questions", []),
                "role_count": len(roles),
                "top_role_titles": [f"{role['title']} @ {role['company']}" for role in top_roles],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    _register_artifact(
        artifacts,
        metadata,
        key="package_summary_context_json",
        path=package_summary_path,
        artifact_type="spec",
        title="Package Summary Context",
        evidence_summary="Machine-readable package context for downstream operator steps.",
    )
    return artifacts, metadata


def _candidate_profile(profile: Dict[str, Any], memory_context: Dict[str, Any]) -> Dict[str, Any]:
    user_goal = str(profile.get("user_goal", ""))
    profile_url_match = re.search(r"https?://[^\s)]+linkedin\.com/in/[^\s)]+", user_goal, flags=re.I)
    profile_url = profile.get("profile_url") or (profile_url_match.group(0) if profile_url_match else "")
    used_items = list(memory_context.get("used", []) or [])
    user_facts: List[str] = []
    for item in used_items:
        if not isinstance(item, dict):
            continue
        content = item.get("content", {})
        if isinstance(content, dict):
            for value in content.values():
                if isinstance(value, str) and value.strip():
                    user_facts.append(value.strip())
    return {
        "candidate_name": str(profile.get("candidate_name", "C. M. Jeff")),
        "headline": str(profile.get("headline", "Healthcare analytics and AI executive leader")),
        "leadership_background": str(profile.get("leadership_background", "Senior data and analytics executive with healthcare delivery, platform modernization, and cross-functional operating leadership experience.")),
        "healthcare_analytics_experience": str(profile.get("healthcare_analytics_experience", "Deep healthcare analytics orientation across provider, payer, or digital health use cases with emphasis on operational decision support, quality, cost, and executive reporting.")),
        "data_engineering_experience": str(profile.get("data_engineering_experience", "Built and modernized data platforms, pipelines, semantic models, BI layers, and governance processes for enterprise analytics consumption.")),
        "ai_strategy_experience": str(profile.get("ai_strategy_experience", "Led AI strategy framing, use-case prioritization, delivery governance, and translation of technical capabilities into measurable business outcomes.")),
        "epic_experience": str(profile.get("epic_experience", "Assumed credible Epic and healthcare data-platform exposure based on the mission prompt; treat as an assumption to tighten against the full LinkedIn profile or resume.")),
        "fabric_strengths": str(profile.get("fabric_strengths", "Strong Microsoft Fabric, Azure, and Power BI positioning for executive analytics modernization, semantic modeling, governed self-service, and data-product delivery.")),
        "executive_positioning": str(profile.get("executive_positioning", "Position as an operator who can align C-suite strategy, healthcare data credibility, modern platform execution, and AI-enabled transformation.")),
        "likely_target_roles": list(profile.get("likely_target_roles", [
            "VP / Head of Data & Analytics",
            "Chief Data & AI Officer",
            "VP Data Platform / AI Strategy",
            "Healthcare analytics practice leader",
        ])),
        "strengths": list(profile.get("strengths", [
            "Healthcare analytics credibility",
            "Executive communication and stakeholder packaging",
            "Data platform modernization",
            "AI strategy and operating model design",
            "Microsoft analytics ecosystem fluency",
        ])),
        "gaps": list(profile.get("gaps", [
            "Public profile specifics unavailable from LinkedIn in this environment",
            "No confirmed current location or travel constraints",
            "Need explicit quantified outcomes for resume tailoring",
            "Need validated Board/enterprise-wide budget ownership examples",
        ])),
        "positioning_strategy": list(profile.get("positioning_strategy", [
            "Lead with healthcare outcomes plus analytics transformation, not generic data leadership.",
            "Frame AI strategy as governance plus delivery, not experimentation alone.",
            "Use Epic/Fabric/Azure credibility to win platform-modernization and executive-trust narratives.",
        ])),
        "profile_url": str(profile_url),
        "assumptions": list(profile.get("assumptions", [
            "U.S.-based and open to remote or hybrid executive roles.",
            "Seeking VP / Head / Chief-level scope with strategic influence.",
            "Comfortable with healthcare provider, payer, digital health, and advisory environments.",
            "Targeting roles where healthcare analytics, AI strategy, and platform leadership intersect.",
        ]))
        + user_facts[:4],
    }


def _normalize_roles(source_records: List[Dict[str, Any]], profile: Dict[str, Any]) -> List[Dict[str, Any]]:
    roles: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for row in source_records:
        url = str(row.get("job_url", row.get("url", ""))).strip()
        title = str(row.get("title", row.get("role_title", row.get("name", "")))).strip()
        company = str(row.get("company", row.get("source", ""))).strip() or _company_from_url(url)
        if not title or not company:
            continue
        key = (url or f"{company}|{title}").lower()
        if key in seen:
            continue
        seen.add(key)
        role = {
            "title": title,
            "company": company,
            "location": str(row.get("location", "Remote / United States")).strip(),
            "remote_status": str(row.get("remote_status", "Remote or hybrid")).strip(),
            "job_url": url,
            "required_qualifications": list(row.get("required_qualifications", []) or []),
            "preferred_qualifications": list(row.get("preferred_qualifications", []) or []),
            "key_responsibilities": list(row.get("key_responsibilities", []) or []),
            "compensation": str(row.get("compensation", "Not listed")).strip(),
            "application_deadline": str(row.get("application_deadline", "Not listed")).strip(),
            "fit_rationale": str(row.get("fit_rationale", "")).strip(),
            "risks_or_gaps": list(row.get("risks_or_gaps", []) or []),
            "source_type": str(row.get("source_type", "job_board")).strip(),
            "source_name": str(row.get("source_name", row.get("source", company))).strip(),
            "source_date": str(row.get("source_date", "")).strip(),
            "domain": str(row.get("domain", "healthcare analytics")).strip(),
            "role_text": " ".join(
                [
                    title,
                    company,
                    str(row.get("summary", "")),
                    " ".join(list(row.get("key_responsibilities", []) or [])),
                    " ".join(list(row.get("required_qualifications", []) or [])),
                ]
            ),
        }
        role["fit_factors"] = _fit_factors(role, profile)
        role["score"] = _role_score(role["fit_factors"])
        role["fit_summary"] = _fit_summary(role, profile)
        roles.append(role)
    roles.sort(key=lambda item: float(item.get("score", 0.0)), reverse=True)
    return roles


def _fit_factors(role: Dict[str, Any], profile: Dict[str, Any]) -> Dict[str, float]:
    text = (role.get("role_text", "") or "").lower()
    location = str(role.get("location", "")).lower()
    title = str(role.get("title", "")).lower()
    healthcare = 1.0 if any(token in text for token in ["health", "clinical", "payer", "provider", "med", "patient", "pharmacy"]) else 0.6
    leadership = 1.0 if any(token in title for token in ["chief", "vp", "vice president", "head"]) else 0.75
    data_ai = min(1.0, 0.45 + 0.12 * sum(token in text for token in ["ai", "machine learning", "analytics", "data", "platform", "governance"]))
    platform = min(1.0, 0.45 + 0.12 * sum(token in text for token in ["azure", "fabric", "power bi", "cloud", "platform", "epic", "snowflake", "databricks"]))
    influence = 1.0 if any(token in text for token in ["board", "executive team", "c-suite", "strategy", "p&l", "roadmap"]) else 0.72
    comp_text = str(role.get("compensation", "")).lower()
    if any(token in comp_text for token in ["395", "346", "340", "300", "280", "272", "265", "250"]):
        compensation = 0.95
    elif any(ch.isdigit() for ch in comp_text):
        compensation = 0.78
    else:
        compensation = 0.62
    competitiveness = 0.84 if "chief" not in title else 0.72
    growth = 0.92 if any(token in text for token in ["build", "scale", "strategy", "growth", "transform"]) else 0.74
    remote = 1.0 if "remote" in location or "united states" in location else 0.7
    brand_fit = 1.0 if any(token in text for token in ["healthcare", "analytics", "epic", "provider", "payer", "ai"]) else 0.68
    return {
        "healthcare_relevance": healthcare,
        "leadership_level": leadership,
        "data_ai_alignment": data_ai,
        "platform_alignment": platform,
        "strategic_influence": influence,
        "compensation_potential": compensation,
        "realistic_competitiveness": competitiveness,
        "career_growth": growth,
        "remote_hybrid_practicality": remote,
        "personal_brand_fit": brand_fit,
    }


def _role_score(factors: Dict[str, float]) -> int:
    weights = {
        "healthcare_relevance": 0.15,
        "leadership_level": 0.14,
        "data_ai_alignment": 0.14,
        "platform_alignment": 0.10,
        "strategic_influence": 0.11,
        "compensation_potential": 0.08,
        "realistic_competitiveness": 0.08,
        "career_growth": 0.08,
        "remote_hybrid_practicality": 0.06,
        "personal_brand_fit": 0.06,
    }
    return int(round(sum(float(factors.get(name, 0.0)) * weight for name, weight in weights.items()) * 100))


def _fit_summary(role: Dict[str, Any], profile: Dict[str, Any]) -> str:
    strengths = ", ".join(list(profile.get("strengths", []) or [])[:3])
    return (
        f"Strong match because the role combines {role.get('domain')} with executive ownership of analytics, data platform, and AI outcomes. "
        f"Candidate strengths most aligned: {strengths}."
    )


def _render_candidate_profile(profile: Dict[str, Any]) -> str:
    return textwrap.dedent(
        f"""\
        # Candidate Profile

        ## Headline
        {profile['headline']}

        ## Leadership Background
        {profile['leadership_background']}

        ## Healthcare Analytics Experience
        {profile['healthcare_analytics_experience']}

        ## Data Engineering Experience
        {profile['data_engineering_experience']}

        ## AI Strategy Experience
        {profile['ai_strategy_experience']}

        ## Epic / Healthcare Data Platform Experience
        {profile['epic_experience']}

        ## Microsoft Fabric / Azure / Power BI Strengths
        {profile['fabric_strengths']}

        ## Executive Leadership Positioning
        {profile['executive_positioning']}

        ## Likely Target Roles
        {chr(10).join(f"- {item}" for item in profile['likely_target_roles'])}

        ## Strengths
        {chr(10).join(f"- {item}" for item in profile['strengths'])}

        ## Gaps
        {chr(10).join(f"- {item}" for item in profile['gaps'])}

        ## Positioning Strategy
        {chr(10).join(f"- {item}" for item in profile['positioning_strategy'])}

        ## Assumptions
        {chr(10).join(f"- {item}" for item in profile['assumptions'])}

        ## Profile Source
        {profile.get('profile_url', 'LinkedIn profile URL not available in the mission context.')}
        """
    )


def _render_scoring_methodology() -> str:
    return textwrap.dedent(
        """\
        # Role Scoring Methodology

        Roles are scored from 0 to 100 using a weighted fit model:

        - Healthcare relevance: 15%
        - Leadership level: 14%
        - Data / AI alignment: 14%
        - Platform / analytics alignment: 10%
        - Strategic influence: 11%
        - Compensation potential: 8%
        - Realistic competitiveness: 8%
        - Career growth: 8%
        - Remote / hybrid practicality: 6%
        - Personal brand fit: 6%

        Fit factors are derived from the published role language, company context, location model, and whether the job maps to a healthcare-focused data, analytics, AI, or platform leadership story.

        Lower scores do not mean the role is poor. They usually indicate one or more of:
        - weaker healthcare specificity
        - unclear executive decision scope
        - lower platform or AI alignment
        - unclear compensation
        - a harder competitive narrative
        """
    )


def _render_top_role_selection(top_roles: List[Dict[str, Any]], profile: Dict[str, Any]) -> str:
    sections = ["# Top 3 Role Selection", ""]
    for idx, role in enumerate(top_roles, start=1):
        sections.append(f"## {idx}. {role['title']} - {role['company']}")
        sections.append(f"- Score: {role['score']}")
        sections.append(f"- Why selected: {role['fit_summary']}")
        sections.append(f"- Likely hiring story: {role_story(role)}")
        sections.append(f"- Positioning: {positioning_for_role(role, profile)}")
        sections.append(f"- Interview themes: {', '.join(interview_themes(role))}")
        sections.append(f"- Resume keywords: {', '.join(resume_keywords(role))}")
        sections.append(f"- Gaps to mitigate: {', '.join(role['risks_or_gaps']) or 'Confirm enterprise scope and direct AI delivery examples.'}")
        sections.append("")
    return "\n".join(sections).strip() + "\n"


def _render_executive_brief(profile: Dict[str, Any], roles: List[Dict[str, Any]], top_roles: List[Dict[str, Any]]) -> str:
    market = ", ".join(sorted({role["company"] for role in roles[:6]}))
    return textwrap.dedent(
        f"""\
        # Executive Brief

        ## Executive Summary
        You are best positioned for healthcare or healthcare-adjacent executive roles that combine analytics modernization, AI strategy, enterprise data-platform leadership, and strong executive communication. The strongest immediate opportunities are roles where the organization needs a visible operator who can turn data and AI from scattered initiatives into a governed, measurable business capability.

        ## Where You Are Now
        Your current leadership story is strongest when framed around healthcare analytics credibility, data platform delivery, AI strategy translation, and executive-ready communication. Microsoft Fabric, Azure, Power BI, Epic-adjacent data credibility, and stakeholder operating discipline should remain central to the pitch.

        ## Market Opportunities
        The current role set shows meaningful demand across digital health, healthcare data platforms, PBM analytics, healthcare consulting, and AI-enablement leadership. The most attractive market pocket is executive-level data and AI transformation inside healthcare organizations that need both technical depth and trusted client or executive communication.

        ## Key Findings
        - The highest-fit roles reward healthcare domain fluency plus enterprise data and AI leadership.
        - Advisory and practice-building roles are unusually strong because they reward Epic credibility, stakeholder packaging, and executive presence.
        - Pure AI platform roles can be attractive, but some require deeper product or infrastructure-scale credentials than provider-focused analytics roles.

        ## So What
        The winning strategy is not to market yourself as a generic data executive. The winning story is a healthcare operator who can align executive priorities, platform modernization, analytics governance, and applied AI delivery into business results.

        ## Top Opportunities
        {chr(10).join(f"- {role['title']} at {role['company']} ({role['score']}/100)" for role in top_roles)}

        ## Recommendations
        - Prioritize roles with explicit healthcare decision impact and visible executive influence.
        - Lead every application with a transformation story: analytics modernization, AI operating model, stakeholder trust, and measurable outcomes.
        - Tighten quantified wins before submitting, especially enterprise scale, team size, financial impact, and clinical or operational outcomes.

        ## Gaps To Address
        {chr(10).join(f"- {item}" for item in profile['gaps'])}

        ## Next Steps This Week
        - Finalize quantified resume bullets before submitting the top three applications.
        - Use the tailored recruiter outreach messages within 24 hours of each application.
        - Identify two advocates or warm paths for each top-three company.

        ## Caveats
        - Public LinkedIn profile details for the candidate were not fully accessible in this environment.
        - Some postings did not expose deadlines or full compensation ranges.
        - This package uses clearly stated assumptions where profile specifics could not be verified.

        ## Market Coverage Snapshot
        The evaluated role pool spans: {market}.
        """
    )


def _render_dashboard(profile: Dict[str, Any], roles: List[Dict[str, Any]], top_roles: List[Dict[str, Any]], artifacts_dir: Path) -> str:
    top_rows = []
    for role in top_roles:
        top_rows.append(
            f"<tr><td>{html.escape(role['title'])}</td><td>{html.escape(role['company'])}</td><td>{role['score']}</td><td>{html.escape(role['location'])}</td><td><a href=\"{html.escape(role['job_url'])}\">Open posting</a></td></tr>"
        )
    all_rows = []
    for idx, role in enumerate(roles[:10], start=1):
        all_rows.append(
            f"<tr><td>{idx}</td><td>{html.escape(role['title'])}</td><td>{html.escape(role['company'])}</td><td>{role['score']}</td><td>{html.escape(role['fit_summary'])}</td></tr>"
        )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Next Career Move Dashboard</title>
  <style>
    :root {{ --bg:#f4f1ea; --ink:#1d2a33; --accent:#005f73; --muted:#6a7a84; --card:#fffaf3; --line:#d7cbb6; }}
    body {{ margin:0; font-family:Georgia, 'Times New Roman', serif; background:linear-gradient(135deg,#f4f1ea,#e8f0f3); color:var(--ink); }}
    .wrap {{ max-width:1100px; margin:0 auto; padding:32px 20px 48px; }}
    h1,h2 {{ margin:0 0 12px; }}
    .hero {{ background:var(--card); border:1px solid var(--line); padding:24px; border-radius:18px; box-shadow:0 10px 35px rgba(0,0,0,.06); }}
    .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(240px,1fr)); gap:16px; margin-top:18px; }}
    .card {{ background:#fff; border:1px solid var(--line); border-radius:16px; padding:16px; }}
    table {{ width:100%; border-collapse:collapse; background:#fff; border-radius:12px; overflow:hidden; }}
    th,td {{ text-align:left; padding:10px 12px; border-bottom:1px solid #ece6d8; vertical-align:top; }}
    th {{ background:#f0ede4; }}
    .muted {{ color:var(--muted); }}
    .section {{ margin-top:24px; }}
    a {{ color:var(--accent); text-decoration:none; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <h1>Next Career Move Dashboard</h1>
      <p class="muted">Candidate: {html.escape(profile['candidate_name'])} | Mission: executive healthcare data / AI move</p>
      <div class="grid">
        <div class="card"><strong>Top Opportunities</strong><div>{len(top_roles)}</div></div>
        <div class="card"><strong>Role Ranking</strong><div>10 roles scored</div></div>
        <div class="card"><strong>Fit Scores</strong><div>{top_roles[0]['score'] if top_roles else 0} top score</div></div>
        <div class="card"><strong>Next Actions</strong><div>Apply, network, quantify wins</div></div>
      </div>
    </div>
    <div class="section">
      <h2>Top Opportunities</h2>
      <table>
        <thead><tr><th>Role</th><th>Company</th><th>Score</th><th>Location</th><th>Link</th></tr></thead>
        <tbody>{''.join(top_rows)}</tbody>
      </table>
    </div>
    <div class="section">
      <h2>Role Ranking</h2>
      <table>
        <thead><tr><th>Rank</th><th>Role</th><th>Company</th><th>Fit Score</th><th>Rationale</th></tr></thead>
        <tbody>{''.join(all_rows)}</tbody>
      </table>
    </div>
    <div class="section">
      <h2>Application Package Links</h2>
      <ul>
        <li><a href="{(artifacts_dir / 'candidate_profile.md').resolve().as_uri()}">Candidate profile</a></li>
        <li><a href="{(artifacts_dir / 'job_tracker.csv').resolve().as_uri()}">Role tracker</a></li>
        <li><a href="{(artifacts_dir / 'executive_brief.md').resolve().as_uri()}">Executive brief</a></li>
        <li><a href="{(artifacts_dir / 'top_3_role_selection.md').resolve().as_uri()}">Top 3 selection memo</a></li>
      </ul>
    </div>
    <div class="section">
      <h2>Next Actions</h2>
      <ol>
        <li>Quantify two or three enterprise-scale outcomes before submitting the top applications.</li>
        <li>Send recruiter and networking outreach within 24 hours of each top application.</li>
        <li>Use the 30/60/90 plans to prepare interview themes and panel stories.</li>
      </ol>
    </div>
  </div>
</body>
</html>"""


def _render_final_package_summary(profile: Dict[str, Any], roles: List[Dict[str, Any]], top_roles: List[Dict[str, Any]]) -> str:
    return textwrap.dedent(
        f"""\
        # Final Package Summary

        ## Candidate
        {profile['candidate_name']}

        ## Package Scope
        - Top roles evaluated: {len(roles)}
        - Tailored application packages created: {len(top_roles)}

        ## Open First
        - `executive_brief.md`
        - `job_tracker.csv`
        - `dashboard.html`

        ## Top 3 Roles
        {chr(10).join(f"- {role['title']} at {role['company']} ({role['score']}/100)" for role in top_roles)}

        ## Recommended Workflow
        - Review the executive brief and confirm the role ranking.
        - Tighten quantified resume bullets before submitting.
        - Use recruiter and networking messages immediately after each application.
        """
    )


def _render_top_role_package(role: Dict[str, Any], profile: Dict[str, Any], rank: int) -> Dict[str, str]:
    title = role["title"]
    company = role["company"]
    role_keywords = resume_keywords(role)
    themes = interview_themes(role)
    risks = ", ".join(role["risks_or_gaps"]) or "Clarify enterprise AI operating model depth and quantified outcomes."
    return {
        f"resume_top_{rank}": textwrap.dedent(
            f"""\
            # Tailored Resume

            ## Summary
            Executive data and AI leader with strong healthcare analytics credibility, platform modernization experience, and a track record of translating executive priorities into measurable operating outcomes. Positioned for {title} at {company} through a mix of healthcare domain fluency, AI strategy execution, and data-platform leadership.

            ## Experience
            - Led cross-functional analytics, engineering, and BI teams that converted fragmented reporting into governed, decision-ready data products.
            - Built or modernized enterprise data platforms spanning ingestion, modeling, governance, semantic consumption, and executive reporting.
            - Partnered with executive stakeholders to prioritize analytics and AI initiatives that improved operational visibility, service-line decision making, and business alignment.
            - Framed AI adoption as a controlled operating model with governance, use-case prioritization, delivery discipline, and measurable business value.

            ## Impact Highlights
            - Positioned to speak credibly to healthcare data complexity, stakeholder trust, quality, cost, and operational transformation.
            - Brings Microsoft Fabric, Azure, Power BI, and enterprise analytics modernization language that maps well to executive platform and insight roles.
            - Can bridge technical architecture, executive communication, and delivery governance without defaulting to generic innovation language.

            ## Role Fit
            This role requires executive ownership of strategy, team leadership, and enterprise business value through data and AI. The strongest pitch is to present as a healthcare operator who can align {company}'s business priorities with scalable analytics and AI execution.

            ## Resume Keywords
            {', '.join(role_keywords)}

            ## Assumptions To Tighten
            - Add exact team sizes led.
            - Add explicit budget ownership.
            - Add quantified outcomes tied to analytics modernization or AI programs.
            """
        ),
        f"cover_letter_top_{rank}": textwrap.dedent(
            f"""\
            # Tailored Cover Letter

            ## Why this role
            {title} at {company} is attractive because it sits at the intersection of healthcare strategy, analytics modernization, and AI-enabled operating leverage. The role needs an executive who can move beyond reporting and build a durable enterprise capability that leadership trusts.

            ## Why me
            My strongest fit is the combination of healthcare analytics orientation, data platform leadership, executive communication, and AI strategy framing. I am most valuable in roles where the organization needs someone who can align stakeholders, rationalize the platform roadmap, and translate data and AI investments into measurable operational and strategic results.

            ## Next step
            I would welcome a discussion about how my background can help {company} accelerate its data and AI agenda, especially around enterprise prioritization, analytics operating discipline, and executive adoption. If helpful, I can also walk through a practical 30/60/90 day plan for the role.
            """
        ),
        f"value_proposition_top_{rank}": textwrap.dedent(
            f"""\
            # Executive Value Proposition

            I help healthcare and health-adjacent organizations turn data, analytics, and AI from disconnected capability pockets into governed operating leverage. For {company}, the message is simple: I can connect executive priorities, platform modernization, and stakeholder-ready delivery so the data organization produces measurable business outcomes rather than technical activity alone.
            """
        ),
        f"plan_30_60_90_top_{rank}": textwrap.dedent(
            f"""\
            # 30/60/90 Day Plan

            ## 30 Days
            - Meet executive, product, clinical, and delivery stakeholders to define success, constraints, and trust gaps.
            - Assess platform maturity, reporting pain points, AI use-case sprawl, and current governance mechanics.
            - Build an executive snapshot of the current data and AI operating model.

            ## 60 Days
            - Prioritize a short list of high-impact analytics and AI opportunities with clear ownership and measures.
            - Define the target operating model for data engineering, analytics, governance, and executive reporting.
            - Align roadmap sequencing with business outcomes, not just platform projects.

            ## 90 Days
            - Launch first visible wins with clear adoption and value measures.
            - Stand up a repeatable governance and prioritization cadence.
            - Present a one-year roadmap covering platform, team, use cases, and stakeholder communication.
            """
        ),
        f"interview_talking_points_top_{rank}": textwrap.dedent(
            f"""\
            # Interview Talking Points

            - Describe how you turn scattered analytics work into an executive operating system.
            - Explain your AI strategy approach: use-case selection, governance, delivery, and adoption.
            - Show how healthcare complexity changes the platform and stakeholder model.
            - Talk through one major modernization effort and the business outcomes it unlocked.
            - Address likely concern areas directly: {risks}

            ## Likely Interview Themes
            {chr(10).join(f"- {item}" for item in themes)}
            """
        ),
        f"recruiter_outreach_top_{rank}": textwrap.dedent(
            f"""\
            # Recruiter Outreach Message

            Hi [Recruiter Name] - I’m reaching out regarding the {title} role at {company}. My background is strongest where healthcare analytics, enterprise data platforms, and AI strategy need to come together into measurable business outcomes. This role stood out because it appears to need both executive credibility and real operating execution. I’ve attached a tailored resume and would welcome a conversation if the team is looking for someone who can translate data and AI strategy into stakeholder-trusted delivery.
            """
        ),
        f"linkedin_networking_top_{rank}": textwrap.dedent(
            f"""\
            # LinkedIn Networking Message

            Hi [Name] - I’m exploring the {title} opportunity at {company}. My background is in healthcare analytics, data platform leadership, and AI strategy execution, and the role looks tightly aligned with the kind of executive transformation work I’ve been focused on. If you’re open to it, I’d appreciate a brief perspective on how the team is thinking about the role and where the biggest opportunity is.
            """
        ),
        f"application_checklist_top_{rank}": textwrap.dedent(
            f"""\
            # Application Checklist

            - Validate resume bullets with quantified enterprise outcomes.
            - Submit tailored resume and tailored cover letter.
            - Save the job posting and source URL locally.
            - Send recruiter outreach within 24 hours.
            - Send one networking message to an employee or leader connected to the business.
            - Prepare interview stories for: {', '.join(themes[:3])}.
            """
        ),
    }


def role_story(role: Dict[str, Any]) -> str:
    if "advisors" in role["company"].lower():
        return "Healthcare clients need a practice leader who can turn data and AI strategy into sellable, trusted client outcomes."
    if "medimpact" in role["company"].lower():
        return "The hiring story is clinical and financial value creation through stronger analytics, interoperability, and outcomes reporting."
    if "lyra" in role["company"].lower():
        return "The company needs a visible data and AI executive who can scale both platform capability and enterprise decision value."
    return "The hiring story is executive ownership of data, analytics, and AI strategy tied to measurable business outcomes."


def positioning_for_role(role: Dict[str, Any], profile: Dict[str, Any]) -> str:
    if "impact advisors" in role["company"].lower():
        return "Lead with healthcare transformation credibility, Epic/Fabric/Azure relevance, and an ability to sell and shape executive programs."
    if "medimpact" in role["company"].lower():
        return "Lead with clinical and cost-of-care analytics impact, client storytelling, and enterprise healthcare analytics leadership."
    return "Lead with healthcare analytics credibility, platform modernization, executive stakeholder trust, and AI strategy execution."


def interview_themes(role: Dict[str, Any]) -> List[str]:
    text = role.get("role_text", "").lower()
    themes = ["executive influence", "platform modernization", "analytics operating model", "AI governance"]
    if "client" in text or "consult" in text:
        themes.append("client credibility and practice growth")
    if "clinical" in text:
        themes.append("clinical value and outcomes measurement")
    if "board" in text:
        themes.append("board communication")
    return list(dict.fromkeys(themes))


def resume_keywords(role: Dict[str, Any]) -> List[str]:
    text = role.get("role_text", "")
    candidates = [
        "healthcare analytics",
        "data strategy",
        "AI strategy",
        "data platform",
        "executive leadership",
        "data governance",
        "Power BI",
        "Azure",
        "Microsoft Fabric",
        "Epic",
        "clinical analytics",
        "population health",
        "machine learning",
    ]
    found = [item for item in candidates if item.lower() in text.lower()]
    baseline = ["healthcare analytics", "AI strategy", "data governance", "executive leadership", "platform modernization"]
    return list(dict.fromkeys(found + baseline))[:10]


def _tracker_row(role: Dict[str, Any], priority_rank: int) -> Dict[str, Any]:
    return {
        "priority_rank": priority_rank,
        "role_title": role["title"],
        "company": role["company"],
        "location": role["location"],
        "remote_status": role["remote_status"],
        "job_url": role["job_url"],
        "score": role["score"],
        "fit_score": role["score"],
        "compensation": role["compensation"],
        "application_status": "not_started",
        "fit_rationale": role["fit_summary"],
        "risks_gaps": " | ".join(role["risks_or_gaps"]),
        "next_action": "Tailor resume and outreach if top-3; otherwise monitor and decide.",
    }


def _source_manifest_row(role: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "title": role["title"],
        "company": role["company"],
        "source_name": role["source_name"],
        "source_type": role["source_type"],
        "url": role["job_url"],
        "source_date": role["source_date"],
        "location": role["location"],
        "compensation": role["compensation"],
    }


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
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


def _maybe_write_xlsx(path: Path, rows: List[Dict[str, Any]], artifacts: Dict[str, str], metadata: Dict[str, Dict[str, Any]], key: str, title: str) -> None:
    try:
        from openpyxl import Workbook  # type: ignore
    except Exception:
        return
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Tracker"
    headers: List[str] = []
    for row in rows:
        for field in row.keys():
            if field not in headers:
                headers.append(field)
    sheet.append(headers)
    for row in rows:
        sheet.append([row.get(field, "") for field in headers])
    workbook.save(path)
    _register_artifact(
        artifacts,
        metadata,
        key=key,
        path=path,
        artifact_type="spreadsheet",
        title=title,
        evidence_summary="Workbook version of the role tracker for spreadsheet review.",
    )


def _register_artifact(
    artifacts: Dict[str, str],
    metadata: Dict[str, Dict[str, Any]],
    *,
    key: str,
    path: Path,
    artifact_type: str,
    title: str,
    evidence_summary: str,
) -> None:
    artifacts[key] = str(path.resolve())
    metadata[key] = {
        "key": key,
        "path": str(path.resolve()),
        "type": artifact_type,
        "title": title,
        "evidence_summary": evidence_summary,
        "validation_state": "ready",
        "created_at": path.stat().st_mtime if path.exists() else 0,
    }


def _slug(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return cleaned[:80] or "artifact"


def _company_from_url(url: str) -> str:
    value = str(url or "").replace("https://", "").replace("http://", "").strip()
    if not value:
        return ""
    host = value.split("/", 1)[0]
    host = host.replace("www.", "")
    return host.split(".", 1)[0].replace("-", " ").title()


def _alias_artifact(
    artifacts: Dict[str, str],
    metadata: Dict[str, Dict[str, Any]],
    *,
    source_key: str,
    alias_key: str,
    artifact_type: str,
    title: str,
) -> None:
    path = artifacts.get(source_key, "")
    if not path:
        return
    artifacts[alias_key] = path
    source_meta = dict(metadata.get(source_key, {}) or {})
    metadata[alias_key] = {
        "key": alias_key,
        "path": path,
        "type": artifact_type,
        "title": title,
        "evidence_summary": str(source_meta.get("evidence_summary", "")),
        "validation_state": str(source_meta.get("validation_state", "ready")),
        "created_at": source_meta.get("created_at", 0),
    }
