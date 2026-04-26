from __future__ import annotations

import json
from pathlib import Path

from . import DEFAULT_WORKSPACE
from .analyze import analyze_outliers, read_csv_rows
from .export import export_workbook, write_csv, write_dashboard_html, write_summary_report
from .ingest import (
    DEFAULT_SERVICE_KEYWORDS,
    ingest_sources,
    load_source_manifest,
    write_default_manifest,
    write_ingestion_outputs,
)
from .rag import ask_question, build_index
from .workflow import ask_workspace_question, build_workspace, extract_current_task_contract


def _workspace(args) -> Path:
    return Path(getattr(args, "workspace", DEFAULT_WORKSPACE))


def payer_ingest(args) -> None:
    workspace = _workspace(args)
    manifest = load_source_manifest(getattr(args, "manifest", None), offline=getattr(args, "offline", False))
    bundle = ingest_sources(
        manifest,
        service_keywords=[part.strip().lower() for part in args.service_keywords.split(",") if part.strip()]
        if getattr(args, "service_keywords", "")
        else DEFAULT_SERVICE_KEYWORDS,
        max_services_per_source=args.max_services_per_source,
        offline_fallback=True,
    )
    outputs = write_ingestion_outputs(workspace, bundle)
    print(json.dumps({"status": "ok", "workspace": str(workspace), "outputs": {k: str(v) for k, v in outputs.items()}}, indent=2))


def payer_analyze(args) -> None:
    workspace = _workspace(args)
    rates = read_csv_rows(workspace / "normalized" / "rates.csv")
    plans = read_csv_rows(workspace / "normalized" / "plans.csv")
    payers = read_csv_rows(workspace / "normalized" / "payers.csv")
    services = read_csv_rows(workspace / "normalized" / "services.csv")
    results = analyze_outliers(
        rates=rates,
        plans=plans,
        payers=payers,
        services=services,
        outlier_threshold=args.outlier_threshold,
        min_peer_count=args.min_peer_count,
    )
    candidates_csv = write_csv(
        workspace / "artifacts" / "outreach_candidates.csv",
        results["candidates"],
        [
            "priority_rank",
            "payer_name",
            "plan_name",
            "service",
            "code",
            "payer_rate",
            "peer_median",
            "peer_min",
            "peer_max",
            "variance_percent",
            "reason_flagged",
            "confidence",
            "recommended_action",
            "source_evidence",
            "compared_service",
            "facility_name",
        ],
    )
    payer_summary_csv = write_csv(
        workspace / "artifacts" / "payer_summary.csv",
        results["payer_summary"],
        [
            "payer_name",
            "number_of_plans",
            "number_of_services_analyzed",
            "number_of_outlier_flags",
            "average_variance",
            "top_concern",
            "confidence",
        ],
    )
    print(
        json.dumps(
            {
                "status": "ok",
                "workspace": str(workspace),
                "candidate_count": len(results["candidates"]),
                "outreach_candidates_csv": str(candidates_csv),
                "payer_summary_csv": str(payer_summary_csv),
            },
            indent=2,
        )
    )


def payer_build_index(args) -> None:
    workspace = _workspace(args)
    index = build_index(workspace)
    print(json.dumps({"status": "ok", "index_path": str(index)}, indent=2))


def payer_export(args) -> None:
    contract = extract_current_task_contract(getattr(args, "instruction", "") or "Durham, NC payer pricing review")
    result = build_workspace(
        contract=contract,
        workspace=_workspace(args),
        manifest_path=getattr(args, "manifest", None),
        service_keywords=[part.strip().lower() for part in args.service_keywords.split(",") if part.strip()]
        if getattr(args, "service_keywords", "")
        else DEFAULT_SERVICE_KEYWORDS,
        max_services_per_source=args.max_services_per_source,
        outlier_threshold=args.outlier_threshold,
        min_peer_count=args.min_peer_count,
        offline_fallback=True,
    )
    print(json.dumps({"status": "ok", **result}, indent=2))


def payer_ask(args) -> None:
    contract = extract_current_task_contract(args.question or "Durham, NC payer pricing review")
    result = ask_workspace_question(args.question, contract=contract, workspace=_workspace(args))
    print(json.dumps(result, indent=2))


def payer_build(args) -> None:
    contract = extract_current_task_contract(getattr(args, "instruction", "") or "Durham, NC payer pricing review")
    result = build_workspace(
        contract=contract,
        workspace=_workspace(args),
        manifest_path=getattr(args, "manifest", None),
        service_keywords=[part.strip().lower() for part in args.service_keywords.split(",") if part.strip()]
        if getattr(args, "service_keywords", "")
        else DEFAULT_SERVICE_KEYWORDS,
        max_services_per_source=args.max_services_per_source,
        outlier_threshold=args.outlier_threshold,
        min_peer_count=args.min_peer_count,
        offline_fallback=True,
    )
    print(json.dumps({"status": "ok", **result}, indent=2))


def payer_init_manifest(args) -> None:
    path = write_default_manifest(args.path)
    print(json.dumps({"status": "ok", "manifest_path": str(path)}, indent=2))
