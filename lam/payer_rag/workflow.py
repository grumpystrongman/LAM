from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from openpyxl import load_workbook

from .analyze import analyze_outliers, read_csv_rows
from .export import (
    build_validation_queue_rows,
    export_workbook,
    write_csv,
    write_dashboard_html,
    write_summary_report,
    write_validation_checklist,
    write_validation_queue_csv,
)
from .ingest import DEFAULT_SERVICE_KEYWORDS, ingest_sources, load_source_manifest, write_ingestion_outputs
from .rag import ask_question, build_index

RUNS_ROOT = Path("data/payer_rag_runs")
DEFAULT_DOMAIN = "insurance payer/plan pricing analysis"
MAJOR_PARAMETER_KEYS = {
    "geography",
    "state",
    "market",
    "domain",
    "requested_outputs",
    "source_constraints",
    "timeframe",
    "required_artifacts",
    "stakeholder_audience",
}
STATE_TOKEN_MAP = {
    "NC": ["nc", "north_carolina", "north carolina"],
    "VA": ["va", "virginia"],
}


@dataclass(slots=True)
class CurrentTaskContract:
    geography: str
    state: str
    market: str
    geography_explicit: bool = False
    domain: str = DEFAULT_DOMAIN
    requested_outputs: list[str] = field(default_factory=list)
    source_constraints: list[str] = field(default_factory=list)
    timeframe: str = "current public data"
    required_artifacts: list[str] = field(default_factory=list)
    stakeholder_audience: str = "payer contracting and healthcare analytics stakeholders"

    def slug(self) -> str:
        return _slugify(self.geography)

    def label(self) -> str:
        return self.geography

    def prefix(self) -> str:
        return self.slug()


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (value or "").strip().lower()).strip("_") or "unknown_market"


def _normalize_service_keywords(service_keywords: Iterable[str] | None) -> list[str]:
    values = [str(item).strip().lower() for item in (service_keywords or DEFAULT_SERVICE_KEYWORDS) if str(item).strip()]
    return list(dict.fromkeys(values))


def _infer_market_tokens(instruction: str) -> tuple[str, str, str, bool]:
    text = re.sub(r"\s+", " ", instruction or "").strip()
    patterns = [
        r"\bfor\s+([A-Z][A-Za-z .'-]+,\s*[A-Z]{2})\b",
        r"\bin\s+([A-Z][A-Za-z .'-]+,\s*[A-Z]{2})\b",
        r"\b([A-Z][A-Za-z .'-]+,\s*[A-Z]{2})\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            geography = re.sub(r"\s+", " ", match.group(1)).strip(" .")
            parts = [part.strip() for part in geography.split(",", 1)]
            market = parts[0]
            state = parts[1] if len(parts) > 1 else ""
            return geography, market, state, True
    low = text.lower()
    if "fairfax" in low and "va" in low:
        return "Fairfax, VA", "Fairfax", "VA", True
    if "fairfax" in low:
        return "Fairfax, VA", "Fairfax", "VA", True
    if "durham" in low and "nc" in low:
        return "Durham, NC", "Durham", "NC", True
    if "durham" in low:
        return "Durham, NC", "Durham", "NC", True
    return "Durham, NC", "Durham", "NC", False


def extract_current_task_contract(instruction: str) -> CurrentTaskContract:
    geography, market, state, geography_explicit = _infer_market_tokens(instruction)
    low = (instruction or "").lower()
    requested_outputs: list[str] = []
    if any(token in low for token in ["rag", "vector store", "retriever", "index"]):
        requested_outputs.append("RAG/vector store")
    if any(token in low for token in ["summary", "stakeholder", "report"]):
        requested_outputs.append("stakeholder summary")
    if any(token in low for token in ["spreadsheet", "xlsx", "workbook", "outreach"]):
        requested_outputs.append("payer outreach spreadsheet")
    if "source manifest" in low or "sources" in low:
        requested_outputs.append("source manifest")
    if "data quality" in low:
        requested_outputs.append("data quality report")
    if "query examples" in low or "example queries" in low:
        requested_outputs.append("RAG query examples")
    if not requested_outputs:
        requested_outputs = [
            "RAG/vector store",
            "stakeholder summary",
            "payer outreach spreadsheet",
            "source manifest",
            "data quality report",
        ]
    source_constraints = [
        "public payer/plan/provider pricing information only",
        "no PHI",
        "no patient-level data",
        "no unsupported fairness claims",
    ]
    required_artifacts = [
        f"{_slugify(geography)}_payer_outreach_candidates.xlsx",
        f"{_slugify(geography)}_payer_dashboard.html",
        f"{_slugify(geography)}_summary_report.md",
        f"{_slugify(geography)}_source_manifest.csv",
        f"{_slugify(geography)}_data_quality_report.md",
        f"{_slugify(geography)}_contract_validation_queue.csv",
        f"{_slugify(geography)}_validation_checklist.md",
        "rag_index/payer_rag.db",
    ]
    return CurrentTaskContract(
        geography=geography,
        state=state,
        market=market,
        geography_explicit=geography_explicit,
        requested_outputs=requested_outputs,
        source_constraints=source_constraints,
        timeframe="current public data",
        required_artifacts=required_artifacts,
    )


def _runs_root() -> Path:
    RUNS_ROOT.mkdir(parents=True, exist_ok=True)
    return RUNS_ROOT


def _new_run_workspace(contract: CurrentTaskContract) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return _runs_root() / f"{contract.slug()}_{ts}"


def _task_contract_path(workspace: Path) -> Path:
    return workspace / "task_contract.json"


def _run_metadata_path(workspace: Path) -> Path:
    return workspace / "run_metadata.json"


def _artifact_paths(workspace: Path, contract: CurrentTaskContract) -> dict[str, str]:
    artifacts_root = workspace / "artifacts"
    normalized_root = workspace / "normalized"
    prefix = contract.prefix()
    return {
        "workbook_xlsx": str((artifacts_root / f"{prefix}_payer_outreach_candidates.xlsx").resolve()),
        "dashboard_html": str((artifacts_root / f"{prefix}_payer_dashboard.html").resolve()),
        "summary_report_md": str((artifacts_root / f"{prefix}_summary_report.md").resolve()),
        "source_manifest_csv": str((artifacts_root / f"{prefix}_source_manifest.csv").resolve()),
        "data_quality_report_md": str((artifacts_root / f"{prefix}_data_quality_report.md").resolve()),
        "outreach_candidates_csv": str((artifacts_root / f"{prefix}_outreach_candidates.csv").resolve()),
        "payer_summary_csv": str((artifacts_root / f"{prefix}_payer_summary.csv").resolve()),
        "validation_queue_csv": str((artifacts_root / f"{prefix}_contract_validation_queue.csv").resolve()),
        "validation_checklist_md": str((artifacts_root / f"{prefix}_validation_checklist.md").resolve()),
        "geography_validation_report_md": str((artifacts_root / f"{prefix}_geography_validation.md").resolve()),
        "rag_index_db": str((artifacts_root / "rag_index" / "payer_rag.db").resolve()),
        "services_csv": str((normalized_root / "services.csv").resolve()),
        "rates_csv": str((normalized_root / "rates.csv").resolve()),
        "task_contract_json": str(_task_contract_path(workspace).resolve()),
        "primary_open_file": str((artifacts_root / f"{prefix}_payer_dashboard.html").resolve()),
    }


def _write_task_contract(workspace: Path, contract: CurrentTaskContract) -> Path:
    path = _task_contract_path(workspace)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(contract), indent=2), encoding="utf-8")
    return path


def _write_run_metadata(
    workspace: Path,
    *,
    contract: CurrentTaskContract,
    invalidated_artifacts: list[str],
    reused_existing_outputs: bool,
    generation_ts: str,
) -> Path:
    payload = {
        "contract": asdict(contract),
        "generation_timestamp": generation_ts,
        "invalidated_artifacts": invalidated_artifacts,
        "reused_existing_outputs": reused_existing_outputs,
    }
    path = _run_metadata_path(workspace)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def _read_task_contract(workspace: Path) -> CurrentTaskContract | None:
    path = _task_contract_path(workspace)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return CurrentTaskContract(**payload)
    except Exception:
        return None


def _workspaces_for_contract(contract: CurrentTaskContract) -> list[Path]:
    roots = [path for path in _runs_root().glob(f"{contract.slug()}_*") if path.is_dir()]
    roots.sort(key=lambda item: item.stat().st_mtime, reverse=True)
    return roots


def _all_workspaces() -> list[Path]:
    roots = [path for path in _runs_root().iterdir() if path.is_dir()]
    roots.sort(key=lambda item: item.stat().st_mtime, reverse=True)
    return roots


def _strict_contract_match(existing: CurrentTaskContract | None, contract: CurrentTaskContract) -> bool:
    if existing is None:
        return False
    for key in MAJOR_PARAMETER_KEYS:
        left = getattr(existing, key, None)
        right = getattr(contract, key, None)
        if left != right:
            return False
    return True


def _artifact_paths_from_existing(workspace: Path, contract: CurrentTaskContract) -> dict[str, str]:
    return _artifact_paths(workspace, contract)


def _stale_tokens(contract: CurrentTaskContract) -> list[str]:
    allowed = {_slugify(contract.geography), _slugify(contract.market), _slugify(contract.state)}
    for token in STATE_TOKEN_MAP.get(contract.state.upper(), []):
        allowed.add(token)
    candidates = ["durham", "durham_nc", "north_carolina", "nc", "fairfax", "fairfax_va", "virginia", "va"]
    return [token for token in candidates if token and token not in allowed]


def _contains_geo_token(text: str, token: str) -> bool:
    escaped = re.escape(token).replace("_", r"[\s_\-]+")
    pattern = rf"(?<![a-z0-9]){escaped}(?![a-z0-9])"
    return re.search(pattern, text.lower()) is not None


def _artifact_text_contains_stale_geography(path: Path, contract: CurrentTaskContract) -> tuple[bool, str]:
    try:
        if path.suffix.lower() == ".xlsx":
            wb = load_workbook(path, read_only=True, data_only=True)
            values: list[str] = []
            for ws in wb.worksheets[:4]:
                for row in ws.iter_rows(min_row=1, max_row=20, values_only=True):
                    values.extend(str(cell) for cell in row if cell not in {None, ""})
            text = " ".join(values).lower()
            wb.close()
        else:
            if path.suffix.lower() not in {".md", ".csv", ".html", ".json", ".txt"}:
                return False, ""
            text = path.read_text(encoding="utf-8", errors="ignore").lower()
    except Exception:
        return False, ""
    requested = contract.geography.lower()
    if requested not in text and path.suffix.lower() in {".md", ".html", ".xlsx"}:
        return True, f"missing requested geography marker '{contract.geography}' in {path.name}"
    for token in _stale_tokens(contract):
        if _contains_geo_token(text, token):
            return True, f"stale geography token '{token}' found in {path.name}"
    return False, ""


def _validate_workspace_relevance(workspace: Path, contract: CurrentTaskContract) -> tuple[bool, list[str]]:
    errors: list[str] = []
    existing = _read_task_contract(workspace)
    if not _strict_contract_match(existing, contract):
        errors.append("task contract mismatch")
        return False, errors
    artifacts = _artifact_paths_from_existing(workspace, contract)
    for key, raw_path in artifacts.items():
        path = Path(raw_path)
        if key == "primary_open_file":
            continue
        if not path.exists():
            errors.append(f"missing artifact: {path.name}")
            continue
        lower_name = path.name.lower()
        if any(token in lower_name for token in _stale_tokens(contract)):
            errors.append(f"stale filename token found: {path.name}")
        bad_text, reason = _artifact_text_contains_stale_geography(path, contract)
        if bad_text:
            errors.append(reason)
    return len(errors) == 0, errors


def _find_latest_valid_workspace(contract: CurrentTaskContract) -> tuple[Path | None, list[str]]:
    invalidated: list[str] = []
    for workspace in _all_workspaces():
        existing = _read_task_contract(workspace)
        if existing is not None and existing.geography != contract.geography:
            invalidated.append(f"{workspace.name}: geography mismatch ({existing.geography} != {contract.geography})")
            continue
        ok, errors = _validate_workspace_relevance(workspace, contract)
        if ok:
            return workspace, invalidated
        invalidated.append(f"{workspace.name}: {'; '.join(errors[:4])}")
    return None, invalidated


def _default_methodology_lines(contract: CurrentTaskContract, outlier_threshold: float) -> list[str]:
    return [
        f"geography: {contract.geography}",
        "data sources: Public provider transparency files, shoppable-service references, and payer transparency reference pages listed in the source manifest.",
        "comparison method: Compare negotiated rates within the same facility, setting, billing class, and service where public machine-readable rates are available.",
        f"threshold: Flag rates more than {outlier_threshold * 100:.0f}% above the peer median as potential pricing outliers.",
        "limitations: Public standard-charge data is not claim-level paid amount data; validate with contracts and sample claims before any outreach.",
        "interpretation guidance: Treat flags as review candidates or possible contract/pricing concerns, not legal conclusions.",
    ]


def _default_limitations(contract: CurrentTaskContract) -> list[str]:
    return [
        f"The build is scoped to {contract.geography} using public transparency and standard-charge data rather than PHI, claims, or remittance-level records.",
        "Some payer transparency and provider reference pages may need manual review if automated fetches are blocked.",
        "Shoppable-service catalogs improve consumer-friendly service matching but may not expose negotiated-rate rows for every payer-plan combination.",
        "Every flagged row should be validated with contracting terms and sample claims before payer outreach.",
    ]


def _rename_output(path: Path, target_name: str) -> Path:
    target = path.with_name(target_name)
    if path.resolve() == target.resolve():
        return path
    if target.exists():
        target.unlink()
    path.replace(target)
    return target


def _rename_outputs_for_contract(workspace: Path, contract: CurrentTaskContract) -> None:
    prefix = contract.prefix()
    artifacts_root = workspace / "artifacts"
    mapping = {
        "durham_nc_payer_outreach_candidates.xlsx": f"{prefix}_payer_outreach_candidates.xlsx",
        "payer_dashboard.html": f"{prefix}_payer_dashboard.html",
        "summary_report.md": f"{prefix}_summary_report.md",
        "source_manifest.csv": f"{prefix}_source_manifest.csv",
        "data_quality_report.md": f"{prefix}_data_quality_report.md",
        "outreach_candidates.csv": f"{prefix}_outreach_candidates.csv",
        "payer_summary.csv": f"{prefix}_payer_summary.csv",
        "contract_validation_queue.csv": f"{prefix}_contract_validation_queue.csv",
        "validation_checklist.md": f"{prefix}_validation_checklist.md",
    }
    for source_name, target_name in mapping.items():
        source = artifacts_root / source_name
        if source.exists():
            _rename_output(source, target_name)


def _write_geography_validation_report(
    workspace: Path,
    contract: CurrentTaskContract,
    artifacts: dict[str, str],
) -> tuple[Path, dict[str, Any]]:
    errors: list[str] = []
    checked: list[str] = []
    for key, raw_path in artifacts.items():
        if key in {"primary_open_file", "rag_index_db", "services_csv", "rates_csv", "task_contract_json", "geography_validation_report_md"}:
            continue
        path = Path(raw_path)
        if not path.exists():
            errors.append(f"missing artifact: {path.name}")
            continue
        checked.append(path.name)
        if any(_contains_geo_token(path.name.lower(), token) for token in _stale_tokens(contract)):
            errors.append(f"wrong geography in filename: {path.name}")
        bad_text, reason = _artifact_text_contains_stale_geography(path, contract)
        if bad_text:
            errors.append(reason)
    target = Path(artifacts["geography_validation_report_md"])
    status = "passed" if not errors else "failed"
    target.write_text(
        "# Geography Consistency Validation\n\n"
        f"- Requested geography: {contract.geography}\n"
        f"- Status: {status}\n"
        f"- Checked artifacts: {', '.join(checked) if checked else 'none'}\n\n"
        "## Findings\n\n"
        + ("\n".join(f"- {item}" for item in errors) if errors else "- All checked artifacts matched the current geography.") + "\n",
        encoding="utf-8",
    )
    return target, {"passed": not errors, "errors": errors, "checked_artifacts": checked}


def build_workspace(
    *,
    contract: CurrentTaskContract,
    workspace: str | Path | None = None,
    manifest_path: str | Path | None = None,
    service_keywords: Iterable[str] | None = None,
    max_services_per_source: int = 24,
    outlier_threshold: float = 0.2,
    min_peer_count: int = 3,
    offline_fallback: bool = True,
    invalidated_artifacts: list[str] | None = None,
) -> dict[str, Any]:
    root = Path(workspace) if workspace else _new_run_workspace(contract)
    root.mkdir(parents=True, exist_ok=True)
    generation_ts = datetime.now().isoformat(timespec="seconds")
    manifest = load_source_manifest(manifest_path, offline=False)
    bundle = ingest_sources(
        manifest,
        service_keywords=_normalize_service_keywords(service_keywords),
        max_services_per_source=max_services_per_source,
        offline_fallback=offline_fallback,
    )
    outputs = write_ingestion_outputs(root, bundle, geography_label=contract.geography)
    rates = read_csv_rows(outputs["rates_csv"])
    plans = read_csv_rows(outputs["plans_csv"])
    payers = read_csv_rows(outputs["payers_csv"])
    services = read_csv_rows(outputs["services_csv"])
    analysis = analyze_outliers(
        rates=rates,
        plans=plans,
        payers=payers,
        services=services,
        outlier_threshold=outlier_threshold,
        min_peer_count=min_peer_count,
    )
    candidates = analysis["candidates"]
    payer_summary = analysis["payer_summary"]
    source_manifest = read_csv_rows(outputs["source_manifest_csv"])
    write_csv(
        root / "artifacts" / "outreach_candidates.csv",
        candidates,
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
    write_csv(
        root / "artifacts" / "payer_summary.csv",
        payer_summary,
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
    validation_rows = build_validation_queue_rows(candidates)
    write_validation_queue_csv(root, validation_rows)
    write_validation_checklist(root, validation_rows, geography_label=contract.geography)
    export_workbook(
        root,
        candidates=candidates,
        payer_summary=payer_summary,
        source_manifest=source_manifest,
        methodology_lines=_default_methodology_lines(contract, outlier_threshold),
    )
    write_dashboard_html(root, geography_label=contract.geography)
    write_summary_report(
        root,
        candidates=candidates,
        payer_summary=payer_summary,
        source_manifest=source_manifest,
        limitations=_default_limitations(contract),
        geography_label=contract.geography,
    )
    _rename_outputs_for_contract(root, contract)
    _write_task_contract(root, contract)
    _write_run_metadata(
        root,
        contract=contract,
        invalidated_artifacts=list(invalidated_artifacts or []),
        reused_existing_outputs=False,
        generation_ts=generation_ts,
    )
    build_index(root)
    artifacts = _artifact_paths(root, contract)
    outputs["source_manifest_csv"] = Path(artifacts["source_manifest_csv"])
    outputs["data_quality_report_md"] = Path(artifacts["data_quality_report_md"])
    validation_path, validation = _write_geography_validation_report(root, contract, artifacts)
    artifacts["geography_validation_report_md"] = str(validation_path.resolve())
    return {
        "workspace": str(root.resolve()),
        "artifact_paths": artifacts,
        "ingestion_outputs": {key: str(Path(value).resolve()) for key, value in outputs.items()},
        "counts": {
            "payers": len(bundle["payers"]),
            "plans": len(bundle["plans"]),
            "services": len(bundle["services"]),
            "rates": len(bundle["rates"]),
            "outreach_candidates": len(candidates),
            "validation_queue": len(validation_rows),
        },
        "issues": list(bundle["issues"]),
        "top_candidates": candidates[:5],
        "current_task_contract": asdict(contract),
        "generation_timestamp": generation_ts,
        "invalidated_artifacts": list(invalidated_artifacts or []),
        "geography_validation": validation,
    }


def ensure_workspace(
    *,
    contract: CurrentTaskContract,
    manifest_path: str | Path | None = None,
    service_keywords: Iterable[str] | None = None,
    max_services_per_source: int = 24,
    outlier_threshold: float = 0.2,
    min_peer_count: int = 3,
    offline_fallback: bool = True,
    allow_reuse: bool = True,
) -> dict[str, Any]:
    reused_existing_outputs = False
    invalidated_artifacts: list[str] = []
    workspace: Path | None = None
    if not allow_reuse:
        invalidated_artifacts = [
            f"{path.name}: explicit geography request requires a fresh run"
            for path in _all_workspaces()[:10]
        ]
    if allow_reuse:
        workspace, invalidated_artifacts = _find_latest_valid_workspace(contract)
        reused_existing_outputs = workspace is not None
    if workspace is not None:
        root = workspace
        artifacts = _artifact_paths(root, contract)
        validation_path, validation = _write_geography_validation_report(root, contract, artifacts)
        artifacts["geography_validation_report_md"] = str(validation_path.resolve())
        counts = {
            "payers": len(read_csv_rows(root / "normalized" / "payers.csv")) if (root / "normalized" / "payers.csv").exists() else 0,
            "plans": len(read_csv_rows(root / "normalized" / "plans.csv")) if (root / "normalized" / "plans.csv").exists() else 0,
            "services": len(read_csv_rows(root / "normalized" / "services.csv")) if (root / "normalized" / "services.csv").exists() else 0,
            "rates": len(read_csv_rows(root / "normalized" / "rates.csv")) if (root / "normalized" / "rates.csv").exists() else 0,
            "outreach_candidates": len(read_csv_rows(Path(artifacts["outreach_candidates_csv"]))) if Path(artifacts["outreach_candidates_csv"]).exists() else 0,
            "validation_queue": len(read_csv_rows(Path(artifacts["validation_queue_csv"]))) if Path(artifacts["validation_queue_csv"]).exists() else 0,
        }
        top_candidates = read_csv_rows(Path(artifacts["outreach_candidates_csv"]))[:5] if Path(artifacts["outreach_candidates_csv"]).exists() else []
        return {
            "workspace": str(root.resolve()),
            "artifact_paths": artifacts,
            "counts": counts,
            "top_candidates": top_candidates,
            "current_task_contract": asdict(contract),
            "reused_existing_outputs": reused_existing_outputs,
            "invalidated_artifacts": invalidated_artifacts,
            "generation_timestamp": json.loads(_run_metadata_path(root).read_text(encoding="utf-8")).get("generation_timestamp", "")
            if _run_metadata_path(root).exists()
            else "",
            "geography_validation": validation,
        }
    return build_workspace(
        contract=contract,
        workspace=None,
        manifest_path=manifest_path,
        service_keywords=service_keywords,
        max_services_per_source=max_services_per_source,
        outlier_threshold=outlier_threshold,
        min_peer_count=min_peer_count,
        offline_fallback=offline_fallback,
        invalidated_artifacts=invalidated_artifacts,
    ) | {"reused_existing_outputs": False}


def ask_workspace_question(
    question: str,
    *,
    contract: CurrentTaskContract,
    workspace: str | Path | None = None,
) -> dict[str, Any]:
    root = Path(workspace) if workspace else (_find_latest_valid_workspace(contract)[0] or Path())
    if not root or not root.exists():
        return {
            "answer": f"No valid workspace exists yet for {contract.geography}. Rebuild the package for this geography first.",
            "sources": [],
            "workspace": "",
            "artifacts": {},
            "current_task_contract": asdict(contract),
        }
    response = ask_question(root, question)
    response["workspace"] = str(root.resolve())
    response["artifacts"] = _artifact_paths(root, contract)
    response["current_task_contract"] = asdict(contract)
    return response
