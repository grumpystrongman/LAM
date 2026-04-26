from __future__ import annotations

import argparse
import os
from pathlib import Path

from lam.adapters.excel_adapter import ExcelAdapter
from lam.adapters.playwright_adapter import PlaywrightAdapter
from lam.adapters.uia_adapter import UIAAdapter
from lam.deep_workbench.workflow import build_workspace as build_code_workbench_workspace
from lam.deep_workbench.workflow import extract_workbench_contract
from lam.dsl.parser import load_workflow
from lam.endpoint_agent.kill_switch import KillSwitch
from lam.endpoint_agent.runner import Runner
from lam.governance.approval_client import ApprovalClient
from lam.governance.audit_logger import AuditLogger
from lam.governance.policy_engine import PolicyEngine
from lam.governance.redaction import Redactor
from lam.interface.web_ui import run_ui_server
from lam.payer_rag.cli import (
    payer_analyze,
    payer_ask,
    payer_build,
    payer_build_index,
    payer_export,
    payer_ingest,
    payer_init_manifest,
)
from lam.services.api_server import ApiAuthConfig, ControlPlaneService, run_http_server
from lam.services.audit_store import AuditStore
from lam.services.sqlite_approval_service import SqliteApprovalService
from lam.services.workflow_store import WorkflowStore


def build_runner(config_dir: str = "config", audit_backend: str = "sqlite") -> Runner:
    policy = PolicyEngine.from_config_dir(config_dir)
    approval_service = SqliteApprovalService()
    approval_client = ApprovalClient(approval_service)
    audit_store = AuditStore(path="data/audit/events.db", backend=audit_backend)
    audit = AuditLogger(sink=audit_store.sink, redactor=Redactor())
    domain_allowlist = policy.config.get("domain_allowlist", {}).get("domains", [])
    adapters = {
        "excel": ExcelAdapter(),
        "playwright": PlaywrightAdapter(domain_allowlist=domain_allowlist, dry_run=True),
        "uia": UIAAdapter(dry_run=True),
    }
    return Runner(
        policy_engine=policy,
        approval_client=approval_client,
        audit_logger=audit,
        adapters=adapters,
        kill_switch=KillSwitch(),
        ask_user_handler=lambda q, options, ctx: options[0] if options else "ok",
    )


def build_control_plane(config_dir: str = "config", audit_backend: str = "sqlite") -> ControlPlaneService:
    policy = PolicyEngine.from_config_dir(config_dir)
    workflow_store = WorkflowStore(root="data/workflows")
    approval_service = SqliteApprovalService(path="data/approvals/approvals.db")
    audit_store = AuditStore(path="data/audit/events.db", backend=audit_backend)
    audit_logger = AuditLogger(sink=audit_store.sink, redactor=Redactor())
    return ControlPlaneService(
        policy_engine=policy,
        approval_service=approval_service,
        workflow_store=workflow_store,
        audit_logger=audit_logger,
    )


def run_workflow(args: argparse.Namespace) -> None:
    runner = build_runner(config_dir=args.config_dir, audit_backend=args.audit_backend)
    workflow = load_workflow(Path(args.workflow))
    identity = {
        "user": {
            "user_id": args.user_id,
            "role": args.role,
            "department": args.department,
            "clearance": args.clearance,
        },
        "device": {"managed": True, "compliant": True, "network_zone": "corp"},
    }
    result = runner.run(workflow, identity)
    print(
        "Run status:"
        f" {result.status}, executed_steps={result.executed_steps}, blocked_step_id={result.blocked_step_id}, errors={result.errors}"
    )


def serve_control_plane(args: argparse.Namespace) -> None:
    service = build_control_plane(config_dir=args.config_dir, audit_backend=args.audit_backend)
    auth = ApiAuthConfig(
        api_key=args.api_key or os.getenv("LAM_API_KEY", ""),
        bearer_secret=args.bearer_secret or os.getenv("LAM_BEARER_SECRET", ""),
        bearer_issuer=args.bearer_issuer or os.getenv("LAM_BEARER_ISSUER", "lam-control-plane"),
        allow_anonymous_health=True,
    )
    run_http_server(service=service, host=args.host, port=args.port, auth_config=auth)


def validate_audit(args: argparse.Namespace) -> None:
    service = build_control_plane(config_dir=args.config_dir, audit_backend=args.audit_backend)
    result = service.validate_audit(actor_id=args.actor_id)
    print(result)


def workbench_create(args: argparse.Namespace) -> None:
    contract = extract_workbench_contract(args.instruction, workspace_root=args.workspace_root)
    result = build_code_workbench_workspace(contract=contract, open_vscode=not args.no_open_vscode)
    print(result)


def main() -> None:
    parser = argparse.ArgumentParser(description="LAM Governance-First Runtime and Control Plane")
    sub = parser.add_subparsers(dest="command", required=True)

    run_parser = sub.add_parser("run", help="Run a workflow in deterministic governance-first mode")
    run_parser.add_argument("--workflow", required=True, help="Path to workflow YAML/JSON")
    run_parser.add_argument("--config-dir", default="config")
    run_parser.add_argument("--audit-backend", choices=["sqlite", "jsonl"], default="sqlite")
    run_parser.add_argument("--user-id", default="local-user")
    run_parser.add_argument("--role", default="Runner")
    run_parser.add_argument("--department", default="Claims")
    run_parser.add_argument("--clearance", default="high")
    run_parser.set_defaults(func=run_workflow)

    serve_parser = sub.add_parser("serve-control-plane", help="Run control-plane HTTP API")
    serve_parser.add_argument("--config-dir", default="config")
    serve_parser.add_argument("--audit-backend", choices=["sqlite", "jsonl"], default="sqlite")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8787)
    serve_parser.add_argument("--api-key", default="")
    serve_parser.add_argument("--bearer-secret", default="")
    serve_parser.add_argument("--bearer-issuer", default="lam-control-plane")
    serve_parser.set_defaults(func=serve_control_plane)

    validate_parser = sub.add_parser("validate-audit", help="Validate audit hash chain")
    validate_parser.add_argument("--config-dir", default="config")
    validate_parser.add_argument("--audit-backend", choices=["sqlite", "jsonl"], default="sqlite")
    validate_parser.add_argument("--actor-id", default="auditor")
    validate_parser.set_defaults(func=validate_audit)

    ui_parser = sub.add_parser("serve-ui", help="Run local Windows operator interface")
    ui_parser.add_argument("--host", default="127.0.0.1")
    ui_parser.add_argument("--port", type=int, default=8795)
    ui_parser.add_argument("--no-open-browser", action="store_true")
    ui_parser.set_defaults(
        func=lambda a: run_ui_server(host=a.host, port=a.port, open_browser=not a.no_open_browser)
    )

    payer_ingest_parser = sub.add_parser("payer-ingest", help="Ingest Durham payer pricing sources")
    payer_ingest_parser.add_argument("--workspace", default="data/payer_rag")
    payer_ingest_parser.add_argument("--manifest", default="")
    payer_ingest_parser.add_argument("--offline", action="store_true")
    payer_ingest_parser.add_argument("--max-services-per-source", type=int, default=18)
    payer_ingest_parser.add_argument(
        "--service-keywords",
        default="mri,ct,colonoscopy,emergency,ultrasound,x-ray,office visit,endoscopy,heart,transplant",
    )
    payer_ingest_parser.set_defaults(func=payer_ingest)

    payer_analyze_parser = sub.add_parser("payer-analyze", help="Analyze outliers in normalized payer pricing data")
    payer_analyze_parser.add_argument("--workspace", default="data/payer_rag")
    payer_analyze_parser.add_argument("--outlier-threshold", type=float, default=0.2)
    payer_analyze_parser.add_argument("--min-peer-count", type=int, default=3)
    payer_analyze_parser.set_defaults(func=payer_analyze)

    payer_build_index_parser = sub.add_parser("payer-build-index", help="Build the local payer RAG index")
    payer_build_index_parser.add_argument("--workspace", default="data/payer_rag")
    payer_build_index_parser.set_defaults(func=payer_build_index)

    payer_export_parser = sub.add_parser("payer-export", help="Export stakeholder artifacts for payer pricing review")
    payer_export_parser.add_argument("--workspace", default="data/payer_rag")
    payer_export_parser.add_argument("--outlier-threshold", type=float, default=0.2)
    payer_export_parser.set_defaults(func=payer_export)

    payer_ask_parser = sub.add_parser("payer-ask", help="Ask a question against the payer RAG workspace")
    payer_ask_parser.add_argument("--workspace", default="data/payer_rag")
    payer_ask_parser.add_argument("--question", required=True)
    payer_ask_parser.set_defaults(func=payer_ask)

    payer_build_parser = sub.add_parser("payer-build", help="Run the end-to-end Durham payer build")
    payer_build_parser.add_argument("--workspace", default="data/payer_rag")
    payer_build_parser.add_argument("--manifest", default="")
    payer_build_parser.add_argument("--offline", action="store_true")
    payer_build_parser.add_argument("--max-services-per-source", type=int, default=18)
    payer_build_parser.add_argument(
        "--service-keywords",
        default="mri,ct,colonoscopy,emergency,ultrasound,x-ray,office visit,endoscopy,heart,transplant",
    )
    payer_build_parser.add_argument("--outlier-threshold", type=float, default=0.2)
    payer_build_parser.add_argument("--min-peer-count", type=int, default=3)
    payer_build_parser.set_defaults(func=payer_build)

    payer_manifest_parser = sub.add_parser("payer-init-manifest", help="Write the default Durham payer source manifest")
    payer_manifest_parser.add_argument("--path", default="data/payer_rag/source_manifest.json")
    payer_manifest_parser.set_defaults(func=payer_init_manifest)

    workbench_parser = sub.add_parser("workbench-create", help="Create a fresh deep-work code workspace")
    workbench_parser.add_argument("--instruction", required=True)
    workbench_parser.add_argument("--workspace-root", default="data/deep_work_runs")
    workbench_parser.add_argument("--no-open-vscode", action="store_true")
    workbench_parser.set_defaults(func=workbench_create)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
