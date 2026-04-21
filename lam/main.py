from __future__ import annotations

import argparse
import os
from pathlib import Path

from lam.adapters.excel_adapter import ExcelAdapter
from lam.adapters.playwright_adapter import PlaywrightAdapter
from lam.adapters.uia_adapter import UIAAdapter
from lam.dsl.parser import load_workflow
from lam.endpoint_agent.kill_switch import KillSwitch
from lam.endpoint_agent.runner import Runner
from lam.governance.approval_client import ApprovalClient
from lam.governance.audit_logger import AuditLogger
from lam.governance.policy_engine import PolicyEngine
from lam.governance.redaction import Redactor
from lam.interface.web_ui import run_ui_server
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

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
