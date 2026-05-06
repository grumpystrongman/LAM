from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
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
from lam.learn.skill_library import SkillLibrary
from lam.learn.skill_runtime import SkillPracticeRuntime
from lam.learn.topic_mastery_runtime import TopicMasteryRuntime
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


def launch_ui_background(args: argparse.Namespace) -> None:
    pythonw = shutil.which("pythonw") or sys.executable
    child_args = [pythonw, "-m", "lam.main", "ui", "--host", args.host, "--port", str(args.port), "--no-open-browser"]
    creationflags = 0x00000008 if os.name == "nt" else 0  # DETACHED_PROCESS
    process = subprocess.Popen(  # noqa: S603
        child_args,
        cwd=str(Path.cwd()),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=creationflags,
        close_fds=True,
    )
    print(
        json.dumps(
            {
                "ok": True,
                "mode": "ui_background",
                "pid": process.pid,
                "url": f"http://{args.host}:{args.port}",
                "browser_opened": False,
            },
            indent=2,
        )
    )


def validate_audit(args: argparse.Namespace) -> None:
    service = build_control_plane(config_dir=args.config_dir, audit_backend=args.audit_backend)
    result = service.validate_audit(actor_id=args.actor_id)
    print(result)


def workbench_create(args: argparse.Namespace) -> None:
    contract = extract_workbench_contract(args.instruction, workspace_root=args.workspace_root)
    result = build_code_workbench_workspace(contract=contract, open_vscode=not args.no_open_vscode)
    print(result)


def topic_learn(args: argparse.Namespace) -> None:
    runtime = TopicMasteryRuntime()
    context = {
        "workspace_dir": args.workspace,
        "topic": args.topic or "",
        "seed_url": args.seed_url or "",
        "skill_library_root": args.skill_root,
    }
    result = runtime.run(args.instruction, context=context)
    _print_result(result, output_format=args.output)


def skill_list(args: argparse.Namespace) -> None:
    library = SkillLibrary(args.skill_root)
    _print_result({"skills": library.list_skills()}, output_format=args.output)


def skill_show(args: argparse.Namespace) -> None:
    library = SkillLibrary(args.skill_root)
    result = library.load_skill(args.skill_id, args.version or "")
    _print_result(result or {"error": "skill_not_found"}, output_format=args.output)


def skill_diff(args: argparse.Namespace) -> None:
    library = SkillLibrary(args.skill_root)
    result = library.diff_versions(args.skill_id, args.left_version or "", args.right_version or "")
    _print_result(result, output_format=args.output)


def skill_practice_preview(args: argparse.Namespace) -> None:
    library = SkillLibrary(args.skill_root)
    skill = library.load_skill(args.skill_id, args.version or "")
    result = SkillPracticeRuntime().build_preview(skill, mode="safe_practice") if skill else {"error": "skill_not_found"}
    _print_result(result, output_format=args.output)


def skill_practice_run(args: argparse.Namespace) -> None:
    library = SkillLibrary(args.skill_root)
    skill = library.load_skill(args.skill_id, args.version or "")
    if not skill:
        _print_result({"error": "skill_not_found"}, output_format=args.output)
        return
    result = SkillPracticeRuntime().execute_practice(skill, mode="safe_practice")
    library.record_practice_run(
        args.skill_id,
        args.version or str(skill.get("version", "")),
        {
            "ok": bool(result.get("ok", False)),
            "checkpoint_count": len(list((result.get("preview", {}) or {}).get("checkpoints", []) or [])),
            "failed_checkpoint_id": str(result.get("failed_checkpoint_id", "") or ""),
            "failed_checkpoint_name": str(result.get("failed_checkpoint_name", "") or ""),
        },
    )
    _print_result(result, output_format=args.output)


def skill_refresh(args: argparse.Namespace) -> None:
    library = SkillLibrary(args.skill_root)
    plan = library.build_refresh_plan(args.skill_id, args.version or "", reason=args.reason, source_url=args.source_url or "")
    runtime = TopicMasteryRuntime()
    result = runtime.run(
        plan["recommended_instruction"],
        context={
            "workspace_dir": args.workspace,
            "topic": plan.get("topic", ""),
            "seed_url": plan.get("seed_url", ""),
            "existing_skill_version": args.version or "",
            "skill_library_root": args.skill_root,
        },
    )
    library.record_refresh_run(
        args.skill_id,
        args.version or "",
        {
            "status": str(result.get("status", "") or ""),
            "selected_sources": int((result.get("source_discovery", {}) or {}).get("selected", 0) or 0),
            "runtime_quality": str(((result.get("source_discovery", {}) or {}).get("adapter_summary", {}) or {}).get("runtime_quality", "") or ""),
        },
    )
    _print_result(result, output_format=args.output)


def _print_result(result: object, *, output_format: str = "text") -> None:
    if output_format == "json":
        print(json.dumps(result, indent=2))
        return
    if isinstance(result, dict):
        for key, value in result.items():
            if isinstance(value, (dict, list)):
                print(f"{key}: {json.dumps(value, indent=2)}")
            else:
                print(f"{key}: {value}")
        return
    print(result)


def _add_output_arg(parser: argparse.ArgumentParser, default: str = "text") -> None:
    parser.add_argument(
        "--output",
        choices=["text", "json"],
        default=default,
        help="Choose human-readable text or machine-readable JSON output",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="OpenLAMb operator runtime, topic learning, and local control plane",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python -m lam.main ui\n"
            "  python -m lam.main topic-learn --instruction \"Learn how to build a Power BI KPI dashboard\" --seed-url https://youtube.com/example\n"
            "  python -m lam.main skill-list --output json\n"
            "  python -m lam.main skill-practice-preview --skill-id skill_power_bi_kpi_dashboard\n"
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run_parser = sub.add_parser(
        "run",
        help="Run a workflow in deterministic governance-first mode",
        description="Execute a saved OpenLAMb workflow through the governance-first runner.",
    )
    run_parser.add_argument("--workflow", required=True, help="Path to workflow YAML/JSON")
    run_parser.add_argument("--config-dir", default="config")
    run_parser.add_argument("--audit-backend", choices=["sqlite", "jsonl"], default="sqlite")
    run_parser.add_argument("--user-id", default="local-user")
    run_parser.add_argument("--role", default="Runner")
    run_parser.add_argument("--department", default="Claims")
    run_parser.add_argument("--clearance", default="high")
    run_parser.set_defaults(func=run_workflow)

    serve_parser = sub.add_parser(
        "serve-control-plane",
        help="Run control-plane HTTP API",
        description="Start the authenticated local control-plane API for approvals, workflows, and audit-backed operations.",
    )
    serve_parser.add_argument("--config-dir", default="config")
    serve_parser.add_argument("--audit-backend", choices=["sqlite", "jsonl"], default="sqlite")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8787)
    serve_parser.add_argument("--api-key", default="")
    serve_parser.add_argument("--bearer-secret", default="")
    serve_parser.add_argument("--bearer-issuer", default="lam-control-plane")
    serve_parser.set_defaults(func=serve_control_plane)

    validate_parser = sub.add_parser(
        "validate-audit",
        help="Validate audit hash chain",
        description="Recompute and validate the immutable local audit chain.",
    )
    validate_parser.add_argument("--config-dir", default="config")
    validate_parser.add_argument("--audit-backend", choices=["sqlite", "jsonl"], default="sqlite")
    validate_parser.add_argument("--actor-id", default="auditor")
    validate_parser.set_defaults(func=validate_audit)

    ui_parser = sub.add_parser(
        "serve-ui",
        help="Run local Windows operator interface",
        description=(
            "Launch the commercial chat/canvas operator UI.\n"
            "Use --background for a detached local UI process that keeps running after the shell exits."
        ),
        epilog=(
            "Examples:\n"
            "  python -m lam.main ui\n"
            "  python -m lam.main ui --port 8814 --background\n"
            "  python -m lam.main ui --host 127.0.0.1 --port 8795 --no-open-browser\n"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    ui_parser.add_argument("--host", default="127.0.0.1")
    ui_parser.add_argument("--port", type=int, default=8795)
    ui_parser.add_argument("--no-open-browser", action="store_true")
    ui_parser.add_argument("--background", action="store_true", help="Launch the UI as a detached background process")
    ui_parser.set_defaults(
        func=lambda a: launch_ui_background(a)
        if a.background
        else run_ui_server(host=a.host, port=a.port, open_browser=not a.no_open_browser)
    )

    ui_alias_parser = sub.add_parser(
        "ui",
        help="Alias for serve-ui",
        description="Alias for serve-ui.",
        epilog=(
            "Examples:\n"
            "  python -m lam.main ui\n"
            "  python -m lam.main ui --background\n"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    ui_alias_parser.add_argument("--host", default="127.0.0.1")
    ui_alias_parser.add_argument("--port", type=int, default=8795)
    ui_alias_parser.add_argument("--no-open-browser", action="store_true")
    ui_alias_parser.add_argument("--background", action="store_true", help="Launch the UI as a detached background process")
    ui_alias_parser.set_defaults(
        func=lambda a: launch_ui_background(a)
        if a.background
        else run_ui_server(host=a.host, port=a.port, open_browser=not a.no_open_browser)
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

    workbench_parser = sub.add_parser(
        "workbench-create",
        help="Create a fresh deep-work code workspace",
        description="Create a fresh code workspace with analysis notes, scaffolded source, and smoke tests.",
    )
    workbench_parser.add_argument("--instruction", required=True)
    workbench_parser.add_argument("--workspace-root", default="data/deep_work_runs")
    workbench_parser.add_argument("--no-open-vscode", action="store_true")
    workbench_parser.set_defaults(func=workbench_create)

    topic_parser = sub.add_parser(
        "topic-learn",
        help="Run Topic Mastery Learn Mode from the CLI",
        description=(
            "Learn a topic from a seed video URL or a topic-only prompt.\n"
            "OpenLAMb will discover related sources, synthesize a workflow, and save a reusable skill."
        ),
        epilog=(
            "Examples:\n"
            "  python -m lam.main topic-learn --instruction \"Learn how to build a Power BI KPI dashboard\" --seed-url https://youtube.com/example\n"
            "  python -m lam.main topic-learn --instruction \"Learn how to create a grant budget narrative\" --topic \"grant budget narrative\" --output json\n"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    topic_parser.add_argument("--instruction", required=True, help="Prompt or mission instruction for learning the topic")
    topic_parser.add_argument("--topic", default="", help="Optional explicit topic override")
    topic_parser.add_argument("--seed-url", default="", help="Optional seed video/source URL")
    topic_parser.add_argument("--workspace", default="data/learn_cli_run", help="Workspace for generated learn artifacts")
    topic_parser.add_argument("--skill-root", default="data/learned_skills", help="Versioned learned-skill library root")
    _add_output_arg(topic_parser, default="text")
    topic_parser.set_defaults(func=topic_learn)

    skill_list_parser = sub.add_parser(
        "skill-list",
        help="List learned skills",
        description="List all learned skills in the local versioned skill library.",
    )
    skill_list_parser.add_argument("--skill-root", default="data/learned_skills")
    _add_output_arg(skill_list_parser, default="text")
    skill_list_parser.set_defaults(func=skill_list)

    skill_show_parser = sub.add_parser(
        "skill-show",
        help="Show a learned skill version",
        description="Load one learned skill version from the local skill library.",
    )
    skill_show_parser.add_argument("--skill-id", required=True)
    skill_show_parser.add_argument("--version", default="")
    skill_show_parser.add_argument("--skill-root", default="data/learned_skills")
    _add_output_arg(skill_show_parser, default="json")
    skill_show_parser.set_defaults(func=skill_show)

    skill_diff_parser = sub.add_parser(
        "skill-diff",
        help="Diff two learned skill versions",
        description="Compare two versions of the same learned skill.",
    )
    skill_diff_parser.add_argument("--skill-id", required=True)
    skill_diff_parser.add_argument("--left-version", default="")
    skill_diff_parser.add_argument("--right-version", required=True)
    skill_diff_parser.add_argument("--skill-root", default="data/learned_skills")
    _add_output_arg(skill_diff_parser, default="text")
    skill_diff_parser.set_defaults(func=skill_diff)

    skill_preview_parser = sub.add_parser(
        "skill-practice-preview",
        help="Build a checkpoint-safe practice preview for a learned skill",
        description="Preview the safe checkpoint-by-checkpoint practice plan for a learned skill.",
    )
    skill_preview_parser.add_argument("--skill-id", required=True)
    skill_preview_parser.add_argument("--version", default="")
    skill_preview_parser.add_argument("--skill-root", default="data/learned_skills")
    _add_output_arg(skill_preview_parser, default="json")
    skill_preview_parser.set_defaults(func=skill_practice_preview)

    skill_run_parser = sub.add_parser(
        "skill-practice-run",
        help="Run safe checkpointed practice for a learned skill",
        description="Execute the safe practice runtime for a learned skill and store practice history.",
    )
    skill_run_parser.add_argument("--skill-id", required=True)
    skill_run_parser.add_argument("--version", default="")
    skill_run_parser.add_argument("--skill-root", default="data/learned_skills")
    _add_output_arg(skill_run_parser, default="json")
    skill_run_parser.set_defaults(func=skill_practice_run)

    skill_refresh_parser = sub.add_parser(
        "skill-refresh",
        help="Refresh a version-sensitive learned skill",
        description="Re-run Topic Mastery against a saved skill so version-sensitive knowledge stays current.",
    )
    skill_refresh_parser.add_argument("--skill-id", required=True)
    skill_refresh_parser.add_argument("--version", default="")
    skill_refresh_parser.add_argument("--source-url", default="")
    skill_refresh_parser.add_argument("--reason", default="version_sensitive_topic_refresh")
    skill_refresh_parser.add_argument("--workspace", default="data/learn_refresh_cli_run")
    skill_refresh_parser.add_argument("--skill-root", default="data/learned_skills")
    _add_output_arg(skill_refresh_parser, default="json")
    skill_refresh_parser.set_defaults(func=skill_refresh)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
