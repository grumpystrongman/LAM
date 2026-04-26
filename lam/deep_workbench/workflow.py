from __future__ import annotations

import json
import re
import textwrap
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from lam.interface.app_launcher import open_app_target


@dataclass
class WorkbenchContract:
    instruction: str
    title: str
    slug: str
    workspace_root: str
    requested_outputs: List[str]
    wants_vscode: bool
    wants_code: bool
    wants_analysis: bool
    audience: str


def extract_workbench_contract(
    instruction: str,
    workspace_root: str = "data/deep_work_runs",
) -> WorkbenchContract:
    normalized = re.sub(r"\s+", " ", str(instruction or "")).strip()
    title = _derive_title(normalized)
    slug = _slugify_title(title)
    low = normalized.lower()
    requested_outputs: List[str] = []
    if any(token in low for token in ["code", "script", "python", "analysis"]):
        requested_outputs.append("code")
    if any(token in low for token in ["report", "summary", "brief", "notes"]):
        requested_outputs.append("document")
    if any(token in low for token in ["spreadsheet", "csv", "xlsx", "workbook"]):
        requested_outputs.append("spreadsheet")
    if "dashboard" in low:
        requested_outputs.append("dashboard")
    if "vscode" in low or "vs code" in low or "visual studio code" in low:
        requested_outputs.append("workspace")
        requested_outputs.append("vscode_workspace")
    if not requested_outputs:
        requested_outputs.extend(["code", "document"])
    return WorkbenchContract(
        instruction=normalized,
        title=title,
        slug=slug,
        workspace_root=workspace_root,
        requested_outputs=list(dict.fromkeys(requested_outputs)),
        wants_vscode=any(token in low for token in ["vscode", "vs code", "visual studio code", "code instance"]),
        wants_code=any(token in low for token in ["write code", "build code", "script", "python", "analysis", "research"]),
        wants_analysis=any(token in low for token in ["analysis", "analy", "research", "study", "compare"]),
        audience="operator",
    )


def build_workspace(
    contract: WorkbenchContract,
    *,
    open_vscode: bool = True,
) -> Dict[str, Any]:
    root = Path(contract.workspace_root)
    root.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    workspace = root / f"{contract.slug}_{timestamp}"
    src_dir = workspace / "src"
    tests_dir = workspace / "tests"
    notes_dir = workspace / "notes"
    artifacts_dir = workspace / "artifacts"
    vscode_dir = workspace / ".vscode"
    for folder in [workspace, src_dir, tests_dir, notes_dir, artifacts_dir, vscode_dir]:
        folder.mkdir(parents=True, exist_ok=True)

    task_contract_path = workspace / "task_contract.json"
    task_brief_path = notes_dir / "task_brief.md"
    analysis_plan_path = notes_dir / "analysis_plan.md"
    analysis_script_path = src_dir / "analysis.py"
    smoke_test_path = tests_dir / "test_smoke.py"
    readme_path = workspace / "README.md"
    tasks_json_path = vscode_dir / "tasks.json"
    settings_json_path = vscode_dir / "settings.json"
    smoke_log_path = artifacts_dir / "smoke_test.log"

    task_contract_path.write_text(json.dumps(asdict(contract), indent=2), encoding="utf-8")
    task_brief_path.write_text(_render_task_brief(contract), encoding="utf-8")
    analysis_plan_path.write_text(_render_analysis_plan(contract), encoding="utf-8")
    analysis_script_path.write_text(_render_analysis_script(contract), encoding="utf-8")
    smoke_test_path.write_text(_render_smoke_test(), encoding="utf-8")
    readme_path.write_text(_render_readme(contract), encoding="utf-8")
    tasks_json_path.write_text(_render_vscode_tasks(), encoding="utf-8")
    settings_json_path.write_text(_render_vscode_settings(), encoding="utf-8")

    smoke_log_path.write_text(
        "\n".join(
            [
                f"generated_at={datetime.now().isoformat(timespec='seconds')}",
                f"title={contract.title}",
                "smoke_check=python -m py_compile src/analysis.py tests/test_smoke.py",
                "status=pending",
            ]
        ),
        encoding="utf-8",
    )

    vscode_launch = {"ok": False, "launched": "", "mode": "not_requested"}
    if open_vscode and contract.wants_vscode:
        ok, launched = open_app_target("vscode", ["-n", str(workspace.resolve())])
        vscode_launch = {
            "ok": ok,
            "launched": launched,
            "mode": "new_window" if ok else "not_found",
        }

    artifact_paths = {
        "workspace_directory": str(workspace.resolve()),
        "task_contract_json": str(task_contract_path.resolve()),
        "task_brief_md": str(task_brief_path.resolve()),
        "analysis_plan_md": str(analysis_plan_path.resolve()),
        "analysis_script_py": str(analysis_script_path.resolve()),
        "smoke_test_py": str(smoke_test_path.resolve()),
        "workspace_readme_md": str(readme_path.resolve()),
        "smoke_log": str(smoke_log_path.resolve()),
        "primary_open_file": str(readme_path.resolve()),
    }
    return {
        "workspace": str(workspace.resolve()),
        "artifact_paths": artifact_paths,
        "vscode_launch": vscode_launch,
        "generation_timestamp": datetime.now().isoformat(timespec="seconds"),
        "current_task_contract": asdict(contract),
    }


def _derive_title(instruction: str) -> str:
    if not instruction:
        return "Deep Work Task"
    cleaned = instruction.strip().rstrip(".")
    pieces = re.split(r"[.!?]", cleaned)
    first = (pieces[0] or cleaned).strip()
    first = re.sub(r"^(please|now|i want|lam should|could you)\s+", "", first, flags=re.IGNORECASE).strip()
    return first[:96] or "Deep Work Task"


def _slugify_title(title: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", title.lower()).strip("_")
    return slug[:48] or "deep_work"


def _render_task_brief(contract: WorkbenchContract) -> str:
    outputs = "\n".join(f"- {item}" for item in contract.requested_outputs)
    return textwrap.dedent(
        f"""\
        # Task Brief

        ## Objective
        {contract.instruction}

        ## Requested outputs
        {outputs}

        ## Operating stance
        - Build inside a fresh workspace.
        - Write runnable analysis code before expanding scope.
        - Leave a paper trail in `notes/` and `artifacts/`.
        - Prefer small, testable increments.
        """
    ).strip() + "\n"


def _render_analysis_plan(contract: WorkbenchContract) -> str:
    return textwrap.dedent(
        f"""\
        # Analysis Plan

        1. Restate the task and constraints from `task_contract.json`.
        2. Collect or place source files under `artifacts/` or a dedicated input folder.
        3. Extend `src/analysis.py` with task-specific data loading and analysis logic.
        4. Add focused tests under `tests/` for the logic that matters.
        5. Run smoke checks, then capture output in `artifacts/`.

        ## Notes
        - Workspace title: {contract.title}
        - VS Code requested: {"yes" if contract.wants_vscode else "no"}
        - Analysis requested: {"yes" if contract.wants_analysis else "no"}
        """
    ).strip() + "\n"


def _render_analysis_script(contract: WorkbenchContract) -> str:
    return textwrap.dedent(
        f"""\
        from __future__ import annotations

        import argparse
        import json
        from pathlib import Path


        def run_analysis(input_path: str = "", output_path: str = "") -> dict:
            base = Path(__file__).resolve().parent.parent
            output_dir = Path(output_path) if output_path else base / "artifacts"
            output_dir.mkdir(parents=True, exist_ok=True)
            payload = {{
                "task_title": {contract.title!r},
                "instruction": {contract.instruction!r},
                "input_path": input_path,
                "status": "scaffold_ready",
                "next_step": "Replace run_analysis() with task-specific logic.",
            }}
            report_path = output_dir / "analysis_summary.json"
            report_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            return payload


        def main() -> None:
            parser = argparse.ArgumentParser(description="Deep work analysis scaffold")
            parser.add_argument("--input", default="", help="Optional input file or directory")
            parser.add_argument("--output", default="", help="Optional output directory")
            args = parser.parse_args()
            result = run_analysis(input_path=args.input, output_path=args.output)
            print(json.dumps(result, indent=2))


        if __name__ == "__main__":
            main()
        """
    )


def _render_smoke_test() -> str:
    return textwrap.dedent(
        """\
        from __future__ import annotations

        from src.analysis import run_analysis


        def test_run_analysis_creates_summary(tmp_path) -> None:
            result = run_analysis(output_path=str(tmp_path))
            assert result["status"] == "scaffold_ready"
            assert (tmp_path / "analysis_summary.json").exists()
        """
    )


def _render_readme(contract: WorkbenchContract) -> str:
    return textwrap.dedent(
        f"""\
        # {contract.title}

        This workspace was generated by OpenLAMb's code workbench flow.

        ## Run

        ```powershell
        python src/analysis.py
        python -m pytest -q
        ```

        ## Files
        - `task_contract.json`: machine-readable task contract
        - `notes/task_brief.md`: user-facing brief
        - `notes/analysis_plan.md`: execution checklist
        - `src/analysis.py`: analysis scaffold
        - `tests/test_smoke.py`: smoke coverage
        """
    ).strip() + "\n"


def _render_vscode_tasks() -> str:
    return json.dumps(
        {
            "version": "2.0.0",
            "tasks": [
                {
                    "label": "Run analysis",
                    "type": "shell",
                    "command": "python src/analysis.py",
                    "problemMatcher": [],
                },
                {
                    "label": "Run smoke tests",
                    "type": "shell",
                    "command": "python -m pytest -q",
                    "problemMatcher": [],
                },
            ],
        },
        indent=2,
    )


def _render_vscode_settings() -> str:
    return json.dumps(
        {
            "python.testing.pytestEnabled": True,
            "python.testing.unittestEnabled": False,
            "python.defaultInterpreterPath": "python",
            "files.exclude": {
                "**/__pycache__": True,
            },
        },
        indent=2,
    )
