from __future__ import annotations

import importlib
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List

import yaml


def run_reliability_suite(
    include_pytest: bool = False,
    pytest_args: List[str] | None = None,
    project_root: str | Path = ".",
    pytest_timeout_seconds: int = 300,
    include_desktop_smoke: bool = False,
    desktop_smoke_runner: Callable[[], Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    """Run a lightweight reliability regression suite and return structured JSON."""
    started_at = time.time()
    checks: List[Dict[str, Any]] = []
    root = Path(project_root).resolve()

    checks.append(_check_python_version())
    checks.append(_check_required_files(root))
    checks.append(_check_yaml_configs(root))
    checks.append(_check_imports())
    checks.append(_check_desktop_smoke(include_desktop_smoke=include_desktop_smoke, runner=desktop_smoke_runner))

    pytest_result = _pytest_not_requested()
    if include_pytest:
        pytest_result = _run_pytest(pytest_args=pytest_args or [], cwd=root, timeout_seconds=pytest_timeout_seconds)

    summary = _build_summary(checks, pytest_result)
    finished_at = time.time()
    ok = summary["failed"] == 0

    decision_log = [
        f"check {item['status']}: {item['name']} ({item.get('duration_ms', 0)} ms)"
        for item in checks
    ]
    if pytest_result.get("requested"):
        decision_log.append(
            f"pytest {'passed' if pytest_result.get('ok') else 'failed'}"
            f" (exit={pytest_result.get('exit_code', -1)}, duration={pytest_result.get('duration_ms', 0)} ms)"
        )

    return {
        "ok": ok,
        "mode": "reliability_suite",
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_ms": int((finished_at - started_at) * 1000),
        "summary": summary,
        "checks": checks,
        "pytest": pytest_result,
        "decision_log": decision_log,
        "canvas": {
            "title": "Reliability Suite Passed" if ok else "Reliability Suite Found Regressions",
            "subtitle": f"{summary['passed']}/{summary['total']} checks passed",
            "cards": [
                {"title": "Passed", "price": str(summary["passed"]), "source": "suite"},
                {"title": "Failed", "price": str(summary["failed"]), "source": "suite"},
                {"title": "Skipped", "price": str(summary["skipped"]), "source": "suite"},
            ],
        },
    }


def _check_python_version() -> Dict[str, Any]:
    started = time.time()
    major, minor = sys.version_info[0], sys.version_info[1]
    ok = (major, minor) >= (3, 10)
    return {
        "name": "python_version",
        "status": "pass" if ok else "fail",
        "ok": ok,
        "details": f"Detected Python {major}.{minor}",
        "duration_ms": int((time.time() - started) * 1000),
    }


def _check_required_files(root: Path) -> Dict[str, Any]:
    started = time.time()
    required = [
        root / "lam" / "interface" / "web_ui.py",
        root / "config" / "policy.yaml",
        root / "tests" / "unit",
    ]
    missing = [str(path.relative_to(root)) for path in required if not path.exists()]
    ok = len(missing) == 0
    return {
        "name": "required_files",
        "status": "pass" if ok else "fail",
        "ok": ok,
        "details": "All required paths exist" if ok else f"Missing: {', '.join(missing)}",
        "duration_ms": int((time.time() - started) * 1000),
    }


def _check_yaml_configs(root: Path) -> Dict[str, Any]:
    started = time.time()
    configs = [
        root / "config" / "policy.yaml",
        root / "config" / "control_plane.yaml",
    ]
    bad: List[str] = []
    for path in configs:
        try:
            yaml.safe_load(path.read_text(encoding="utf-8"))
        except Exception as exc:  # pylint: disable=broad-exception-caught
            bad.append(f"{path.relative_to(root)} ({exc})")
    ok = len(bad) == 0
    return {
        "name": "yaml_parse",
        "status": "pass" if ok else "fail",
        "ok": ok,
        "details": "YAML config parse succeeded" if ok else "; ".join(bad),
        "duration_ms": int((time.time() - started) * 1000),
    }


def _check_imports() -> Dict[str, Any]:
    started = time.time()
    modules = [
        "lam.interface.search_agent",
        "lam.interface.scheduler",
        "lam.interface.password_vault",
    ]
    failed: List[str] = []
    for module in modules:
        try:
            importlib.import_module(module)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            failed.append(f"{module}: {exc}")
    ok = len(failed) == 0
    return {
        "name": "import_smoke",
        "status": "pass" if ok else "fail",
        "ok": ok,
        "details": "Core modules imported" if ok else "; ".join(failed),
        "duration_ms": int((time.time() - started) * 1000),
    }


def _pytest_not_requested() -> Dict[str, Any]:
    return {
        "requested": False,
        "ran": False,
        "ok": True,
        "status": "skipped",
        "exit_code": 0,
        "args": [],
        "duration_ms": 0,
        "output_tail": [],
    }


def _run_pytest(pytest_args: List[str], cwd: Path, timeout_seconds: int) -> Dict[str, Any]:
    started = time.time()
    cmd = [sys.executable, "-m", "pytest", *pytest_args]
    try:
        proc = subprocess.run(  # noqa: S603
            cmd,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=max(1, int(timeout_seconds)),
            check=False,
        )
        output = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
        lines = [line for line in output.splitlines() if line.strip()]
        ok = proc.returncode == 0
        return {
            "requested": True,
            "ran": True,
            "ok": ok,
            "status": "pass" if ok else "fail",
            "exit_code": proc.returncode,
            "args": pytest_args,
            "duration_ms": int((time.time() - started) * 1000),
            "output_tail": lines[-40:],
        }
    except subprocess.TimeoutExpired as exc:
        combined = ""
        if exc.stdout:
            combined += exc.stdout
        if exc.stderr:
            combined += "\n" + exc.stderr
        lines = [line for line in combined.splitlines() if line.strip()]
        return {
            "requested": True,
            "ran": True,
            "ok": False,
            "status": "fail",
            "exit_code": -1,
            "args": pytest_args,
            "duration_ms": int((time.time() - started) * 1000),
            "error": f"pytest timed out after {timeout_seconds} seconds",
            "output_tail": lines[-40:],
        }


def _check_desktop_smoke(
    include_desktop_smoke: bool,
    runner: Callable[[], Dict[str, Any]] | None,
) -> Dict[str, Any]:
    started = time.time()
    if not include_desktop_smoke:
        return {
            "name": "desktop_notepad_hello_world",
            "status": "skipped",
            "ok": True,
            "details": "Desktop smoke disabled.",
            "duration_ms": int((time.time() - started) * 1000),
        }
    if runner is None:
        return {
            "name": "desktop_notepad_hello_world",
            "status": "fail",
            "ok": False,
            "details": "Desktop smoke runner unavailable.",
            "duration_ms": int((time.time() - started) * 1000),
        }
    try:
        result = runner()
        ok = bool(result.get("ok", False))
        return {
            "name": "desktop_notepad_hello_world",
            "status": "pass" if ok else "fail",
            "ok": ok,
            "details": str(result.get("message", result.get("error", "desktop smoke executed"))),
            "duration_ms": int((time.time() - started) * 1000),
        }
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return {
            "name": "desktop_notepad_hello_world",
            "status": "fail",
            "ok": False,
            "details": str(exc),
            "duration_ms": int((time.time() - started) * 1000),
        }


def _build_summary(checks: List[Dict[str, Any]], pytest_result: Dict[str, Any]) -> Dict[str, int]:
    passed = len([item for item in checks if item.get("status") == "pass"])
    failed = len([item for item in checks if item.get("status") == "fail"])
    skipped = len([item for item in checks if item.get("status") == "skipped"])

    if pytest_result.get("requested"):
        if pytest_result.get("status") == "pass":
            passed += 1
        elif pytest_result.get("status") == "fail":
            failed += 1
        else:
            skipped += 1

    total = passed + failed + skipped
    return {
        "total": total,
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
    }
