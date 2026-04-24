from __future__ import annotations

import json
import subprocess
import time
import urllib.error
import urllib.request
from typing import Any, Dict


DEFAULT_DOCKER_IMAGE = "mcr.microsoft.com/playwright/python:v1.53.0-jammy"
DEFAULT_DOCKER_CONTAINER = "lam-browser-worker"
DEFAULT_HOST_CDP_PORT = 9223
DEFAULT_CONTAINER_CDP_PORT = 9222
DEFAULT_DOCKER_BOOT_TIMEOUT_SECONDS = 20


def normalize_browser_worker_mode(mode: str) -> str:
    value = str(mode or "").strip().lower()
    if value not in {"local", "docker"}:
        return "local"
    return value


def ensure_browser_worker(
    *,
    mode: str,
    host_cdp_port: int = DEFAULT_HOST_CDP_PORT,
    container_cdp_port: int = DEFAULT_CONTAINER_CDP_PORT,
    container_name: str = DEFAULT_DOCKER_CONTAINER,
    image: str = DEFAULT_DOCKER_IMAGE,
    boot_timeout_seconds: int = DEFAULT_DOCKER_BOOT_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    worker_mode = normalize_browser_worker_mode(mode)
    if worker_mode == "local":
        return {"ok": True, "mode": "local", "debug_port": 9222, "status": "local_session"}
    try:
        host_port = max(1024, int(host_cdp_port))
        container_port = max(1024, int(container_cdp_port))
        timeout_seconds = max(5, int(boot_timeout_seconds))
    except Exception:
        host_port = DEFAULT_HOST_CDP_PORT
        container_port = DEFAULT_CONTAINER_CDP_PORT
        timeout_seconds = DEFAULT_DOCKER_BOOT_TIMEOUT_SECONDS

    available = _docker_available()
    if not available.get("ok"):
        return {
            "ok": False,
            "mode": "docker",
            "error": "docker_unavailable",
            "detail": available.get("detail", "docker command unavailable"),
            "debug_port": host_port,
        }

    running = _docker_container_running(container_name=container_name)
    if not running:
        started = _start_docker_browser_worker(
            image=image,
            container_name=container_name,
            host_cdp_port=host_port,
            container_cdp_port=container_port,
        )
        if not started.get("ok"):
            return {
                "ok": False,
                "mode": "docker",
                "error": "docker_worker_start_failed",
                "detail": started.get("detail", ""),
                "debug_port": host_port,
                "container_name": container_name,
                "image": image,
            }

    ready = _wait_for_cdp(host_port=host_port, timeout_seconds=timeout_seconds)
    if not ready.get("ok"):
        return {
            "ok": False,
            "mode": "docker",
            "error": "docker_worker_not_ready",
            "detail": ready.get("detail", ""),
            "debug_port": host_port,
            "container_name": container_name,
            "image": image,
        }

    return {
        "ok": True,
        "mode": "docker",
        "status": "ready",
        "debug_port": host_port,
        "container_name": container_name,
        "image": image,
        "cdp_version": ready.get("version", ""),
        "cdp_websocket": ready.get("websocket", ""),
    }


def _docker_available() -> Dict[str, Any]:
    try:
        out = subprocess.run(
            ["docker", "version", "--format", "{{.Server.Version}}"],
            capture_output=True,
            text=True,
            timeout=6,
            check=False,
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return {"ok": False, "detail": str(exc)}
    if out.returncode != 0:
        detail = (out.stderr or out.stdout or "").strip()
        return {"ok": False, "detail": detail or "docker version failed"}
    return {"ok": True, "detail": (out.stdout or "").strip()}


def _docker_container_running(container_name: str) -> bool:
    try:
        out = subprocess.run(
            ["docker", "ps", "--filter", f"name=^{container_name}$", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        if out.returncode != 0:
            return False
        names = [x.strip() for x in (out.stdout or "").splitlines() if x.strip()]
        return container_name in names
    except Exception:
        return False


def _start_docker_browser_worker(
    *,
    image: str,
    container_name: str,
    host_cdp_port: int,
    container_cdp_port: int,
) -> Dict[str, Any]:
    _ = _remove_docker_container(container_name)
    launch_script = (
        "set -e; "
        "for b in chromium chromium-browser google-chrome-stable google-chrome; do "
        "if command -v \"$b\" >/dev/null 2>&1; then "
        f"exec \"$b\" --remote-debugging-address=0.0.0.0 --remote-debugging-port={int(container_cdp_port)} "
        "--user-data-dir=/tmp/chrome-data --no-sandbox --disable-dev-shm-usage about:blank; "
        "fi; "
        "done; "
        "echo 'No chromium/chrome binary found in container'; "
        "sleep 3600"
    )
    cmd = [
        "docker",
        "run",
        "-d",
        "--rm",
        "--name",
        container_name,
        "-p",
        f"{int(host_cdp_port)}:{int(container_cdp_port)}",
        image,
        "sh",
        "-lc",
        launch_script,
    ]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=20, check=False)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return {"ok": False, "detail": str(exc)}
    if out.returncode != 0:
        detail = (out.stderr or out.stdout or "").strip()
        return {"ok": False, "detail": detail or "docker run failed"}
    return {"ok": True, "container_id": (out.stdout or "").strip()}


def _remove_docker_container(container_name: str) -> Dict[str, Any]:
    try:
        out = subprocess.run(
            ["docker", "rm", "-f", container_name],
            capture_output=True,
            text=True,
            timeout=8,
            check=False,
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return {"ok": False, "detail": str(exc)}
    if out.returncode != 0 and "No such container" not in str(out.stderr or ""):
        return {"ok": False, "detail": (out.stderr or out.stdout or "").strip()}
    return {"ok": True}


def _wait_for_cdp(host_port: int, timeout_seconds: int) -> Dict[str, Any]:
    deadline = time.time() + float(max(5, timeout_seconds))
    url = f"http://127.0.0.1:{int(host_port)}/json/version"
    last_error = ""
    while time.time() < deadline:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "LAM/1.0"})
            with urllib.request.urlopen(req, timeout=2) as resp:  # nosec B310 - localhost only
                raw = resp.read().decode("utf-8", errors="ignore")
            payload = json.loads(raw) if raw else {}
            return {
                "ok": True,
                "version": str(payload.get("Browser", "")),
                "websocket": str(payload.get("webSocketDebuggerUrl", "")),
            }
        except (urllib.error.URLError, ValueError, TimeoutError) as exc:
            last_error = str(exc)
            time.sleep(0.4)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            last_error = str(exc)
            time.sleep(0.4)
    return {"ok": False, "detail": last_error or "cdp endpoint unavailable"}

