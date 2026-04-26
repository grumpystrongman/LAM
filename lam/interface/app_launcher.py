from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Tuple


APP_CANDIDATES: Dict[str, list[str]] = {
    "chatgpt": [
        "chatgpt.exe",
        r"%LOCALAPPDATA%\Programs\ChatGPT\ChatGPT.exe",
        r"%ProgramFiles%\ChatGPT\ChatGPT.exe",
    ],
    "chrome": [
        "chrome.exe",
        r"%ProgramFiles%\Google\Chrome\Application\chrome.exe",
        r"%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe",
    ],
    "edge": [
        "msedge.exe",
        r"%ProgramFiles%\Microsoft\Edge\Application\msedge.exe",
        r"%ProgramFiles(x86)%\Microsoft\Edge\Application\msedge.exe",
    ],
    "excel": [
        "excel.exe",
        r"%ProgramFiles%\Microsoft Office\root\Office16\EXCEL.EXE",
        r"%ProgramFiles(x86)%\Microsoft Office\root\Office16\EXCEL.EXE",
    ],
    "vscode": [
        "code",
        "code.cmd",
        "code.exe",
        r"%LOCALAPPDATA%\Programs\Microsoft VS Code\Code.exe",
        r"%ProgramFiles%\Microsoft VS Code\Code.exe",
        r"%ProgramFiles(x86)%\Microsoft VS Code\Code.exe",
    ],
}


def normalize_app_name(text: str) -> str:
    lowered = re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()
    aliases = {
        "vs code": "vscode",
        "visual studio code": "vscode",
        "code": "vscode",
    }
    if lowered in aliases:
        return aliases[lowered]
    for known in APP_CANDIDATES:
        if known in lowered:
            return known
    for alias, known in aliases.items():
        if alias in lowered:
            return known
    return lowered.split()[0] if lowered else ""


def open_installed_app(app_name: str) -> Tuple[bool, str]:
    """Open an installed app by alias/path/start-menu id."""
    return open_app_target(app_name)


def open_app_target(app_name: str, launch_args: Optional[List[str]] = None) -> Tuple[bool, str]:
    """Open an installed app by alias/path/start-menu id with optional args."""
    key = normalize_app_name(app_name)
    candidates = APP_CANDIDATES.get(key, [f"{key}.exe", key])
    launch_args = list(launch_args or [])

    for candidate in candidates:
        expanded = os.path.expandvars(candidate)
        exe = shutil.which(expanded)
        if exe:
            subprocess.Popen([exe, *launch_args], shell=False)  # noqa: S603
            return True, exe
        path = Path(expanded)
        if path.exists():
            os.startfile(str(path), arguments=" ".join(launch_args))  # type: ignore[attr-defined]
            return True, str(path)

    app_id = _lookup_startapps_id(app_name)
    if app_id and not launch_args:
        subprocess.Popen(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                f'Start-Process "shell:AppsFolder\\{app_id}"',
            ],
            shell=False,
        )  # noqa: S603
        return True, f"shell:AppsFolder\\{app_id}"

    return False, ""


def _lookup_startapps_id(app_name: str) -> Optional[str]:
    cmd = (
        "$name=$args[0]; "
        "Get-StartApps | Where-Object { $_.Name -like ('*' + $name + '*') } "
        "| Select-Object -First 1 | ConvertTo-Json -Compress"
    )
    try:
        out = subprocess.check_output(  # noqa: S603
            ["powershell", "-NoProfile", "-Command", cmd, "--%", app_name],
            text=True,
            timeout=8,
        )
    except Exception:
        return None
    out = out.strip()
    if not out:
        return None
    try:
        import json

        obj = json.loads(out)
        return obj.get("AppID")
    except Exception:
        return None


def list_installed_apps(query: str = "", limit: int = 40) -> List[Dict[str, str]]:
    cmd = "Get-StartApps | Select-Object Name,AppID | ConvertTo-Json -Compress"
    try:
        out = subprocess.check_output(  # noqa: S603
            ["powershell", "-NoProfile", "-Command", cmd],
            text=True,
            timeout=15,
        )
    except Exception:
        return []
    import json

    try:
        data = json.loads(out.strip() or "[]")
        if isinstance(data, dict):
            data = [data]
    except Exception:
        return []
    q = query.lower().strip()
    rows = []
    for item in data:
        name = str(item.get("Name", ""))
        appid = str(item.get("AppID", ""))
        if q and q not in name.lower() and q not in appid.lower():
            continue
        rows.append({"name": name, "app_id": appid})
        if len(rows) >= limit:
            break
    return rows


def is_app_running(app_name: str) -> Tuple[bool, str]:
    key = normalize_app_name(app_name)
    if not key:
        return False, ""
    probe = key
    if probe.endswith(".exe"):
        probe = probe[:-4]
    cmd = (
        "$name=$args[0]; "
        "Get-Process -Name $name -ErrorAction SilentlyContinue | "
        "Select-Object -First 1 ProcessName,Id | ConvertTo-Json -Compress"
    )
    try:
        out = subprocess.check_output(  # noqa: S603
            ["powershell", "-NoProfile", "-Command", cmd, "--%", probe],
            text=True,
            timeout=8,
        )
    except Exception:
        return False, ""
    out = out.strip()
    if not out:
        return False, ""
    try:
        import json

        obj = json.loads(out)
        pid = str(obj.get("Id", "")).strip()
        proc = str(obj.get("ProcessName", "")).strip()
        return bool(pid), f"{proc}:{pid}" if pid else proc
    except Exception:
        return False, ""
