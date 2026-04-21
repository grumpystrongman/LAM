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
}


def normalize_app_name(text: str) -> str:
    lowered = re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()
    for known in APP_CANDIDATES:
        if known in lowered:
            return known
    return lowered.split()[0] if lowered else ""


def open_installed_app(app_name: str) -> Tuple[bool, str]:
    """Open an installed app by alias/path/start-menu id."""
    key = normalize_app_name(app_name)
    candidates = APP_CANDIDATES.get(key, [f"{key}.exe", key])

    for candidate in candidates:
        expanded = os.path.expandvars(candidate)
        exe = shutil.which(expanded)
        if exe:
            subprocess.Popen([exe], shell=False)  # noqa: S603
            return True, exe
        path = Path(expanded)
        if path.exists():
            os.startfile(str(path))  # type: ignore[attr-defined]
            return True, str(path)

    app_id = _lookup_startapps_id(app_name)
    if app_id:
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
