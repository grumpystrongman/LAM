from __future__ import annotations

import base64
import logging
import time
from pathlib import Path


LOGGER = logging.getLogger(__name__)
CLIPBOARD_ASSETS_ROOT = Path(__file__).resolve().parents[2] / "data" / "reports" / "study_assets"


def image_to_base64(file_path: str | Path) -> str:
    path = Path(file_path)
    if not path.exists() or not path.is_file():
        return ""
    try:
        return base64.b64encode(path.read_bytes()).decode("ascii")
    except Exception as exc:  # pylint: disable=broad-exception-caught
        LOGGER.warning("Failed to encode image as base64 for %s: %s", path, exc)
        return ""


def base64_to_image(b64_string: str, output_path: str | Path) -> Path:
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = base64.b64decode(str(b64_string or "").encode("ascii"))
    target.write_bytes(payload)
    return target


def capture_clipboard_image(output_path: str | Path | None = None) -> str:
    try:
        from PIL import ImageGrab  # type: ignore
    except Exception as exc:  # pylint: disable=broad-exception-caught
        LOGGER.warning("Clipboard image capture is unavailable because PIL.ImageGrab could not be imported: %s", exc)
        return ""
    try:
        clip = ImageGrab.grabclipboard()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        LOGGER.warning("Clipboard image capture failed: %s", exc)
        return ""
    if clip is None:
        return ""
    if hasattr(clip, "save"):
        target = Path(output_path) if output_path else (CLIPBOARD_ASSETS_ROOT / "clipboard" / f"clipboard_{int(time.time())}.png")
        target.parent.mkdir(parents=True, exist_ok=True)
        clip.save(str(target), format="PNG")
        return str(target.resolve())
    if isinstance(clip, list):
        for item in clip:
            path = Path(str(item))
            if path.exists() and path.is_file():
                return str(path.resolve())
    return ""
