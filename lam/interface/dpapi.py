from __future__ import annotations

import base64
import ctypes
from ctypes import wintypes


_CRYPTPROTECT_UI_FORBIDDEN = 0x01


class _DATA_BLOB(ctypes.Structure):
    _fields_ = [
        ("cbData", wintypes.DWORD),
        ("pbData", ctypes.POINTER(ctypes.c_byte)),
    ]


def _to_blob(data: bytes) -> _DATA_BLOB:
    buffer = (ctypes.c_byte * len(data)).from_buffer_copy(data)
    return _DATA_BLOB(len(data), ctypes.cast(buffer, ctypes.POINTER(ctypes.c_byte)))


def _from_blob(blob: _DATA_BLOB) -> bytes:
    if not blob.cbData:
        return b""
    data = ctypes.string_at(blob.pbData, blob.cbData)
    ctypes.windll.kernel32.LocalFree(blob.pbData)
    return data


def dpapi_available() -> bool:
    try:
        _ = ctypes.windll.crypt32.CryptProtectData
        return True
    except Exception:
        return False


def protect_bytes(value: bytes, description: str = "LAMLocalSecret") -> bytes:
    if not value:
        return b""
    if not dpapi_available():
        return value
    in_blob = _to_blob(value)
    out_blob = _DATA_BLOB()
    ok = ctypes.windll.crypt32.CryptProtectData(
        ctypes.byref(in_blob),
        ctypes.c_wchar_p(description),
        None,
        None,
        None,
        _CRYPTPROTECT_UI_FORBIDDEN,
        ctypes.byref(out_blob),
    )
    if not ok:
        raise OSError("DPAPI protect failed")
    return _from_blob(out_blob)


def unprotect_bytes(value: bytes) -> bytes:
    if not value:
        return b""
    if not dpapi_available():
        return value
    in_blob = _to_blob(value)
    out_blob = _DATA_BLOB()
    desc = ctypes.c_wchar_p()
    ok = ctypes.windll.crypt32.CryptUnprotectData(
        ctypes.byref(in_blob),
        ctypes.byref(desc),
        None,
        None,
        None,
        _CRYPTPROTECT_UI_FORBIDDEN,
        ctypes.byref(out_blob),
    )
    if not ok:
        raise OSError("DPAPI unprotect failed")
    return _from_blob(out_blob)


def protect_text(value: str) -> str:
    raw = value.encode("utf-8")
    return base64.b64encode(protect_bytes(raw)).decode("ascii")


def unprotect_text(value: str) -> str:
    if not value:
        return ""
    raw = base64.b64decode(value.encode("ascii"))
    return unprotect_bytes(raw).decode("utf-8", errors="ignore")
