from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(raw: str) -> bytes:
    padded = raw + "=" * (-len(raw) % 4)
    return base64.urlsafe_b64decode(padded)


@dataclass(slots=True)
class Principal:
    subject: str
    roles: List[str]
    department: str = ""
    clearance: str = ""
    issued_at: int = 0
    expires_at: int = 0

    def to_identity_ctx(self, device_ctx: Dict[str, Any]) -> Dict[str, Any]:
        role = self.roles[0] if self.roles else ""
        return {
            "user": {
                "user_id": self.subject,
                "role": role,
                "department": self.department,
                "clearance": self.clearance,
            },
            "device": device_ctx,
        }


class TokenAuth:
    """
    HMAC-signed bearer token utility for internal control-plane API.
    Intended for private-network service authentication.
    """

    def __init__(self, secret: str, issuer: str = "lam-control-plane") -> None:
        if not secret:
            raise ValueError("TokenAuth requires a non-empty secret")
        self.secret = secret.encode("utf-8")
        self.issuer = issuer

    def issue(self, subject: str, roles: List[str], ttl_seconds: int = 3600, **attrs: Any) -> str:
        now = int(time.time())
        payload = {
            "iss": self.issuer,
            "sub": subject,
            "roles": roles,
            "department": attrs.get("department", ""),
            "clearance": attrs.get("clearance", ""),
            "iat": now,
            "exp": now + int(ttl_seconds),
        }
        header = {"alg": "HS256", "typ": "JWT"}
        header_b64 = _b64url_encode(json.dumps(header, separators=(",", ":"), sort_keys=True).encode("utf-8"))
        payload_b64 = _b64url_encode(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
        signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
        signature = hmac.new(self.secret, signing_input, hashlib.sha256).digest()
        return f"{header_b64}.{payload_b64}.{_b64url_encode(signature)}"

    def verify(self, token: str) -> Principal:
        parts = token.split(".")
        if len(parts) != 3:
            raise PermissionError("Invalid token format")
        header_b64, payload_b64, signature_b64 = parts
        signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
        expected = hmac.new(self.secret, signing_input, hashlib.sha256).digest()
        provided = _b64url_decode(signature_b64)
        if not hmac.compare_digest(expected, provided):
            raise PermissionError("Invalid token signature")

        payload = json.loads(_b64url_decode(payload_b64))
        now = int(time.time())
        if payload.get("iss") != self.issuer:
            raise PermissionError("Invalid token issuer")
        if int(payload.get("exp", 0)) < now:
            raise PermissionError("Token expired")

        return Principal(
            subject=str(payload.get("sub", "")),
            roles=list(payload.get("roles", [])),
            department=str(payload.get("department", "")),
            clearance=str(payload.get("clearance", "")),
            issued_at=int(payload.get("iat", 0)),
            expires_at=int(payload.get("exp", 0)),
        )

