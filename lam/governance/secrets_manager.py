from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from lam.interface.password_vault import LocalPasswordVault


@dataclass(slots=True)
class SecretRef:
    name: str
    provider: str = "windows_credential_manager"


class SecretsManager:
    """
    Stub integration point for Windows Credential Manager / DPAPI or enterprise vault.
    No plaintext credential storage is allowed.
    """

    def get_secret(self, ref: SecretRef) -> Optional[str]:
        # Local-only credential vault backed by DPAPI-protected SQLite.
        vault = LocalPasswordVault()
        found = vault.find_entry_by_service(ref.name)
        if not found.get("ok"):
            return None
        entry = found.get("entry", {})
        return str(entry.get("password", "")) or None
