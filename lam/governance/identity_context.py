from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List


@dataclass(slots=True)
class UserContext:
    user_id: str
    role: str
    department: str = ""
    job_code: str = ""
    site: str = ""
    clearance: str = ""
    groups: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class DeviceContext:
    device_id: str
    managed: bool = True
    compliant: bool = True
    network_zone: str = "corp"
    hostname: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class IdentityContext:
    user: UserContext
    device: DeviceContext

    def to_dict(self) -> Dict[str, Any]:
        return {"user": self.user.to_dict(), "device": self.device.to_dict()}

