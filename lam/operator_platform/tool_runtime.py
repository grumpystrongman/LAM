from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List


@dataclass(slots=True)
class ToolRuntime:
    browser: bool = True
    desktop: bool = True
    shell: bool = True
    filesystem: bool = True
    spreadsheet: bool = True
    vector_store: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def available_tool_families(self) -> List[str]:
        return [name for name, enabled in self.to_dict().items() if enabled]
