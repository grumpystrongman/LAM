from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional


class ExcelAdapter:
    """
    Deterministic spreadsheet interface.
    Real deployments can bind to COM or openpyxl depending on policy.
    """

    def __init__(self, workbook_path: Optional[str] = None) -> None:
        self.workbook_path = workbook_path
        self._memory_rows: List[Dict[str, Any]] = []
        self._memory_cells: Dict[tuple[str, int, str], Any] = {}

    def set_memory_rows(self, rows: List[Dict[str, Any]]) -> None:
        self._memory_rows = []
        for idx, row in enumerate(rows, start=2):
            enriched = dict(row)
            enriched["_index"] = idx
            self._memory_rows.append(enriched)

    def read_rows(self, sheet: str, start_row: int = 2, end_row: Optional[int] = None) -> List[Dict[str, Any]]:
        if self._memory_rows:
            rows = [row for row in self._memory_rows if row["_index"] >= start_row]
            if end_row is not None:
                rows = [row for row in rows if row["_index"] <= int(end_row)]
            return rows
        return []

    def read_cell(self, sheet: str, row: int, column: str) -> Any:
        key = (sheet, int(row), column)
        if key in self._memory_cells:
            return self._memory_cells[key]
        if self._memory_rows:
            for item in self._memory_rows:
                if item.get("_index") == int(row):
                    # Approximate map A->member_name, B->claim_id, C->status
                    col_map = {"A": "member_name", "B": "claim_id", "C": "status"}
                    return item.get(col_map.get(column, column))
        return None

    def set_cell(self, sheet: str, row: int, column: str, value: Any) -> None:
        key = (sheet, int(row), column)
        self._memory_cells[key] = value

    @staticmethod
    def validate_workbook_path(path: str) -> bool:
        return Path(path).exists()

