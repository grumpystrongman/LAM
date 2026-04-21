import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from lam.interface import web_ui


class TestWebUiHistory(unittest.TestCase):
    def test_load_save_history_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            temp_path = Path(td) / "history.json"
            with patch("lam.interface.web_ui._history_path", return_value=temp_path):
                sample = [{"ok": True, "instruction": "x"}]
                web_ui._save_history(sample)
                loaded = web_ui._load_history()
                self.assertEqual(loaded, sample)
                raw = json.loads(temp_path.read_text(encoding="utf-8"))
                self.assertEqual(raw, sample)


if __name__ == "__main__":
    unittest.main()
