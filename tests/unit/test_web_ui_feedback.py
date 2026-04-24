import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from lam.interface import web_ui


class TestWebUiFeedback(unittest.TestCase):
    def test_append_feedback_writes_jsonl_row(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "feedback.jsonl"
            with patch("lam.interface.web_ui._feedback_path", return_value=path):
                web_ui._append_feedback(
                    {
                        "session_id": "s1",
                        "task_id": "t1",
                        "message_id": "m1",
                        "rating": 1,
                        "reason": "great_result",
                        "comment": "nice",
                        "timestamp": 1.0,
                    }
                )
            raw = path.read_text(encoding="utf-8").strip()
            payload = json.loads(raw)
            self.assertEqual(payload.get("reason"), "great_result")
            self.assertEqual(payload.get("rating"), 1)


if __name__ == "__main__":
    unittest.main()
