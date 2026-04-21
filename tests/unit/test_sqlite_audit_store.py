import sqlite3
from pathlib import Path
import uuid
import unittest

from lam.governance.audit_logger import AuditLogger
from lam.governance.redaction import Redactor
from lam.services.audit_store import SqliteAuditSink


class TestSqliteAuditStore(unittest.TestCase):
    def setUp(self) -> None:
        root = Path("test_artifacts")
        root.mkdir(parents=True, exist_ok=True)
        self.db_path = root / f"audit_test_{uuid.uuid4().hex}.db"
        self.sink = SqliteAuditSink(path=self.db_path)

    def tearDown(self) -> None:
        # Windows may hold file handles briefly; cleanup is handled out-of-band.
        pass

    def test_append_only_and_chain(self) -> None:
        logger = AuditLogger(sink=self.sink, redactor=Redactor())
        logger.append_event("event_one", {"foo": "bar"})
        logger.append_event("event_two", {"phi": "123-45-6789"})

        errors = logger.validate_chain()
        self.assertEqual(errors, [])

        with sqlite3.connect(self.db_path) as conn:
            with self.assertRaises(sqlite3.DatabaseError):
                conn.execute("UPDATE audit_events SET event_json = '{}' WHERE seq = 1")
                conn.commit()


if __name__ == "__main__":
    unittest.main()
