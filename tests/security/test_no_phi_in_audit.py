import re
import unittest
from pathlib import Path

from lam.governance.audit_logger import AuditLogger, JsonlAuditSink
from lam.governance.redaction import Redactor


class TestAuditSecurity(unittest.TestCase):
    def test_no_raw_phi_persisted(self) -> None:
        artifacts = Path("test_artifacts")
        artifacts.mkdir(parents=True, exist_ok=True)
        audit_path = artifacts / "security_audit.jsonl"
        if audit_path.exists():
            audit_path.unlink()
        sink = JsonlAuditSink(audit_path)
        logger = AuditLogger(sink=sink, redactor=Redactor())
        logger.append_event(
            "policy_decision",
            {
                "patient_note": "Member SSN 123-45-6789, email john@example.org, phone 212-555-9999",
                "decision": "allow",
            },
        )
        raw = audit_path.read_text(encoding="utf-8")
        self.assertIsNone(re.search(r"\b123-45-6789\b", raw))
        self.assertIsNone(re.search(r"john@example.org", raw))
        self.assertIsNone(re.search(r"212-555-9999", raw))
        self.assertIn("<REDACTED:SSN>", raw)
        audit_path.unlink()


if __name__ == "__main__":
    unittest.main()
