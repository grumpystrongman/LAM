import unittest

from lam.governance.redaction import Redactor


class TestRedaction(unittest.TestCase):
    def test_redacts_sensitive_text(self) -> None:
        redactor = Redactor()
        payload = {
            "notes": "Patient SSN 123-45-6789 and email jane.doe@example.org",
            "nested": {"phone": "Call 212-555-8899"},
        }
        clean, meta = redactor.redact_for_persistence(payload)
        self.assertNotIn("123-45-6789", str(clean))
        self.assertNotIn("jane.doe@example.org", str(clean))
        self.assertIn("<REDACTED:SSN>", str(clean))
        self.assertGreaterEqual(meta["confidence"], 0.99)


if __name__ == "__main__":
    unittest.main()

