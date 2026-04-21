import unittest
from unittest.mock import patch

from lam.interface.operator_contract import attach_operator_contract


class TestOperatorContractSchemaGate(unittest.TestCase):
    def test_contract_schema_validation_passes(self) -> None:
        result = {
            "ok": True,
            "mode": "autonomous_plan_execute",
            "query": "Epic Systems competitors",
            "artifacts": {"report_md": "C:\\temp\\report.md"},
            "results": [{"title": "Oracle Health", "url": "https://example.com"}],
            "canvas": {"title": "Search Summary"},
        }
        plan_steps = [{"action": "research", "target": "Epic Systems competitors"}]
        out = attach_operator_contract(
            instruction="Research top competitors to Epic Systems and build a report.",
            result=result,
            plan_steps=plan_steps,
        )
        self.assertIn("schema_validation", out)
        self.assertTrue(out["schema_validation"]["passed"])

    @patch("lam.interface.operator_contract.validate_contract_objects")
    def test_contract_schema_validation_failure_blocks_run(self, mock_validate) -> None:
        mock_validate.return_value = (False, ["plan_contract:<root>: simulated schema violation"])
        result = {
            "ok": True,
            "mode": "autonomous_plan_execute",
            "query": "Epic Systems competitors",
            "artifacts": {"report_md": "C:\\temp\\report.md"},
            "results": [{"title": "Oracle Health", "url": "https://example.com"}],
            "canvas": {"title": "Search Summary"},
        }
        plan_steps = [{"action": "research", "target": "Epic Systems competitors"}]
        out = attach_operator_contract(
            instruction="Research top competitors to Epic Systems and build a report.",
            result=result,
            plan_steps=plan_steps,
        )
        self.assertFalse(out["ok"])
        self.assertFalse(out["schema_validation"]["passed"])
        self.assertIn("schema_validation_errors", out)
        self.assertEqual(out["final_report"]["status"], "failed")


if __name__ == "__main__":
    unittest.main()
