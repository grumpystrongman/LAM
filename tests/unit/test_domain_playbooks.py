import unittest

from lam.interface.domain_playbooks import (
    build_step_obligations,
    evaluate_step_obligations,
    validate_plan_steps,
    validate_transition_graph,
)


class TestDomainPlaybooks(unittest.TestCase):
    def test_validate_transition_graph_passes_for_email_triage(self) -> None:
        steps = [
            {"kind": "list_recent_messages"},
            {"kind": "read_message"},
            {"kind": "create_draft"},
            {"kind": "save_csv"},
            {"kind": "present"},
        ]
        out = validate_transition_graph("email_triage", steps)
        self.assertTrue(out.get("ok"))

    def test_validate_transition_graph_fails_for_wrong_order(self) -> None:
        steps = [
            {"kind": "list_recent_messages"},
            {"kind": "save_csv"},
            {"kind": "read_message"},
        ]
        out = validate_transition_graph("email_triage", steps)
        self.assertFalse(out.get("ok"))
        self.assertGreaterEqual(len(out.get("errors", [])), 1)

    def test_evaluate_step_obligations_detects_missing_artifact(self) -> None:
        steps = [
            {"kind": "list_recent_messages"},
            {"kind": "read_message"},
            {"kind": "create_draft"},
            {"kind": "save_csv"},
            {"kind": "present"},
        ]
        self.assertTrue(validate_plan_steps("email_triage", steps).get("ok"))
        obligations = build_step_obligations("email_triage", steps)
        out = evaluate_step_obligations(
            domain="email_triage",
            steps=steps,
            obligations=obligations,
            result={
                "results_count": 2,
                "source_status": {"gmail_ui": "ok"},
                "artifacts": {},
                "opened_url": "",
                "mode": "email_triage",
            },
        )
        self.assertFalse(out.get("ok"))
        self.assertGreaterEqual(len(out.get("errors", [])), 1)

    def test_validate_code_workbench_plan(self) -> None:
        steps = [
            {"kind": "research"},
            {"kind": "extract"},
            {"kind": "analyze"},
            {"kind": "produce"},
            {"kind": "present"},
        ]
        self.assertTrue(validate_plan_steps("code_workbench", steps).get("ok"))
        self.assertTrue(validate_transition_graph("code_workbench", steps).get("ok"))


if __name__ == "__main__":
    unittest.main()
