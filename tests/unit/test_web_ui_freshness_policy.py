import unittest
from unittest.mock import patch

from lam.interface import web_ui


class TestWebUiFreshnessPolicy(unittest.TestCase):
    def test_infer_instruction_domain_detects_code_workbench(self) -> None:
        domain = web_ui._infer_instruction_domain(
            "Create a new VS Code workspace, write analysis code, and leave me a runnable scaffold."
        )
        self.assertEqual(domain, "code_workbench")

    def test_infer_instruction_domain_detects_deep_analysis_without_vscode(self) -> None:
        domain = web_ui._infer_instruction_domain(
            "Research public hospital pricing data, build a RAG model, write and test the code, fix failures, and package the result for stakeholders."
        )
        self.assertEqual(domain, "code_workbench")

    def test_infer_instruction_domain_keeps_payer_precedence(self) -> None:
        domain = web_ui._infer_instruction_domain(
            "Review Durham, NC payer pricing, build a vector store, write and test the code, and create the stakeholder workbook."
        )
        self.assertEqual(domain, "payer_pricing_review")

    def test_resolve_domain_defaults_applies_policy(self) -> None:
        with patch(
            "lam.interface.web_ui._load_policy_freshness_defaults",
            return_value={
                "enabled": True,
                "domains": {
                    "web_research": {
                        "artifact_reuse_mode": "always_regenerate",
                        "artifact_reuse_max_age_hours": 6,
                    }
                },
            },
        ):
            mode, hours, domain, source = web_ui._resolve_domain_freshness_defaults(
                instruction="Research latest AI market news",
                requested_mode="reuse_if_recent",
                requested_hours=72,
                use_domain_defaults=True,
            )
        self.assertEqual(domain, "web_research")
        self.assertEqual(mode, "always_regenerate")
        self.assertEqual(hours, 6)
        self.assertEqual(source, "domain_default")

    def test_resolve_domain_defaults_can_be_disabled(self) -> None:
        with patch(
            "lam.interface.web_ui._load_policy_freshness_defaults",
            return_value={
                "enabled": True,
                "domains": {
                    "web_research": {
                        "artifact_reuse_mode": "always_regenerate",
                        "artifact_reuse_max_age_hours": 6,
                    }
                },
            },
        ):
            mode, hours, domain, source = web_ui._resolve_domain_freshness_defaults(
                instruction="Research latest AI market news",
                requested_mode="reuse_if_recent",
                requested_hours=72,
                use_domain_defaults=False,
            )
        self.assertEqual(domain, "web_research")
        self.assertEqual(mode, "reuse_if_recent")
        self.assertEqual(hours, 72)
        self.assertEqual(source, "manual_override")

    def test_set_policy_freshness_domain_updates_yaml_shape(self) -> None:
        with patch("lam.interface.web_ui._load_policy_yaml", return_value={"policies": {}}), patch(
            "lam.interface.web_ui._save_policy_yaml"
        ) as mock_save:
            out = web_ui._set_policy_freshness_domain(
                domain="artifact_generation",
                mode="reuse_if_recent",
                max_age_hours=168,
            )
        self.assertTrue(out.get("ok"))
        self.assertEqual(out.get("domain"), "artifact_generation")
        self.assertEqual(out.get("mode"), "reuse_if_recent")
        self.assertEqual(out.get("max_age_hours"), 168)
        self.assertTrue(mock_save.called)
        saved = mock_save.call_args.args[0]
        policies = saved.get("policies", {}) or {}
        freshness = policies.get("freshness_defaults", {}) or {}
        domains = freshness.get("domains", {}) or {}
        self.assertIn("artifact_generation", domains)
        self.assertEqual(domains["artifact_generation"]["artifact_reuse_mode"], "reuse_if_recent")


if __name__ == "__main__":
    unittest.main()
