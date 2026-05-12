import unittest

from lam.dsl.validator import evaluate_condition


class TestDslValidator(unittest.TestCase):
    def test_evaluate_condition_supports_bool_compare_and_attribute(self) -> None:
        runtime = {
            "identity": {
                "user": {"role": "Runner"},
                "device": {"managed": True, "compliant": True},
            },
            "risk_tier": "high",
            "count": 3,
        }
        self.assertTrue(evaluate_condition("identity.user.role == 'Runner' and identity.device.managed", runtime))
        self.assertTrue(evaluate_condition("count >= 3 and risk_tier == 'high'", runtime))
        self.assertFalse(evaluate_condition("count < 2 or identity.user.role == 'Auditor'", runtime))

    def test_evaluate_condition_blocks_unsupported_nodes(self) -> None:
        runtime = {"x": 1}
        with self.assertRaises(ValueError):
            evaluate_condition("__import__('os').system('whoami')", runtime)


if __name__ == "__main__":
    unittest.main()

