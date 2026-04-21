import unittest

from lam.interface.user_defaults import load_defaults, save_defaults


class TestUserDefaults(unittest.TestCase):
    def test_save_and_load_defaults(self) -> None:
        user = "test-user"
        values = {"step_mode": True, "ai_backend": "deterministic-local", "compression_mode": "aggressive"}
        save_defaults(values, user=user)
        loaded = load_defaults(user=user)
        self.assertEqual(loaded.get("compression_mode"), "aggressive")


if __name__ == "__main__":
    unittest.main()

