import unittest

from lam.governance.authn import TokenAuth


class TestTokenAuth(unittest.TestCase):
    def test_issue_and_verify(self) -> None:
        auth = TokenAuth(secret="test-secret", issuer="lam-test")
        token = auth.issue(subject="alice", roles=["Runner"], ttl_seconds=120, department="Claims", clearance="high")
        principal = auth.verify(token)
        self.assertEqual(principal.subject, "alice")
        self.assertEqual(principal.roles, ["Runner"])
        self.assertEqual(principal.department, "Claims")


if __name__ == "__main__":
    unittest.main()

