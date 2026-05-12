import unittest

from lam.governance.authn import TokenAuth
from lam.services.api_server import ApiAuth, ApiAuthConfig


class TestApiAuth(unittest.TestCase):
    def test_fails_closed_when_auth_not_configured(self) -> None:
        auth = ApiAuth(ApiAuthConfig())
        with self.assertRaises(PermissionError):
            auth.authenticate({})

    def test_allows_insecure_anonymous_when_explicitly_enabled(self) -> None:
        auth = ApiAuth(ApiAuthConfig(allow_insecure_anonymous_api=True))
        principal = auth.authenticate({})
        self.assertEqual(principal.get("actor_id"), "anonymous")

    def test_accepts_api_key(self) -> None:
        auth = ApiAuth(ApiAuthConfig(api_key="secret-key"))
        principal = auth.authenticate({"x-api-key": "secret-key"})
        self.assertEqual(principal.get("actor_id"), "api_key_client")

    def test_accepts_bearer_token(self) -> None:
        config = ApiAuthConfig(bearer_secret="secret", bearer_issuer="issuer-1")
        auth = ApiAuth(config)
        token = TokenAuth(secret="secret", issuer="issuer-1").issue(subject="svc-user", roles=["Runner"])
        principal = auth.authenticate({"authorization": f"Bearer {token}"})
        self.assertEqual(principal.get("actor_id"), "svc-user")


if __name__ == "__main__":
    unittest.main()

