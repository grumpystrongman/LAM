import unittest

from lam.interface.app_launcher import list_installed_apps, normalize_app_name


class TestAppLauncher(unittest.TestCase):
    def test_normalize(self) -> None:
        self.assertEqual(normalize_app_name("Open ChatGPT app"), "chatgpt")

    def test_list_installed_apps(self) -> None:
        apps = list_installed_apps(query="chat", limit=5)
        self.assertIsInstance(apps, list)


if __name__ == "__main__":
    unittest.main()

