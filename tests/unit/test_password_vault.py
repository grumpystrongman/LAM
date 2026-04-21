import tempfile
import unittest
from pathlib import Path

from lam.interface.password_vault import LocalPasswordVault


class TestPasswordVault(unittest.TestCase):
    def test_put_get_list_delete(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            vault = LocalPasswordVault(db_path=Path(td) / "vault.db")
            saved = vault.put_entry(service="linkedin", username="me@example.com", password="Secret!12345", tags=["jobs"], favorite=True)
            self.assertTrue(saved["ok"])
            entry_id = saved["id"]
            listed = vault.list_entries(query="link")
            self.assertGreaterEqual(len(listed), 1)
            self.assertIn("username_masked", listed[0])

            full = vault.get_entry(entry_id, include_secret=True)
            self.assertTrue(full["ok"])
            self.assertEqual(full["entry"]["service"], "linkedin")
            self.assertEqual(full["entry"]["username"], "me@example.com")

            resolved = vault.find_entry_by_service("linked")
            self.assertTrue(resolved["ok"])
            self.assertEqual(resolved["entry"]["service"], "linkedin")

            removed = vault.delete_entry(entry_id)
            self.assertTrue(removed["ok"])

    def test_generate_password(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            vault = LocalPasswordVault(db_path=Path(td) / "vault.db")
            result = vault.generate_password(length=24)
            self.assertTrue(result["ok"])
            self.assertGreaterEqual(len(result["password"]), 24)
            self.assertIn(result["strength"]["rating"], {"medium", "strong"})

    def test_export_import_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            db1 = root / "vault1.db"
            db2 = root / "vault2.db"
            export = root / "vault_export.lamvault"

            v1 = LocalPasswordVault(db_path=db1)
            v1.put_entry(service="github", username="alice", password="A!ice123456")
            out = v1.export_encrypted(str(export))
            self.assertTrue(out["ok"])
            self.assertTrue(export.exists())

            v2 = LocalPasswordVault(db_path=db2)
            imp = v2.import_encrypted(str(export), merge=True)
            self.assertTrue(imp["ok"])
            found = v2.find_entry_by_service("github")
            self.assertTrue(found["ok"])
            self.assertEqual(found["entry"]["username"], "alice")


if __name__ == "__main__":
    unittest.main()
