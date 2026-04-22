import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from lam.interface.reliability_suite import run_reliability_suite


class TestReliabilitySuite(unittest.TestCase):
    def test_suite_without_pytest(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "lam" / "interface").mkdir(parents=True)
            (root / "tests" / "unit").mkdir(parents=True)
            (root / "config").mkdir(parents=True)
            (root / "lam" / "interface" / "web_ui.py").write_text("# stub", encoding="utf-8")
            (root / "config" / "policy.yaml").write_text("policies: {}", encoding="utf-8")
            (root / "config" / "control_plane.yaml").write_text("auth: {}", encoding="utf-8")

            with patch("lam.interface.reliability_suite.importlib.import_module", return_value=object()):
                result = run_reliability_suite(include_pytest=False, project_root=root)

            self.assertTrue(result["ok"])
            self.assertEqual(result["mode"], "reliability_suite")
            self.assertEqual(result["summary"]["failed"], 0)
            self.assertFalse(result["pytest"]["requested"])

    def test_suite_with_pytest_failure_marks_not_ok(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "lam" / "interface").mkdir(parents=True)
            (root / "tests" / "unit").mkdir(parents=True)
            (root / "config").mkdir(parents=True)
            (root / "lam" / "interface" / "web_ui.py").write_text("# stub", encoding="utf-8")
            (root / "config" / "policy.yaml").write_text("policies: {}", encoding="utf-8")
            (root / "config" / "control_plane.yaml").write_text("auth: {}", encoding="utf-8")

            completed = type("Completed", (), {"returncode": 1, "stdout": "1 failed", "stderr": ""})
            passing_check = {"name": "stub", "status": "pass", "ok": True, "details": "ok", "duration_ms": 0}
            with patch("lam.interface.reliability_suite._check_python_version", return_value=passing_check):
                with patch("lam.interface.reliability_suite._check_required_files", return_value=passing_check):
                    with patch("lam.interface.reliability_suite._check_yaml_configs", return_value=passing_check):
                        with patch("lam.interface.reliability_suite._check_imports", return_value=passing_check):
                            with patch("lam.interface.reliability_suite.subprocess.run", return_value=completed):
                                result = run_reliability_suite(include_pytest=True, pytest_args=["tests/unit/test_web_ui_history.py"], project_root=root)

            self.assertFalse(result["ok"])
            self.assertTrue(result["pytest"]["requested"])
            self.assertEqual(result["pytest"]["exit_code"], 1)
            self.assertGreaterEqual(result["summary"]["failed"], 1)

    def test_suite_with_desktop_smoke_failure_marks_not_ok(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "lam" / "interface").mkdir(parents=True)
            (root / "tests" / "unit").mkdir(parents=True)
            (root / "config").mkdir(parents=True)
            (root / "lam" / "interface" / "web_ui.py").write_text("# stub", encoding="utf-8")
            (root / "config" / "policy.yaml").write_text("policies: {}", encoding="utf-8")
            (root / "config" / "control_plane.yaml").write_text("auth: {}", encoding="utf-8")

            with patch("lam.interface.reliability_suite.importlib.import_module", return_value=object()):
                result = run_reliability_suite(
                    include_pytest=False,
                    project_root=root,
                    include_desktop_smoke=True,
                    desktop_smoke_runner=lambda: {"ok": False, "error": "desktop unavailable"},
                )

            self.assertFalse(result["ok"])
            names = [c.get("name") for c in result.get("checks", [])]
            self.assertIn("desktop_notepad_hello_world", names)
            self.assertGreaterEqual(result["summary"]["failed"], 1)


if __name__ == "__main__":
    unittest.main()
