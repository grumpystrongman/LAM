import unittest
from pathlib import Path
from unittest.mock import patch
import shutil

from lam.deep_workbench.workflow import build_workspace, extract_workbench_contract
from lam.interface.app_launcher import normalize_app_name


class TestDeepWorkbench(unittest.TestCase):
    def _temp_root(self) -> str:
        root = Path("data") / "test_artifacts"
        root.mkdir(parents=True, exist_ok=True)
        return str(root.resolve())

    def test_extract_workbench_contract_detects_vscode_and_code_outputs(self) -> None:
        contract = extract_workbench_contract(
            "Create a new VS Code workspace for this task, write analysis code, and leave me a runnable scaffold."
        )
        self.assertTrue(contract.wants_vscode)
        self.assertTrue(contract.wants_code)
        self.assertIn("code", contract.requested_outputs)
        self.assertIn("workspace", contract.requested_outputs)

    def test_normalize_app_name_maps_vscode_aliases(self) -> None:
        self.assertEqual(normalize_app_name("VS Code"), "vscode")
        self.assertEqual(normalize_app_name("Visual Studio Code"), "vscode")
        self.assertEqual(normalize_app_name("code"), "vscode")

    @patch("lam.deep_workbench.workflow.open_app_target")
    def test_build_workspace_creates_scaffold_and_vscode_metadata(self, mock_open_app) -> None:
        mock_open_app.return_value = (True, "code")
        td = Path(self._temp_root()) / "deep_workbench_case"
        shutil.rmtree(td, ignore_errors=True)
        td.mkdir(parents=True, exist_ok=True)
        try:
            contract = extract_workbench_contract(
                "Create a new VS Code workspace for analysis and write code for me.",
                workspace_root=str(td),
            )
            result = build_workspace(contract=contract, open_vscode=True)
            workspace = Path(result["workspace"])
            self.assertTrue((workspace / "src" / "analysis.py").exists())
            self.assertTrue((workspace / "tests" / "test_smoke.py").exists())
            self.assertTrue((workspace / ".vscode" / "tasks.json").exists())
            self.assertTrue((workspace / "notes" / "task_brief.md").exists())
            self.assertTrue(result["vscode_launch"]["ok"])
            mock_open_app.assert_called_once()
        finally:
            shutil.rmtree(td, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
