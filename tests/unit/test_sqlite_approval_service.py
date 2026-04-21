from pathlib import Path
import uuid
import unittest

from lam.services.sqlite_approval_service import SqliteApprovalService


class TestSqliteApprovalService(unittest.TestCase):
    def setUp(self) -> None:
        root = Path("test_artifacts")
        root.mkdir(parents=True, exist_ok=True)
        self.db_path = root / f"approval_test_{uuid.uuid4().hex}.db"
        self.service = SqliteApprovalService(path=self.db_path)

    def tearDown(self) -> None:
        # Windows may hold file handles briefly; cleanup is handled out-of-band.
        pass

    def test_dual_approval(self) -> None:
        request_id = self.service.create_request(
            step={"id": "10", "type": "submit_action"},
            approver_levels=["manager", "compliance"],
            context={"workflow_id": "wf1"},
        )
        self.assertEqual(self.service.get_status(request_id), "pending")

        status = self.service.approve(request_id, approver_id="mgr1", approver_level="manager")
        self.assertEqual(status, "pending")

        status = self.service.approve(request_id, approver_id="comp1", approver_level="compliance")
        self.assertEqual(status, "approved")
        self.assertEqual(self.service.get_status(request_id), "approved")


if __name__ == "__main__":
    unittest.main()
