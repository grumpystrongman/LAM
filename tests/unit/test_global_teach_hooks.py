import unittest

from lam.interface.global_teach_hooks import GlobalTeachHooks
from lam.interface.teach_recorder import TeachRecorder


class TestGlobalTeachHooks(unittest.TestCase):
    def test_start_stop(self) -> None:
        hooks = GlobalTeachHooks(TeachRecorder())
        started = hooks.start()
        self.assertIn("ok", started)
        stopped = hooks.stop()
        self.assertTrue(stopped["ok"])


if __name__ == "__main__":
    unittest.main()

