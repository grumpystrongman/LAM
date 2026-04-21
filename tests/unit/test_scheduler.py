from pathlib import Path
import time
import uuid
import unittest

from lam.interface.scheduler import ScheduleEngine


class TestScheduler(unittest.TestCase):
    def test_interval_and_event_jobs(self) -> None:
        calls = []

        def callback(job):
            calls.append(job.automation_name)
            return {"ok": True}

        path = Path("test_artifacts") / f"sched_{uuid.uuid4().hex}.json"
        engine = ScheduleEngine(path, callback)
        try:
            engine.start()
            engine.add_job(name="i1", automation_name="auto1", kind="interval", value="1")
            engine.add_job(name="e1", automation_name="auto2", kind="event", value="manual")
            engine.trigger_event("manual")
            time.sleep(2.5)
            self.assertTrue(len(calls) >= 2)
            jobs = engine.list_jobs()
            self.assertGreaterEqual(len(jobs), 2)
        finally:
            engine.stop()


if __name__ == "__main__":
    unittest.main()

