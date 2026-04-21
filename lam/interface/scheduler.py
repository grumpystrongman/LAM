from __future__ import annotations

import json
import threading
import time
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List


@dataclass(slots=True)
class ScheduleJob:
    id: str
    name: str
    automation_name: str
    kind: str  # interval | daily | event
    value: str
    enabled: bool = True
    created_ts: float = 0.0
    last_run_ts: float = 0.0
    next_run_ts: float = 0.0
    event_name: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class ScheduleEngine:
    def __init__(self, storage_path: str | Path, run_callback: Callable[[ScheduleJob], Dict[str, Any]]) -> None:
        self.storage_path = Path(storage_path)
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        self.run_callback = run_callback
        self.lock = threading.Lock()
        self.jobs: Dict[str, ScheduleJob] = {}
        self.event_queue: List[str] = []
        self.history: List[Dict[str, Any]] = []
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._load()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        self.trigger_event("on_startup")

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)

    def add_job(self, name: str, automation_name: str, kind: str, value: str) -> ScheduleJob:
        now = time.time()
        job = ScheduleJob(
            id=str(uuid.uuid4()),
            name=name.strip() or f"{kind}:{automation_name}",
            automation_name=automation_name.strip(),
            kind=kind,
            value=value,
            enabled=True,
            created_ts=now,
            next_run_ts=self._compute_next_run(kind, value, now),
            event_name=value if kind == "event" else "",
        )
        with self.lock:
            self.jobs[job.id] = job
            self._save_locked()
        return job

    def delete_job(self, job_id: str) -> bool:
        with self.lock:
            removed = self.jobs.pop(job_id, None) is not None
            if removed:
                self._save_locked()
            return removed

    def list_jobs(self) -> List[Dict[str, Any]]:
        with self.lock:
            return [job.to_dict() for job in self.jobs.values()]

    def list_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self.lock:
            return list(self.history[-limit:])

    def trigger_event(self, event_name: str) -> None:
        with self.lock:
            self.event_queue.append(event_name.strip().lower())

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            now = time.time()
            due_jobs: List[ScheduleJob] = []
            queued_events: List[str] = []
            with self.lock:
                if self.event_queue:
                    queued_events = list(self.event_queue)
                    self.event_queue.clear()
                for job in self.jobs.values():
                    if not job.enabled:
                        continue
                    if job.kind in {"interval", "daily"} and now >= job.next_run_ts > 0:
                        due_jobs.append(job)
                if queued_events:
                    for job in self.jobs.values():
                        if job.enabled and job.kind == "event" and job.event_name in queued_events:
                            due_jobs.append(job)

            for job in due_jobs:
                self._run_job(job, now)
            time.sleep(1.0)

    def _run_job(self, job: ScheduleJob, now: float) -> None:
        try:
            result = self.run_callback(job)
            status = "ok" if result.get("ok", False) else "error"
        except Exception as exc:  # pylint: disable=broad-exception-caught
            status = "exception"
            result = {"ok": False, "error": str(exc)}

        with self.lock:
            current = self.jobs.get(job.id)
            if current is None:
                return
            current.last_run_ts = now
            if current.kind in {"interval", "daily"}:
                current.next_run_ts = self._compute_next_run(current.kind, current.value, now)
            record = {
                "job_id": current.id,
                "job_name": current.name,
                "automation_name": current.automation_name,
                "status": status,
                "result": result,
                "ts": now,
            }
            self.history.append(record)
            self.history = self.history[-500:]
            self._save_locked()

    def _compute_next_run(self, kind: str, value: str, now: float) -> float:
        if kind == "interval":
            try:
                seconds = max(1, int(value))
            except ValueError:
                seconds = 60
            return now + seconds
        if kind == "daily":
            # value format HH:MM local time
            try:
                hh, mm = value.split(":", 1)
                target_hour = int(hh)
                target_minute = int(mm)
            except Exception:
                target_hour = 9
                target_minute = 0
            dt_now = datetime.fromtimestamp(now)
            dt_target = dt_now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
            if dt_target.timestamp() <= now:
                dt_target = dt_target.fromtimestamp(dt_target.timestamp() + 86400)
            return dt_target.timestamp()
        return 0.0

    def _load(self) -> None:
        if not self.storage_path.exists():
            return
        try:
            raw = json.loads(self.storage_path.read_text(encoding="utf-8"))
        except Exception:
            return
        jobs = raw.get("jobs", [])
        history = raw.get("history", [])
        with self.lock:
            for item in jobs:
                try:
                    job = ScheduleJob(**item)
                    self.jobs[job.id] = job
                except Exception:
                    continue
            self.history = history[-500:]

    def _save_locked(self) -> None:
        payload = {
            "jobs": [job.to_dict() for job in self.jobs.values()],
            "history": self.history[-500:],
        }
        self.storage_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

