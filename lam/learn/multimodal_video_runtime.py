from __future__ import annotations

import json
import math
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.parse
import uuid
from dataclasses import dataclass
from datetime import datetime
from importlib.util import find_spec
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

from lam.interface.local_vector_store import LocalVectorStore

from .transcript_extractor import extract_transcript
from .video_ingest import ingest_video_source


@dataclass(slots=True)
class Segment:
    start_seconds: float
    end_seconds: float
    text: str


def analyze_multimodal_video(
    *,
    topic: str,
    source: Dict[str, object],
    workspace: Path,
    context: Dict[str, Any] | None = None,
    memory_store: Any | None = None,
) -> Dict[str, Any]:
    runtime = MultimodalVideoRuntime(memory_store=memory_store)
    return runtime.analyze(topic=topic, source=source, workspace=workspace, context=context)


class MultimodalVideoRuntime:
    def __init__(
        self,
        *,
        memory_store: Any | None = None,
        vector_path: str | Path = "data/knowledge/topic_video_memory.db",
        default_chunk_seconds: int = 300,
    ) -> None:
        self.memory_store = memory_store or self._default_memory_store()
        self.vector_store = LocalVectorStore(path=vector_path, dims=384)
        self.default_chunk_seconds = max(30, int(default_chunk_seconds))

    def analyze(self, *, topic: str, source: Dict[str, object], workspace: Path, context: Dict[str, Any] | None = None) -> Dict[str, Any]:
        ctx = dict(context or {})
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]
        run_dir = workspace / "video_learning_runs" / run_id
        run_dir.mkdir(parents=True, exist_ok=True)

        process = _new_process_state(run_id=run_id, topic=topic, source_url=str(source.get("source_url", "")))
        self._mark_phase(process, "ingestion_started", status="running")
        payload = ingest_video_source(str(source.get("source_url", "")), source)
        local_video = self._resolve_local_video(payload=payload, run_dir=run_dir, context=ctx)
        metadata = self._video_metadata(local_video)
        process["metrics"]["video_duration_seconds"] = metadata.get("duration_seconds", 0.0)
        self._mark_phase(process, "metadata_resolved", status="done", details={"has_local_video": bool(local_video), "duration_seconds": metadata.get("duration_seconds", 0.0)})

        transcript = self._extract_transcript(payload=payload, local_video=local_video, run_dir=run_dir, context=ctx, metadata=metadata)
        segments = self._transcript_segments(transcript, duration_seconds=float(metadata.get("duration_seconds", 0.0) or 0.0))
        self._mark_phase(
            process,
            "transcript_extracted",
            status="done",
            details={"method": transcript.get("method", ""), "coverage": transcript.get("coverage", 0.0), "segment_count": len(segments)},
        )

        sampled_frames, scene_markers = self._extract_visual_signals(local_video=local_video, run_dir=run_dir, context=ctx, metadata=metadata)
        self._mark_phase(
            process,
            "visual_signals_extracted",
            status="done",
            details={"frame_count": len(sampled_frames), "scene_marker_count": len(scene_markers)},
        )

        chunk_seconds = max(5, int(ctx.get("video_chunk_seconds", self.default_chunk_seconds) or self.default_chunk_seconds))
        max_chunks = max(1, int(ctx.get("video_max_chunks", 400) or 400))
        chunk_reports = self._analyze_chunks(
            topic=topic,
            transcript_segments=segments,
            sampled_frames=sampled_frames,
            scene_markers=scene_markers,
            duration_seconds=float(metadata.get("duration_seconds", 0.0) or 0.0),
            chunk_seconds=chunk_seconds,
            max_chunks=max_chunks,
        )
        self._mark_phase(
            process,
            "chunk_analysis_complete",
            status="done",
            details={"chunk_count": len(chunk_reports), "chunk_seconds": chunk_seconds, "max_chunks": max_chunks},
        )

        visual_observations = _build_observations(chunk_reports)
        follow_up = _build_follow_up_questions(topic=topic, transcript=transcript, chunk_reports=chunk_reports, scene_markers=scene_markers)
        self._mark_phase(
            process,
            "follow_up_generated",
            status="done",
            details={"question_count": len(follow_up.get("questions", [])), "research_query_count": len(follow_up.get("research_queries", []))},
        )

        memory_refs = self._persist_learning_memory(
            topic=topic,
            payload=payload,
            chunk_reports=chunk_reports,
            follow_up=follow_up,
            transcript=transcript,
            process=process,
        )
        self._mark_phase(process, "memory_persisted", status="done", details={"memory_ref_count": len(memory_refs)})
        self._mark_phase(process, "complete", status="done", details={"status": "completed"})
        process["status"] = "completed"
        process["completed_at"] = _now_iso()

        process_path = run_dir / "process_state.json"
        process_path.write_text(json.dumps(process, indent=2), encoding="utf-8")

        chunk_text = "\n".join(str(item.get("summary", "") or "") for item in chunk_reports if str(item.get("summary", "")).strip())
        inferred_timestamps = [str(item.get("window", {}).get("start", "")) for item in chunk_reports[:12]]
        return {
            "transcript": transcript,
            "sampled_frames": sampled_frames,
            "visual_observations": visual_observations,
            "key_timestamps": inferred_timestamps[:12],
            "chunk_reports": chunk_reports,
            "process_state": process,
            "follow_up_questions": follow_up,
            "learning_memory_refs": memory_refs,
            "inferred_process_text": chunk_text,
            "process_state_path": str(process_path.resolve()),
            "local_video_path": str(local_video) if local_video else "",
            "scene_markers": scene_markers,
        }

    def _mark_phase(self, process: Dict[str, Any], phase: str, *, status: str, details: Dict[str, Any] | None = None) -> None:
        process.setdefault("phases", [])
        process["phases"].append({"phase": phase, "status": status, "timestamp": _now_iso(), "details": dict(details or {})})

    def _default_memory_store(self) -> Any:
        from lam.operator_platform.memory_store import MemoryStore

        return MemoryStore()

    def _resolve_local_video(self, *, payload: Dict[str, Any], run_dir: Path, context: Dict[str, Any]) -> Path | None:
        direct = _resolve_local_path_candidates(
            [
                str(payload.get("local_video_path", "")),
                str(payload.get("video_path", "")),
                str(payload.get("file_path", "")),
                str(payload.get("source_url", "")),
                str(context.get("local_video_path", "")),
            ]
        )
        if direct is not None:
            return direct

        source_url = str(payload.get("source_url", "")).strip()
        if not source_url:
            return None
        if not bool(context.get("allow_video_download", True)):
            return None
        if shutil.which("yt-dlp") is None:
            return None
        if "youtube.com" not in source_url and "youtu.be" not in source_url and "vimeo.com" not in source_url:
            return None

        cache_dir = run_dir / "downloaded_video"
        cache_dir.mkdir(parents=True, exist_ok=True)
        out_tmpl = str((cache_dir / "%(id)s.%(ext)s").resolve())
        cmd = ["yt-dlp", "--no-playlist", "-f", "mp4/best", "-o", out_tmpl, source_url]
        try:
            proc = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=int(context.get("video_download_timeout_sec", 600) or 600))
        except Exception:
            return None
        if proc.returncode != 0:
            return None
        files = sorted(cache_dir.glob("*"), key=lambda p: p.stat().st_mtime, reverse=True)
        return files[0] if files else None

    def _video_metadata(self, local_video: Path | None) -> Dict[str, Any]:
        if local_video is None or not local_video.exists():
            return {"duration_seconds": 0.0}
        if shutil.which("ffprobe") is None:
            return {"duration_seconds": 0.0}
        cmd = [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "json",
            str(local_video),
        ]
        try:
            proc = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=30)
            payload = json.loads(proc.stdout or "{}") if proc.returncode == 0 else {}
            duration = float(((payload.get("format", {}) or {}).get("duration", 0.0) or 0.0))
            return {"duration_seconds": round(max(0.0, duration), 3)}
        except Exception:
            return {"duration_seconds": 0.0}

    def _extract_transcript(
        self,
        *,
        payload: Dict[str, Any],
        local_video: Path | None,
        run_dir: Path,
        context: Dict[str, Any],
        metadata: Dict[str, Any],
    ) -> Dict[str, Any]:
        base = extract_transcript(payload)
        if float(base.get("coverage", 0.0) or 0.0) >= 0.75:
            return base
        whisper = self._try_whisper_transcript(local_video=local_video, run_dir=run_dir, context=context, duration_seconds=float(metadata.get("duration_seconds", 0.0) or 0.0))
        if whisper:
            return whisper
        return base

    def _try_whisper_transcript(
        self,
        *,
        local_video: Path | None,
        run_dir: Path,
        context: Dict[str, Any],
        duration_seconds: float,
    ) -> Dict[str, Any] | None:
        if local_video is None or not local_video.exists():
            return None
        if not bool(context.get("enable_whisper_cli", True)):
            return None
        whisper_runner = _resolve_whisper_runner(context=context)
        if not whisper_runner:
            return None
        out_dir = run_dir / "whisper"
        out_dir.mkdir(parents=True, exist_ok=True)
        timeout = int(context.get("whisper_timeout_sec", max(120, int(duration_seconds * 2) + 60)) or 240)
        cmd = [
            *whisper_runner,
            str(local_video),
            "--model",
            str(context.get("whisper_model", "base")),
            "--task",
            "transcribe",
            "--output_dir",
            str(out_dir.resolve()),
            "--output_format",
            "json",
            "--fp16",
            "False",
        ]
        try:
            env = dict(os.environ)
            env.setdefault("PYTHONUTF8", "1")
            env.setdefault("PYTHONIOENCODING", "utf-8")
            proc = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=timeout, env=env)
        except Exception:
            return None
        if proc.returncode != 0:
            return None
        json_candidates = sorted(out_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not json_candidates:
            return None
        try:
            payload = json.loads(json_candidates[0].read_text(encoding="utf-8", errors="ignore"))
        except Exception:
            return None
        text = str(payload.get("text", "") or "").strip()
        if not text:
            return None
        raw_segments = [dict(item) for item in list(payload.get("segments", []) or []) if isinstance(item, dict)]
        duration = max(0.0, float(duration_seconds))
        seg_duration = sum(max(0.0, float(item.get("end", 0.0) or 0.0) - float(item.get("start", 0.0) or 0.0)) for item in raw_segments)
        coverage = 0.85
        if duration > 0.0 and seg_duration > 0.0:
            coverage = min(0.99, max(0.45, seg_duration / duration))
        return {
            "text": text,
            "coverage": round(coverage, 3),
            "method": "whisper_cli",
            "segments": [
                {
                    "start_seconds": float(item.get("start", 0.0) or 0.0),
                    "end_seconds": float(item.get("end", 0.0) or 0.0),
                    "text": str(item.get("text", "") or "").strip(),
                }
                for item in raw_segments
                if str(item.get("text", "")).strip()
            ],
        }

    def _extract_visual_signals(
        self,
        *,
        local_video: Path | None,
        run_dir: Path,
        context: Dict[str, Any],
        metadata: Dict[str, Any],
    ) -> Tuple[List[Dict[str, Any]], List[float]]:
        if local_video is None or not local_video.exists():
            return [], []
        if shutil.which("ffmpeg") is None:
            return [], []

        duration = float(metadata.get("duration_seconds", 0.0) or 0.0)
        max_frames = max(8, int(context.get("max_sampled_frames", 80) or 80))
        if duration > 0:
            interval = max(2, int(math.ceil(duration / max_frames)))
        else:
            interval = int(context.get("frame_interval_seconds", 8) or 8)
        frame_dir = run_dir / "sampled_frames"
        frame_dir.mkdir(parents=True, exist_ok=True)
        out_pattern = str((frame_dir / "frame_%06d.jpg").resolve())
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            str(local_video),
            "-vf",
            f"fps=1/{interval}",
            "-q:v",
            "3",
            out_pattern,
        ]
        try:
            subprocess.run(
                cmd,
                check=False,
                capture_output=True,
                text=True,
                timeout=int(context.get("frame_extract_timeout_sec", max(90, int(duration * 2) + 45)) or 180),
            )
        except Exception:
            return [], []

        frame_files = sorted(frame_dir.glob("frame_*.jpg"))
        sampled: List[Dict[str, Any]] = []
        for idx, frame in enumerate(frame_files):
            sec = float(idx * interval)
            sampled.append(
                {
                    "timestamp": _seconds_to_hhmmss(sec),
                    "seconds": round(sec, 3),
                    "reason": "sampled_frame",
                    "text": "",
                    "frame_path": str(frame.resolve()),
                }
            )
        sampled = self._annotate_frames_with_ocr(sampled_frames=sampled, context=context)
        scene_markers = self._scene_markers(local_video=local_video, context=context)
        return sampled[:max_frames], scene_markers

    def _annotate_frames_with_ocr(self, *, sampled_frames: List[Dict[str, Any]], context: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not sampled_frames:
            return sampled_frames
        if not bool(context.get("enable_tesseract_ocr", True)):
            return sampled_frames
        tesseract_cmd = _resolve_tesseract_cmd(context=context)
        if not tesseract_cmd:
            return sampled_frames
        max_ocr = max(1, int(context.get("max_ocr_frames", 24) or 24))
        for frame in sampled_frames[:max_ocr]:
            frame_path = str(frame.get("frame_path", "") or "")
            if not frame_path:
                continue
            cmd = [tesseract_cmd, frame_path, "stdout", "-l", "eng", "--psm", "6"]
            try:
                proc = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=20)
            except Exception:
                continue
            if proc.returncode != 0:
                continue
            ocr = re.sub(r"\s+", " ", str(proc.stdout or "")).strip()
            if ocr:
                frame["text"] = ocr[:240]
                frame["reason"] = "ocr_frame"
        return sampled_frames

    def _scene_markers(self, *, local_video: Path, context: Dict[str, Any]) -> List[float]:
        threshold = float(context.get("scene_threshold", 0.35) or 0.35)
        cmd = [
            "ffmpeg",
            "-i",
            str(local_video),
            "-vf",
            f"select=gt(scene\\,{threshold}),showinfo",
            "-f",
            "null",
            "NUL",
        ]
        try:
            proc = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=int(context.get("scene_detect_timeout_sec", 150) or 150))
        except Exception:
            return []
        markers: List[float] = []
        text = (proc.stderr or "") + "\n" + (proc.stdout or "")
        for match in re.finditer(r"pts_time:([0-9]+(?:\.[0-9]+)?)", text):
            try:
                markers.append(float(match.group(1)))
            except Exception:
                continue
        markers = sorted({round(item, 3) for item in markers})
        return markers[:200]

    def _transcript_segments(self, transcript: Dict[str, Any], duration_seconds: float) -> List[Segment]:
        raw_segments = [dict(item) for item in list(transcript.get("segments", []) or []) if isinstance(item, dict)]
        segments: List[Segment] = []
        for item in raw_segments:
            text = str(item.get("text", "") or "").strip()
            if not text:
                continue
            start = float(item.get("start_seconds", item.get("start", 0.0)) or 0.0)
            end = float(item.get("end_seconds", item.get("end", start + 4.0)) or (start + 4.0))
            if end <= start:
                end = start + 2.0
            segments.append(Segment(start_seconds=max(0.0, start), end_seconds=max(start + 0.5, end), text=text))
        if segments:
            return segments

        text = str(transcript.get("text", "") or "").strip()
        if not text:
            return []
        sentences = [item.strip() for item in re.split(r"(?<=[.!?])\s+", text) if item.strip()]
        if not sentences:
            sentences = [text]
        if duration_seconds <= 0.0:
            duration_seconds = float(max(30, len(sentences) * 6))
        step = max(3.0, duration_seconds / max(1, len(sentences)))
        for idx, sentence in enumerate(sentences):
            start = idx * step
            end = min(duration_seconds, start + step)
            segments.append(Segment(start_seconds=start, end_seconds=end, text=sentence))
        return segments

    def _analyze_chunks(
        self,
        *,
        topic: str,
        transcript_segments: Sequence[Segment],
        sampled_frames: List[Dict[str, Any]],
        scene_markers: List[float],
        duration_seconds: float,
        chunk_seconds: int,
        max_chunks: int,
    ) -> List[Dict[str, Any]]:
        max_time = max(
            duration_seconds,
            max((segment.end_seconds for segment in transcript_segments), default=0.0),
            max((float(frame.get("seconds", 0.0) or 0.0) for frame in sampled_frames), default=0.0),
        )
        chunk_count = min(max_chunks, max(1, int(math.ceil(max_time / float(chunk_seconds or 1)))))
        reports: List[Dict[str, Any]] = []
        for idx in range(chunk_count):
            start = float(idx * chunk_seconds)
            end = float((idx + 1) * chunk_seconds)
            segs = [segment for segment in transcript_segments if (segment.start_seconds < end and segment.end_seconds >= start)]
            chunk_text = " ".join(segment.text for segment in segs).strip()
            chunk_frames = [frame for frame in sampled_frames if start <= float(frame.get("seconds", 0.0) or 0.0) < end]
            chunk_scenes = [value for value in scene_markers if start <= value < end]
            stage = _infer_stage(chunk_text)
            verbs = _verbs_in_text(chunk_text)
            checklist = _process_checklist(chunk_text)
            summary = _chunk_summary(topic=topic, stage=stage, chunk_text=chunk_text, checklist=checklist, scene_count=len(chunk_scenes), frame_count=len(chunk_frames))
            reports.append(
                {
                    "chunk_index": idx + 1,
                    "window": {"start": _seconds_to_hhmmss(start), "end": _seconds_to_hhmmss(end), "start_seconds": round(start, 3), "end_seconds": round(end, 3)},
                    "stage": stage,
                    "summary": summary,
                    "action_signals": verbs[:12],
                    "checklist": checklist,
                    "scene_change_count": len(chunk_scenes),
                    "frame_count": len(chunk_frames),
                    "ocr_hints": [str(frame.get("text", "") or "") for frame in chunk_frames if str(frame.get("text", "")).strip()][:4],
                    "transcript_excerpt": chunk_text[:420],
                }
            )
        return reports

    def _persist_learning_memory(
        self,
        *,
        topic: str,
        payload: Dict[str, Any],
        chunk_reports: List[Dict[str, Any]],
        follow_up: Dict[str, Any],
        transcript: Dict[str, Any],
        process: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        refs: List[Dict[str, Any]] = []
        namespace = f"topic_video::{_slug(topic)}"
        source_url = str(payload.get("source_url", ""))
        for chunk in chunk_reports:
            chunk_idx = int(chunk.get("chunk_index", 0) or 0)
            summary = str(chunk.get("summary", "") or "")
            excerpt = str(chunk.get("transcript_excerpt", "") or "")
            ocr = " | ".join(str(item) for item in list(chunk.get("ocr_hints", []) or []) if str(item).strip())
            memory_text = "\n".join([f"Topic: {topic}", f"Source: {source_url}", f"Chunk: {chunk_idx}", summary, excerpt, ocr]).strip()
            if len(memory_text) < 20:
                continue
            self.vector_store.add_document(
                app_name=namespace,
                source_url=f"{source_url}#chunk-{chunk_idx}",
                title=f"{topic} chunk {chunk_idx}",
                content=memory_text,
            )
            mem_payload = {
                "type": "video_chunk_learning",
                "scope": "project",
                "project_id": _slug(topic),
                "content": {
                    "topic": topic,
                    "source_url": source_url,
                    "chunk_index": chunk_idx,
                    "window": dict(chunk.get("window", {}) or {}),
                    "summary": summary,
                    "stage": str(chunk.get("stage", "")),
                },
                "tags": ["topic_mastery", "video_chunk", _slug(topic)],
                "source": "multimodal_video_runtime",
                "confidence": float(transcript.get("coverage", 0.5) or 0.5),
                "retrieval_policy": "strict",
                "invalidation_keys": {"domain": "topic_learning", "topic": topic},
            }
            mem_id = self.memory_store.save_memory(mem_payload)
            refs.append({"kind": "chunk_memory", "chunk_index": chunk_idx, "memory_id": mem_id})
        fq_payload = {
            "type": "follow_up_questions",
            "scope": "project",
            "project_id": _slug(topic),
            "content": {"topic": topic, "questions": list(follow_up.get("questions", []) or []), "research_queries": list(follow_up.get("research_queries", []) or [])},
            "tags": ["topic_mastery", "follow_up", _slug(topic)],
            "source": "multimodal_video_runtime",
            "confidence": 0.62,
            "retrieval_policy": "strict",
            "invalidation_keys": {"domain": "topic_learning", "topic": topic},
        }
        refs.append({"kind": "follow_up_memory", "memory_id": self.memory_store.save_memory(fq_payload)})
        refs.append({"kind": "vector_namespace", "namespace": namespace, "process_status": str(process.get("status", ""))})
        return refs


def _new_process_state(*, run_id: str, topic: str, source_url: str) -> Dict[str, Any]:
    return {
        "run_id": run_id,
        "topic": topic,
        "source_url": source_url,
        "status": "running",
        "started_at": _now_iso(),
        "completed_at": "",
        "phases": [],
        "metrics": {},
    }


def _build_observations(chunk_reports: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    observations: List[Dict[str, str]] = []
    for chunk in chunk_reports:
        observations.append(
            {
                "timestamp": str((chunk.get("window", {}) or {}).get("start", "")),
                "app_or_site": "Video workflow",
                "workflow_stage": str(chunk.get("stage", "")),
                "ui_elements": ", ".join(list(chunk.get("action_signals", []) or [])[:5]),
                "uncertainty": "low" if str(chunk.get("transcript_excerpt", "")).strip() else "medium",
            }
        )
    return observations


def _build_follow_up_questions(
    *,
    topic: str,
    transcript: Dict[str, Any],
    chunk_reports: List[Dict[str, Any]],
    scene_markers: List[float],
) -> Dict[str, List[str]]:
    coverage = float(transcript.get("coverage", 0.0) or 0.0)
    questions: List[str] = []
    queries: List[str] = []
    if coverage < 0.7:
        questions.append("Which critical steps were spoken quickly or unclearly and need confirmation from official docs?")
        queries.append(f"{topic} official step by step checklist")
    if len(scene_markers) <= 1:
        questions.append("Are there hidden interface transitions not captured by scene-change detection?")
        queries.append(f"{topic} UI walkthrough with visible clicks")
    risky_chunks = []
    for chunk in chunk_reports:
        action_text = " ".join(str(item) for item in list(chunk.get("action_signals", []) or []))
        if any(token in action_text for token in ["publish", "submit", "approve", "send"]):
            risky_chunks.append(chunk)
    if risky_chunks:
        questions.append("What pre-flight validation gates are required before irreversible actions?")
        queries.append(f"{topic} approval checklist before submit publish")
    if not any(str(chunk.get("checklist", "")).strip() for chunk in chunk_reports):
        questions.append("What explicit ordered checklist should be followed for repeatable execution?")
        queries.append(f"{topic} standard operating procedure SOP")
    questions.append("What edge cases and exception paths were not demonstrated in the video?")
    queries.append(f"{topic} common failures troubleshooting")
    dedup_q = _dedupe_text(questions)[:10]
    dedup_queries = _dedupe_text(queries)[:10]
    return {"questions": dedup_q, "research_queries": dedup_queries}


def _chunk_summary(*, topic: str, stage: str, chunk_text: str, checklist: List[str], scene_count: int, frame_count: int) -> str:
    if not chunk_text:
        return f"{stage} phase observed with {frame_count} sampled frames and {scene_count} scene transitions; transcript signal is limited."
    checklist_text = "; ".join(checklist[:3]) if checklist else "No explicit checklist extracted."
    return f"{stage} phase for {topic}. Checklist: {checklist_text}. Evidence includes {frame_count} frames and {scene_count} scene transitions."


def _infer_stage(text: str) -> str:
    low = str(text or "").lower()
    if any(token in low for token in ["sign in", "login", "open", "launch", "install", "setup"]):
        return "setup"
    if any(token in low for token in ["navigate", "click", "select", "open tab", "menu"]):
        return "navigation"
    if any(token in low for token in ["type", "enter", "fill", "edit", "paste"]):
        return "data_entry"
    if any(token in low for token in ["verify", "validate", "check", "review", "audit"]):
        return "validation"
    if any(token in low for token in ["submit", "publish", "send", "approve"]):
        return "submission"
    return "workflow"


def _verbs_in_text(text: str) -> List[str]:
    low = str(text or "").lower()
    verbs = ["open", "click", "select", "type", "enter", "edit", "review", "validate", "check", "run", "submit", "publish", "approve", "export"]
    return [verb for verb in verbs if f" {verb} " in f" {low} "]


def _process_checklist(text: str) -> List[str]:
    sentences = [item.strip() for item in re.split(r"(?<=[.!?])\s+", str(text or "")) if item.strip()]
    checklist: List[str] = []
    for sentence in sentences:
        low = sentence.lower()
        if any(low.startswith(verb) or f" {verb} " in low for verb in ["open", "click", "select", "type", "enter", "review", "validate", "check", "submit"]):
            checklist.append(sentence[:180])
        if len(checklist) >= 8:
            break
    return checklist


def _resolve_local_path_candidates(candidates: Sequence[str]) -> Path | None:
    for raw in candidates:
        value = str(raw or "").strip()
        if not value:
            continue
        if value.lower().startswith("file://"):
            parsed = urllib.parse.urlparse(value)
            value = urllib.parse.unquote(parsed.path or "")
            if re.match(r"^/[A-Za-z]:", value):
                value = value[1:]
        path = Path(value)
        if path.exists() and path.is_file():
            return path.resolve()
    return None


def _seconds_to_hhmmss(seconds: float) -> str:
    total = max(0, int(seconds))
    hours = total // 3600
    mins = (total % 3600) // 60
    secs = total % 60
    return f"{hours:02d}:{mins:02d}:{secs:02d}"


def _dedupe_text(items: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in items:
        text = re.sub(r"\s+", " ", str(item or "")).strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(text or "").lower()).strip("_")[:80] or "topic"


def _resolve_tesseract_cmd(*, context: Dict[str, Any]) -> str:
    configured = str(context.get("tesseract_cmd", "") or "").strip()
    if configured and Path(configured).exists():
        return configured
    which = shutil.which("tesseract")
    if which:
        return which
    for candidate in [
        r"C:\Program Files\Tesseract-OCR\tesseract.exe",
        r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
    ]:
        if Path(candidate).exists():
            return candidate
    return ""


def _resolve_whisper_runner(*, context: Dict[str, Any]) -> List[str]:
    configured = str(context.get("whisper_cmd", "") or "").strip()
    if configured and Path(configured).exists():
        return [configured]
    which = shutil.which("whisper")
    if which:
        return [which]
    executable = Path(sys.executable).resolve()
    sibling = executable.parent / "whisper.exe"
    if sibling.exists():
        return [str(sibling)]
    project_venv = Path(".venv") / "Scripts" / "whisper.exe"
    if project_venv.exists():
        return [str(project_venv.resolve())]
    # Fallback: if whisper is installed in the current interpreter, module execution works.
    if find_spec("whisper") is not None:
        return [sys.executable, "-m", "whisper"]
    return []
