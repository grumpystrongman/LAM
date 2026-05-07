import shutil
import subprocess
import unittest
from pathlib import Path

from lam.learn.multimodal_video_runtime import MultimodalVideoRuntime


class _DummyMemoryStore:
    def __init__(self) -> None:
        self.items = []

    def save_memory(self, item):
        memory_id = f"mem_{len(self.items) + 1}"
        self.items.append((memory_id, item))
        return memory_id


class TestMultimodalVideoRuntime(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path("data") / "test_artifacts" / "multimodal_video_case"
        shutil.rmtree(self.root, ignore_errors=True)
        self.root.mkdir(parents=True, exist_ok=True)
        self.video_path = self.root / "short_demo.mp4"
        self._build_short_video(self.video_path)

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def _build_short_video(self, path: Path) -> None:
        if shutil.which("ffmpeg") is None:
            self.skipTest("ffmpeg not available")
        cmd = [
            "ffmpeg",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "color=c=black:s=640x360:d=2",
            "-f",
            "lavfi",
            "-i",
            "color=c=blue:s=640x360:d=2",
            "-f",
            "lavfi",
            "-i",
            "color=c=green:s=640x360:d=2",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=880:duration=6",
            "-filter_complex",
            "[0:v][1:v][2:v]concat=n=3:v=1:a=0[v]",
            "-map",
            "[v]",
            "-map",
            "3:a",
            "-shortest",
            str(path),
        ]
        proc = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=90)
        if proc.returncode != 0:
            self.skipTest("failed to build synthetic test video")

    def test_runtime_analyzes_short_video_with_process_state(self) -> None:
        runtime = MultimodalVideoRuntime(memory_store=_DummyMemoryStore(), vector_path=self.root / "vectors.db", default_chunk_seconds=90)
        source = {
            "source_url": str(self.video_path.resolve()),
            "source_type": "video",
            "title": "Billing consolidation short demo",
            "transcript": "Open the billing app. Select the account group. Enter merged billing rules. Validate totals. Submit consolidation review.",
            "local_video_path": str(self.video_path.resolve()),
        }
        result = runtime.analyze(topic="billing consolidation workflow", source=source, workspace=self.root / "run1", context={"enable_whisper_cli": False})
        self.assertEqual(result["process_state"]["status"], "completed")
        self.assertTrue(result["chunk_reports"])
        self.assertTrue(result["visual_observations"])
        self.assertTrue(result["learning_memory_refs"])
        self.assertIn("questions", result["follow_up_questions"])
        self.assertTrue(Path(result["process_state_path"]).exists())


if __name__ == "__main__":
    unittest.main()
