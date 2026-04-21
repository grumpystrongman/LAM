import unittest

from lam.interface.teach_recorder import TeachRecorder


class TestTeachRecorder(unittest.TestCase):
    def test_record_and_generate_instruction(self) -> None:
        rec = TeachRecorder()
        rec.start("chatgpt")
        rec.capture_click({"value": "New chat", "metadata": {"name": "New chat"}})
        rec.capture_type("hello")
        rec.capture_hotkey("enter")
        result = rec.stop()
        self.assertTrue(result["ok"])
        self.assertIn("open chatgpt app", result["instruction"])
        self.assertIn("click New chat", result["instruction"])
        self.assertGreaterEqual(result["raw_event_count"], result["step_count"])

    def test_compress_type_bursts(self) -> None:
        rec = TeachRecorder()
        rec.start("chatgpt")
        rec.capture_type("hello")
        rec.capture_type(" ")
        rec.capture_type("world")
        result = rec.stop()
        actions = [e["action"] for e in result["compressed_events"]]
        self.assertEqual(actions.count("type_text"), 1)
        self.assertIn('type "hello world"', result["instruction"])

    def test_compression_modes(self) -> None:
        rec = TeachRecorder()
        rec.set_compression_mode("strict")
        rec.start("chatgpt")
        rec.capture_type("h")
        rec.capture_type("i")
        strict = rec.stop()

        rec.set_compression_mode("aggressive")
        rec.start("chatgpt")
        rec.capture_type("h")
        rec.capture_type("i")
        agg = rec.stop()

        self.assertGreaterEqual(strict["step_count"], agg["step_count"])


if __name__ == "__main__":
    unittest.main()
