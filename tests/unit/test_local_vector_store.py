from pathlib import Path
import uuid
import unittest

from lam.interface.local_vector_store import LocalVectorStore


class TestLocalVectorStore(unittest.TestCase):
    def test_add_and_search(self) -> None:
        root = Path("test_artifacts")
        root.mkdir(parents=True, exist_ok=True)
        db_path = root / f"knowledge_{uuid.uuid4().hex}.db"
        store = LocalVectorStore(path=db_path)
        store.add_document(
            app_name="chatgpt",
            source_url="https://example.com/chatgpt-guide",
            title="ChatGPT desktop quickstart",
            content="Open new chat, type prompt, use conversation history for recall.",
        )
        results = store.search("chatgpt", "how to use chat history", top_k=3)
        self.assertGreaterEqual(len(results), 1)
        self.assertIn("quickstart", results[0]["title"].lower())


if __name__ == "__main__":
    unittest.main()

