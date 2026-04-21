import unittest

from lam.interface.selector_picker import capture_selector_at_cursor


class TestSelectorPicker(unittest.TestCase):
    def test_capture_selector(self) -> None:
        result = capture_selector_at_cursor().to_dict()
        self.assertIn("ok", result)
        self.assertIn("error", result)


if __name__ == "__main__":
    unittest.main()

