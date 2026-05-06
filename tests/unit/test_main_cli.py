import unittest

from lam.main import build_parser


class TestMainCli(unittest.TestCase):
    def test_parser_includes_topic_and_skill_commands(self) -> None:
        parser = build_parser()
        help_text = parser.format_help()
        self.assertIn("topic-learn", help_text)
        self.assertIn("skill-list", help_text)
        self.assertIn("skill-practice-preview", help_text)
        self.assertIn("skill-practice-run", help_text)
        self.assertIn("skill-refresh", help_text)
        self.assertIn("ui", help_text)

    def test_topic_learn_parser_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "topic-learn",
                "--instruction",
                "Learn how to build a Power BI KPI dashboard",
                "--seed-url",
                "https://youtube.com/example",
                "--output",
                "json",
            ]
        )
        self.assertEqual(args.command, "topic-learn")
        self.assertEqual(args.seed_url, "https://youtube.com/example")
        self.assertEqual(args.output, "json")

    def test_ui_subparser_help_contains_background_flag(self) -> None:
        parser = build_parser()
        ui_parser = parser._subparsers._group_actions[0].choices["ui"]  # type: ignore[attr-defined]  # noqa: SLF001
        help_text = ui_parser.format_help()
        self.assertIn("--background", help_text)

    def test_topic_subparser_help_contains_examples(self) -> None:
        parser = build_parser()
        topic_parser = parser._subparsers._group_actions[0].choices["topic-learn"]  # type: ignore[attr-defined]  # noqa: SLF001
        help_text = topic_parser.format_help()
        self.assertIn("Learn how to build a Power BI KPI dashboard", help_text)


if __name__ == "__main__":
    unittest.main()
