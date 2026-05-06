# Testing and Validation

## Fast Product Smoke Slice

```powershell
python -m pytest -q tests\unit\test_main_cli.py tests\unit\test_password_vault.py tests\unit\test_web_ui_human_suite.py tests\unit\test_skill_library.py tests\unit\test_skill_runtime.py tests\unit\test_topic_mastery.py
```

## Broader Operator Slice

```powershell
python -m pytest -q tests\unit\test_search_agent.py tests\unit\test_operator_platform.py tests\unit\test_mission_runtime.py tests\unit\test_web_ui_human_suite.py
```

## What These Cover

- CLI parser and help surface
- local UI product surface
- password vault resilience
- learned-skill storage and practice runtime
- Topic Mastery Learn Mode
- mission runtime and operator-platform integration

## Practical Rule

When changing OpenLAMb, do not stop at code changes.
Run at least one fast UI/CLI slice and one domain/runtime slice that matches the feature you touched.
