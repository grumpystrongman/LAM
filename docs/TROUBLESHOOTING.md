# Troubleshooting

## UI Starts But Is Not Reachable

Preferred detached launch:

```powershell
python -m lam.main ui --background --port 8814
```

Then open the printed URL.

If you want to run it in the foreground:

```powershell
python -m lam.main ui --no-open-browser
```

## Vault List Shows Unavailable Entries

This means one or more vault entries could not be unprotected in the current Windows session.
OpenLAMb now keeps the vault usable and marks those entries as unavailable instead of breaking the UI.

Typical causes:
- different Windows user context
- changed DPAPI scope
- migrated local data from another machine

## Canvas Covers The UI On Small Screens

Canvas no longer auto-opens on compact/mobile viewports.
If you still see it, close it and continue in Command Center.

## A Learned Skill Looks Weak

Do not run it blindly.
Instead:
1. inspect the mastery guide
2. preview safe practice
3. refresh the topic if it is version-sensitive
4. re-teach the execution path if the conceptual steps are right but app grounding is weak

## Teach Replay Keeps Failing

Check:
- branch timeline
- checkpoint heat
- re-teach guidance
- selector drift in the target app

If multiple branches fail at the same checkpoint, re-teach that checkpoint specifically.

## Topic Mastery Feels Generic

That usually means one of these is true:
- source quality was weak
- transcript coverage was weak
- the topic is version-sensitive and needs refresh
- the task needed app-specific grounding after conceptual learning

## CLI Output Is Too Verbose For Scripting

Use JSON output where supported:

```powershell
python -m lam.main skill-list --output json
```

## Windows Automation Quality Is Weak

Common reasons:
- optional local automation packages not installed
- OCR dependency not configured
- target UI unstable or heavily virtualized
- app state not consistent between runs

## Tests To Run First

```powershell
python -m pytest -q tests\unit\test_main_cli.py tests\unit\test_password_vault.py tests\unit\test_web_ui_human_suite.py
```
