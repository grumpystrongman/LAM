# Getting Started

## What You Need

Minimum:
- Windows
- Python 3.11+
- local browser access for web automation

## Easiest Install For PowerShell Users

Run these commands one line at a time from the OpenLAMb folder.

Preferred:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python -m pip install --upgrade pip
.\.venv\Scripts\python -m pip install -e .
```

If `py` is not available:

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install --upgrade pip
.\.venv\Scripts\python -m pip install -e .
```

This avoids the usual PowerShell pain points:
- no activate script required
- no multiline command blocks
- no need to remember whether the shell is using the right Python

## Verify It Works

```powershell
.\.venv\Scripts\python -m lam.main --help
```

If that prints the CLI help, the install is good.

## Optional Desktop Automation Packages

```powershell
.\.venv\Scripts\python -m pip install pywinauto pynput pyautogui opencv-python pillow pytesseract
```

## Launch the UI

Standard:

```powershell
.\.venv\Scripts\python -m lam.main ui
```

Detached background launch:

```powershell
.\.venv\Scripts\python -m lam.main ui --background --port 8814
```

Open the local URL printed by the CLI, usually:

```text
http://127.0.0.1:8795
```

If you used `--port 8814`, open:

```text
http://127.0.0.1:8814
```

## Your First 10 Minutes

1. Click `Accept Control`
2. Run a safe task:

```text
open notepad app then type "OpenLAMb is live" then press enter
```

3. Open Canvas and inspect the result
4. Try a research prompt:

```text
Research the market for AI desktop agents and build an executive briefing with recommendations.
```

5. Open `Skill Studio` and inspect learned-topic outputs if they already exist

## First Useful Prompts

### Desktop automation

```text
open excel app then type "weekly review ready" then press enter
```

### Research and artifact generation

```text
Find 10 healthcare analytics and AI leadership roles and build a ranked tracker with fit notes.
```

### Topic Mastery

```text
Learn how to build a Power BI KPI dashboard from this YouTube tutorial and create a reusable skill.
```

### Deep workbench

```text
Create a new VS Code workspace for this task, write analysis code, add smoke tests, and leave me a runnable scaffold.
```

## Where To Go Next

- [UI Guide](UI_GUIDE.md)
- [CLI Guide](CLI_GUIDE.md)
- [Teach Mode Guide](TEACH_MODE_GUIDE.md)
- [Topic Mastery Guide](TOPIC_MASTERY_GUIDE.md)
