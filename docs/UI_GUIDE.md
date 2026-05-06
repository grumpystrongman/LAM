# UI Guide

![OpenLAMb command center](assets/ui-command-center.png)

## Product Layout

OpenLAMb is organized around four working surfaces:

1. `Command Center`
   Write the goal, not the click sequence.
2. `Operator Conversation`
   Watch progress, narration, and package status.
3. `Canvas`
   Inspect artifacts, runtime context, and developer details.
4. `Learned Skill Studio`
   Manage topic-learned skills, practice, scheduling, and refresh.

## Sidebar

The left rail is for high-frequency navigation:
- Command Center
- Conversation
- Teach Mode
- Skill Studio
- Canvas
- History

On compact/mobile layouts, the sidebar collapses automatically.

## Command Center

Use this panel for:
- ad hoc instructions
- preview vs run
- clipboard capture
- quick templates
- progress monitoring

Prompting rule:
- describe the business outcome you want
- avoid writing brittle click-by-click steps unless that is intentional

## Workspace Hero

The hero section gives immediate product context:
- current mode
- artifact count
- learned skill count
- scheduler state

This section exists so the UI feels like an operating console, not a blank chat box.

## Operator Conversation

This is the primary progress surface.
Use it to understand:
- what is happening now
- what was produced
- whether the run is paused, partial, blocked, or complete

## Canvas

Canvas is where you inspect:
- live pages
- artifacts
- runtime timeline
- platform cards
- developer details

Desktop:
- Canvas opens as a right-side work surface.

Compact/mobile:
- Canvas does not auto-open so the main workflow stays usable.

## Learned Skill Studio

Skill Studio is for reusable knowledge, not ad hoc tasks.

Use it to:
- load a learned skill
- diff versions
- preview or run safe practice
- schedule practice
- refresh a topic-sensitive skill
- edit skill metadata and JSON

## Teach Mode

Teach Mode is for learning workflows from demonstration.

Use it when:
- the workflow is repeated often
- a deterministic desktop/browser path is desirable
- you want OpenLAMb to reuse a human-demonstrated process

See [Teach Mode Guide](TEACH_MODE_GUIDE.md).

## Vault and Credentials

The local vault is designed for practical use without cloud sync.

Important behavior:
- vault listing degrades gracefully if one entry cannot be decrypted in the current session
- autofill still requires explicit control grant
- risky follow-up actions remain confirmation-gated

## Good UI Habits

- click `Accept Control` only when you intend to delegate
- use `Preview` before high-impact tasks
- open Canvas when you want to inspect evidence or artifacts
- use Skill Studio for durable capabilities, not one-off exploration
- use Topic Mastery before asking the system to perform unfamiliar expert work
