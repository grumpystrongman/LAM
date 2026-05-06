from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List

from lam.interface.learned_recipe import RecipeCritic, RecipeMemory, build_learned_recipe, recipe_to_instruction
from lam.interface.teach_runtime import DemonstrationSegmenter, ScreenObservationStream, TeachReplayRuntime


@dataclass(slots=True)
class TeachEvent:
    ts: float
    action: str
    payload: Dict[str, Any]


@dataclass(slots=True)
class TeachRecorder:
    recording: bool = False
    app_name: str = ""
    events: List[TeachEvent] = field(default_factory=list)
    compression_window_seconds: float = 1.0
    compression_mode: str = "normal"
    recipe_memory: RecipeMemory = field(default_factory=RecipeMemory)
    observation_stream: ScreenObservationStream = field(default_factory=ScreenObservationStream)
    segmenter: DemonstrationSegmenter = field(default_factory=DemonstrationSegmenter)
    replay_runtime: TeachReplayRuntime = field(default_factory=TeachReplayRuntime)
    last_result: Dict[str, Any] = field(default_factory=dict)

    def start(self, app_name: str) -> Dict[str, Any]:
        self.recording = True
        self.app_name = (app_name or "").strip().lower()
        self.events = []
        self.last_result = {}
        return {"ok": True, "recording": self.recording, "app_name": self.app_name}

    def stop(self) -> Dict[str, Any]:
        self.recording = False
        compressed = self._compress_events(self.events)
        instruction = self._to_instruction(compressed)
        observation_frames = self.observation_stream.build(app_name=self.app_name, compressed_events=compressed)
        observation_segments = self.segmenter.segment(observation_frames)
        recipe = build_learned_recipe(
            self.app_name,
            compressed,
            observation_frames=observation_frames,
            observation_segments=observation_segments,
        )
        recipe_critic = RecipeCritic().evaluate(recipe)
        recipe_path = self.recipe_memory.save(recipe)
        replay_plan = self.replay_runtime.build_plan(recipe=recipe)
        result = {
            "ok": True,
            "recording": self.recording,
            "app_name": self.app_name,
            "instruction": instruction,
            "adaptive_instruction": recipe_to_instruction(recipe),
            "events": [self._event_to_dict(e) for e in self.events],
            "compressed_events": compressed,
            "observation_frames": observation_frames,
            "observation_segments": observation_segments,
            "step_count": len(compressed),
            "raw_event_count": len(self.events),
            "compression_mode": self.compression_mode,
            "learned_recipe": recipe.to_dict(),
            "recipe_critic": recipe_critic.to_dict(),
            "recipe_path": recipe_path,
            "replay_plan": replay_plan,
        }
        self.last_result = result
        return result

    def set_compression_mode(self, mode: str) -> Dict[str, Any]:
        value = (mode or "normal").strip().lower()
        if value not in {"aggressive", "normal", "strict"}:
            value = "normal"
        self.compression_mode = value
        return {"ok": True, "compression_mode": self.compression_mode}

    def capture_click(self, selector: Dict[str, Any]) -> Dict[str, Any]:
        return self._push("click", {"selector": selector})

    def capture_type(self, text: str) -> Dict[str, Any]:
        return self._push("type_text", {"text": text})

    def capture_hotkey(self, keys: str) -> Dict[str, Any]:
        return self._push("hotkey", {"keys": keys})

    def capture_wait(self, seconds: int) -> Dict[str, Any]:
        return self._push("wait", {"seconds": max(1, int(seconds))})

    def state(self) -> Dict[str, Any]:
        recipe = dict(self.last_result.get("learned_recipe", {}) or {})
        critic = dict(self.last_result.get("recipe_critic", {}) or {})
        replay = dict(self.last_result.get("replay_plan", {}) or {})
        return {
            "active": self.recording,
            "recording": self.recording,
            "app_name": self.app_name,
            "event_count": len(self.events),
            "events": [self._event_to_dict(e) for e in self.events],
            "last_recipe": {
                "recipe_id": recipe.get("recipe_id", ""),
                "learned_goal": recipe.get("learned_goal", ""),
                "confidence": recipe.get("confidence", 0.0),
                "required_inputs": list(recipe.get("required_inputs", []) or []),
                "success_signals": list(recipe.get("success_signals", []) or []),
                "state_snapshots": list(recipe.get("state_snapshots", []) or []),
                "critic_passed": bool(critic.get("passed", False)),
                "critic_score": critic.get("score", 0.0),
                "autorun_ready": bool(replay.get("can_autorun", False)),
                "missing_inputs": list(replay.get("missing_inputs", []) or []),
                "recipe_path": self.last_result.get("recipe_path", ""),
            },
            "last_segments": list(self.last_result.get("observation_segments", []) or []),
        }

    def _push(self, action: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self.recording:
            return {"ok": False, "error": "Teach recorder is not active."}
        event = TeachEvent(ts=time.time(), action=action, payload=payload)
        self.events.append(event)
        return {"ok": True, "event": self._event_to_dict(event), "count": len(self.events)}

    def _to_instruction(self, events: List[Dict[str, Any]]) -> str:
        parts: List[str] = []
        if self.app_name:
            parts.append(f"open {self.app_name} app")
        for event in events:
            action = event.get("action", "")
            payload = event.get("payload", {})
            if action == "click":
                sel = payload.get("selector", {})
                label = (
                    sel.get("metadata", {}).get("name")
                    or sel.get("metadata", {}).get("automation_id")
                    or sel.get("value")
                    or "captured element"
                )
                parts.append(f"click {label}")
            elif action == "type_text":
                parts.append(f"type \"{payload.get('text', '')}\"")
            elif action == "hotkey":
                parts.append(f"press {payload.get('keys', '')}")
            elif action == "wait":
                parts.append(f"wait {payload.get('seconds', 1)} seconds")
        return " then ".join(parts)

    @staticmethod
    def _event_to_dict(event: TeachEvent) -> Dict[str, Any]:
        return {"ts": event.ts, "action": event.action, "payload": event.payload}

    def _compress_events(self, events: List[TeachEvent]) -> List[Dict[str, Any]]:
        """Merge noisy rapid-fire teach events into cleaner reusable steps."""
        if not events:
            return []
        mode = self.compression_mode
        typing_window = 2.0 if mode == "aggressive" else 1.0 if mode == "normal" else 0.35
        click_window = 1.25 if mode == "aggressive" else 0.8 if mode == "normal" else 0.25
        hotkey_window = 1.0 if mode == "aggressive" else 0.8 if mode == "normal" else 0.25
        wait_window = 2.0 if mode == "aggressive" else 1.5 if mode == "normal" else 0.5
        out: List[Dict[str, Any]] = []

        for event in events:
            item = self._event_to_dict(event)
            if not out:
                out.append(item)
                continue

            prev = out[-1]
            dt = float(item["ts"]) - float(prev["ts"])
            if dt < 0:
                dt = 0.0

            # Merge adjacent type bursts into one type action.
            if prev["action"] == "type_text" and item["action"] == "type_text" and dt <= typing_window:
                prev_text = str(prev["payload"].get("text", ""))
                next_text = str(item["payload"].get("text", ""))
                if prev_text and next_text:
                    prev["payload"]["text"] = prev_text + next_text
                elif next_text:
                    prev["payload"]["text"] = next_text
                prev["ts"] = item["ts"]
                continue

            # Collapse rapid duplicate clicks on same selector.
            if prev["action"] == "click" and item["action"] == "click" and dt <= click_window:
                prev_sel = prev["payload"].get("selector", {})
                next_sel = item["payload"].get("selector", {})
                if self._same_selector(prev_sel, next_sel):
                    prev["ts"] = item["ts"]
                    continue

            # Collapse repeated same hotkey in burst (keep one).
            if prev["action"] == "hotkey" and item["action"] == "hotkey" and dt <= hotkey_window:
                if str(prev["payload"].get("keys", "")).lower() == str(item["payload"].get("keys", "")).lower():
                    prev["ts"] = item["ts"]
                    continue

            # Merge adjacent waits into one longer wait.
            if prev["action"] == "wait" and item["action"] == "wait" and dt <= wait_window:
                prev_seconds = int(prev["payload"].get("seconds", 0))
                next_seconds = int(item["payload"].get("seconds", 0))
                prev["payload"]["seconds"] = max(1, prev_seconds + next_seconds)
                prev["ts"] = item["ts"]
                continue

            if mode == "aggressive" and prev["action"] == "wait" and item["action"] == "type_text" and int(prev["payload"].get("seconds", 1)) <= 1:
                # Drop tiny waits before typing in aggressive mode.
                out[-1] = item
                continue

            out.append(item)

        return out

    @staticmethod
    def _same_selector(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
        if not isinstance(a, dict) or not isinstance(b, dict):
            return False
        va = str(a.get("value", "")).strip().lower()
        vb = str(b.get("value", "")).strip().lower()
        if va and vb and va == vb:
            return True
        ma = a.get("metadata", {}) if isinstance(a.get("metadata", {}), dict) else {}
        mb = b.get("metadata", {}) if isinstance(b.get("metadata", {}), dict) else {}
        for key in ("automation_id", "name", "class_name"):
            xa = str(ma.get(key, "")).strip().lower()
            xb = str(mb.get(key, "")).strip().lower()
            if xa and xb and xa == xb:
                return True
        return False
