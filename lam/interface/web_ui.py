from __future__ import annotations

import html as html_lib
import json
import mimetypes
import threading
import time
import uuid
import webbrowser
from dataclasses import dataclass, field
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import parse_qs, quote, urlparse

import yaml

from lam.adapters.uia_adapter import UIAAdapter
from lam.interface.ai_backend import AI_BACKENDS, normalize_backend
from lam.interface.app_launcher import list_installed_apps, open_installed_app
from lam.interface.global_teach_hooks import GlobalTeachHooks
from lam.interface.scheduler import ScheduleEngine, ScheduleJob
from lam.interface.search_agent import (
    execute_instruction,
    focus_auth_session,
    preview_instruction,
    resume_pending_plan,
)
from lam.interface.selector_picker import capture_selector_at_cursor
from lam.interface.teach_recorder import TeachRecorder
from lam.interface.user_defaults import current_user, load_defaults, save_defaults
from lam.interface.password_vault import LocalPasswordVault
from lam.interface.browser_worker import normalize_browser_worker_mode
from lam.interface.human_operator_benchmark import benchmark_from_last_run
from lam.interface.human_operator_scenario_runner import (
    run_human_operator_20_suite,
    run_human_operator_killer_suite,
)
from lam.interface.reliability_suite import run_reliability_suite
from lam.interface.world_model import build_ui_world_model


@dataclass(slots=True)
class UiState:
    control_granted: bool = False
    control_granted_at: float = 0.0
    paused_for_credentials: bool = False
    pause_reason: str = ""
    pending_plan: Dict[str, Any] = field(default_factory=dict)
    pending_auth_instruction: str = ""
    pending_auth_url: str = ""
    pending_auth_session_id: str = ""
    step_mode: bool = False
    manual_auth_phase: bool = True
    browser_worker_mode: str = "local"
    human_like_interaction: bool = True
    ai_backend: str = "deterministic-local"
    compression_mode: str = "normal"
    min_live_non_curated_citations: int = 3
    artifact_reuse_mode: str = "reuse_if_recent"
    artifact_reuse_max_age_hours: int = 72
    use_domain_freshness_defaults: bool = True
    user_id: str = field(default_factory=current_user)
    saved_automations: Dict[str, str] = field(default_factory=dict)
    history: List[Dict[str, Any]] = field(default_factory=list)
    recorder: TeachRecorder = field(default_factory=TeachRecorder)
    global_hooks: GlobalTeachHooks | None = None
    last_selector: Dict[str, Any] = field(default_factory=dict)
    scheduler: ScheduleEngine | None = None
    vault: LocalPasswordVault = field(default_factory=LocalPasswordVault)
    tasks: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    current_task_id: str = ""
    auth_loop_signature: str = ""
    auth_loop_count: int = 0
    auth_loop_blocked: bool = False
    reliability_suite_task_id: str = ""
    reliability_suite_result: Dict[str, Any] = field(default_factory=dict)
    benchmark_last_result: Dict[str, Any] = field(default_factory=dict)
    human_suite_task_id: str = ""
    human_suite_result: Dict[str, Any] = field(default_factory=dict)
    killer_suite_task_id: str = ""
    killer_suite_result: Dict[str, Any] = field(default_factory=dict)
    preflight_required: bool = True
    lock: threading.Lock = field(default_factory=threading.Lock)

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            schedules = self.scheduler.list_jobs() if self.scheduler else []
            schedule_history = self.scheduler.list_history(limit=50) if self.scheduler else []
            preflight = _preflight_status_locked(self)
            current_task = dict(self.tasks.get(self.current_task_id, {})) if self.current_task_id else {}
            auth_recovery = _build_auth_recovery_recommendation_locked(
                state=self,
                current_task=current_task,
            )
            world_model = build_ui_world_model(
                control_granted=self.control_granted,
                paused_for_credentials=self.paused_for_credentials,
                pause_reason=self.pause_reason,
                pending_auth_url=self.pending_auth_url,
                pending_auth_session_id=self.pending_auth_session_id,
                current_task=current_task,
                history=self.history,
                preflight=preflight,
            )
            return {
                "control_granted": self.control_granted,
                "control_granted_at": self.control_granted_at,
                "paused_for_credentials": self.paused_for_credentials,
                "pause_reason": self.pause_reason,
                "has_pending_plan": bool(self.pending_plan),
                "has_pending_auth": bool(self.pending_auth_instruction),
                "pending_auth_url": self.pending_auth_url,
                "pending_auth_session_id": self.pending_auth_session_id,
                "step_mode": self.step_mode,
                "manual_auth_phase": self.manual_auth_phase,
                "browser_worker_mode": self.browser_worker_mode,
                "human_like_interaction": self.human_like_interaction,
                "ai_backend": self.ai_backend,
                "compression_mode": self.compression_mode,
                "min_live_non_curated_citations": self.min_live_non_curated_citations,
                "artifact_reuse_mode": self.artifact_reuse_mode,
                "artifact_reuse_max_age_hours": self.artifact_reuse_max_age_hours,
                "use_domain_freshness_defaults": self.use_domain_freshness_defaults,
                "freshness_policy": _load_policy_freshness_defaults(),
                "ai_backends": AI_BACKENDS,
                "user_id": self.user_id,
                "saved_automations": dict(self.saved_automations),
                "history": list(self.history),
                "teach": self.recorder.state(),
                "global_teach_active": bool(self.global_hooks.active) if self.global_hooks else False,
                "last_selector": dict(self.last_selector),
                "schedules": schedules,
                "schedule_history": schedule_history,
                "vault_status": self.vault.status(),
                "current_task_id": self.current_task_id,
                "task": current_task,
                "auth_loop_count": self.auth_loop_count,
                "auth_loop_blocked": self.auth_loop_blocked,
                "auth_loop_signature": self.auth_loop_signature,
                "auth_recovery": auth_recovery,
                "reliability_suite_task_id": self.reliability_suite_task_id,
                "reliability_suite_result": dict(self.reliability_suite_result),
                "benchmark_last_result": dict(self.benchmark_last_result),
                "human_suite_task_id": self.human_suite_task_id,
                "human_suite_result": dict(self.human_suite_result),
                "killer_suite_task_id": self.killer_suite_task_id,
                "killer_suite_result": dict(self.killer_suite_result),
                "preflight_required": self.preflight_required,
                "preflight": preflight,
                "world_model": world_model,
            }


HTML_PAGE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>LAM Windows Interface</title>
  <style>
    :root { --bg:#f6f7fb; --panel:#fff; --ink:#14212b; --accent:#0f766e; --muted:#6b7280; --warn:#b45309; }
    body { margin:0; background:var(--bg); color:var(--ink); font-family:"Segoe UI",Tahoma,sans-serif; }
    .wrap { display:grid; grid-template-columns:360px 1fr; height:100vh; }
    .side { background:#0f172a; color:#d1d5db; padding:14px; overflow:auto; }
    .brand-wrap { display:flex; align-items:center; gap:8px; }
    .brand-logo {
      width:22px;
      height:22px;
      border-radius:7px;
      object-fit:cover;
      border:1px solid #dbe3ef;
      box-shadow:0 1px 3px rgba(15,23,42,0.08);
      background:#fff;
    }
    .brand { font-size:18px; font-weight:700; margin-bottom:0; }
    .status { background:#111827; border:1px solid #1f2937; padding:10px; border-radius:10px; margin-bottom:12px; }
    @keyframes slowflash { 0% { opacity:1; } 50% { opacity:0.45; } 100% { opacity:1; } }
    .auth-alert { display:none; background:#7f1d1d; border:1px solid #ef4444; color:#fee2e2; padding:10px; border-radius:10px; margin-bottom:12px; animation: slowflash 2s ease-in-out infinite; }
    .history-item { border:1px solid #1f2937; border-radius:10px; margin-bottom:8px; padding:8px; cursor:pointer; }
    .history-item:hover { background:#1f2937; }
    .main { padding:18px; display:flex; flex-direction:column; gap:12px; overflow:auto; }
    .panel { background:var(--panel); border:1px solid #e5e7eb; border-radius:12px; padding:12px; }
    .row { display:flex; gap:8px; align-items:center; flex-wrap:wrap; margin-top:6px; }
    input[type=text], input[type=number], select { min-width:180px; font-size:14px; padding:9px; border:1px solid #d1d5db; border-radius:10px; }
    input.wide { flex:1; min-width:280px; }
    button { border:0; border-radius:10px; padding:9px 12px; cursor:pointer; font-weight:600; }
    .primary { background:var(--accent); color:white; }
    .warn { background:#fef3c7; color:#92400e; border:1px solid #fcd34d; }
    .summary-head { font-weight:700; font-size:18px; color:#0f172a; }
    .summary-sub { color:#475569; font-size:14px; margin-top:2px; }
    .summary-grid { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:10px; margin-top:10px; }
    .summary-card { border:1px solid #e2e8f0; border-radius:10px; background:#ffffff; padding:10px; }
    .summary-card .t { font-size:13px; color:#0f172a; font-weight:600; }
    .summary-card .m { font-size:12px; color:#64748b; margin-top:4px; word-break:break-word; }
    .badge { display:inline-block; padding:2px 8px; border-radius:999px; font-size:11px; font-weight:600; border:1px solid #cbd5e1; background:#eef2ff; color:#334155; }
    .summary-card .badge { margin-top:6px; }
    .badge.validated, .badge.revised_validated { background:#ecfdf5; color:#166534; border-color:#bbf7d0; }
    .badge.pending_critic_review, .badge.revision_required { background:#fff7ed; color:#9a3412; border-color:#fed7aa; }
    .badge.failed_review { background:#fef2f2; color:#991b1b; border-color:#fecaca; }
    .artifact-chip-list { display:flex; flex-wrap:wrap; gap:8px; margin-top:8px; }
    .artifact-chip { border:1px solid #e2e8f0; border-radius:12px; padding:8px 10px; background:#fff; min-width:180px; }
    .artifact-chip-head { display:flex; align-items:center; gap:6px; flex-wrap:wrap; }
    .artifact-chip-title { font-size:12px; font-weight:600; color:#0f172a; word-break:break-word; }
    .artifact-chip-actions { display:flex; align-items:center; gap:8px; margin-top:6px; }
    .artifact-open { font-size:12px; color:#0f766e; text-decoration:none; font-weight:600; }
    .timeline-filter-btn.active { background:#dbeafe; color:#1d4ed8; border:1px solid #93c5fd; }
    .artifact-list a { color:#0f766e; text-decoration:none; }
    .json-box { white-space:pre-wrap; max-height:260px; overflow:auto; font-size:12px; color:#0f172a; background:#f8fafc; border:1px solid #e2e8f0; border-radius:10px; padding:10px; }
    .small { color:var(--muted); font-size:12px; }
    .grid2 { display:grid; grid-template-columns:1fr 1fr; gap:12px; }
    .mono { font-family:Consolas,Menlo,monospace; font-size:12px; }
    .progress-wrap { background:#e5e7eb; border-radius:999px; height:14px; overflow:hidden; }
    .progress-bar { height:100%; width:0%; background:linear-gradient(90deg,#0f766e,#6366f1); transition:width .2s ease; }
    .progress-log { max-height:140px; overflow:auto; font-size:12px; background:#f8fafc; border:1px solid #e2e8f0; border-radius:10px; padding:8px; }
    .strict-wrap { margin-top:8px; border:1px solid #e2e8f0; border-radius:10px; background:#f8fafc; padding:8px; }
    .strict-step { border:1px solid #e2e8f0; border-radius:8px; background:#fff; padding:8px; margin-top:6px; }
    .strict-rule { display:inline-block; border-radius:999px; padding:2px 8px; font-size:11px; margin-right:6px; margin-top:4px; background:#fee2e2; color:#991b1b; border:1px solid #fecaca; }
    .work-canvas-frame { width:100%; height:420px; border:1px solid #e2e8f0; border-radius:10px; background:#fff; }
    .work-canvas-note { margin-top:6px; margin-bottom:8px; }
    .assistant-feed { max-height:260px; overflow:auto; display:flex; flex-direction:column; gap:8px; background:#f8fafc; border:1px solid #e2e8f0; border-radius:10px; padding:10px; }
    .assistant-msg { max-width:90%; border-radius:12px; padding:8px 10px; font-size:13px; line-height:1.35; }
    .assistant-msg.agent { align-self:flex-start; background:#ffffff; border:1px solid #e2e8f0; color:#0f172a; }
    .assistant-msg.user { align-self:flex-end; background:#0f766e; color:#ffffff; border:1px solid #0f766e; }
    .assistant-msg.meta { align-self:center; background:#e2e8f0; color:#334155; border:1px solid #cbd5e1; font-size:12px; }
    .auth-recovery { display:none; border:1px solid #fecaca; background:#fff7ed; border-radius:10px; padding:10px; }
    .auth-recovery .code { font-family:Consolas,Menlo,monospace; font-size:12px; color:#7f1d1d; background:#fee2e2; border:1px solid #fecaca; border-radius:8px; padding:3px 8px; }
    .auth-recovery .hint { font-size:12px; color:#7c2d12; margin-top:6px; }

    /* Chat-first product layout overrides */
    body { background:#f4f6fb; }
    .wrap { grid-template-columns:260px 1fr; transition:grid-template-columns .18s ease; }
    body.sidebar-compact .wrap { grid-template-columns:72px 1fr; }
    .side { background:#f8fafc; color:#0f172a; border-right:1px solid #e2e8f0; }
    body.sidebar-compact .side .brand-label,
    body.sidebar-compact .side #history,
    body.sidebar-compact .side .small { display:none; }
    .status { background:#fff; border:1px solid #e2e8f0; color:#334155; }
    .history-item { border:1px solid #e2e8f0; background:#fff; }
    .history-item:hover { background:#eff6ff; }
    .main { padding:14px; display:grid; grid-template-rows:auto auto 1fr; gap:10px; min-height:100vh; }
    #chatControlPanel .advanced-control { display:none; }
    #chatControlPanel { padding:10px; }
    #chatControlPanel .chat-topbar { margin-top:0; justify-content:space-between; }
    #chatControlPanel .chat-composer-row input.wide { min-width:0; width:100%; }
    #chatControlPanel .chat-progress-log { display:none; }
    #chatPanel { display:flex; flex-direction:column; min-height:0; }
    #chatPanel .assistant-feed { flex:1; max-height:none; background:#fff; border:1px solid #e2e8f0; }
    .main > #worldPanel, .main > #opsPanel, .main > #vaultPanel, .main > #runSummaryPanel, .main > #teachSchedulePanel { display:none; }
    .canvas-toggle { background:#2563eb; color:#fff; border:1px solid #1d4ed8; }
    #canvasPanel {
      position:fixed;
      top:0;
      right:-540px;
      width:540px;
      height:100vh;
      z-index:30;
      border-left:1px solid #e2e8f0;
      box-shadow:0 12px 40px rgba(15,23,42,0.16);
      background:#fff;
      transition:right .2s ease;
      overflow:auto;
      padding-bottom:18px;
    }
    body.canvas-open #canvasPanel { right:0; }
    #canvasPanel .canvas-header { position:sticky; top:0; background:#fff; z-index:2; border-bottom:1px solid #e2e8f0; padding-bottom:8px; }
    #canvasPanel .canvas-section { margin-top:10px; }
    .timeline-group { border:1px solid #e2e8f0; border-radius:10px; background:#fff; margin-bottom:8px; }
    .timeline-group summary { cursor:pointer; padding:10px; font-weight:600; color:#0f172a; }
    .timeline-group-body { padding:0 10px 10px 10px; }
    .timeline-event { font-size:12px; color:#475569; margin-top:4px; }
    .timeline-critic { font-size:12px; color:#334155; margin-top:6px; padding-top:6px; border-top:1px dashed #e2e8f0; }
    #canvasPanel .canvas-debug details { margin-top:8px; }
    #developerDetailsMount .panel { display:none; }
    #canvasPanel details[open] #developerDetailsMount .panel { display:block; margin-top:10px; }
    .feedback-row { display:flex; gap:6px; flex-wrap:wrap; margin-top:8px; }
    .feedback-row button { border:1px solid #e2e8f0; background:#fff; color:#334155; border-radius:999px; padding:4px 10px; font-size:12px; }
    @media (max-width: 1180px) {
      .wrap { grid-template-columns:1fr; }
      .side { max-height:30vh; }
      #canvasPanel { width:100%; right:-100%; }
    }
  </style>
</head>
<body>
<div class="wrap">
  <aside class="side">
    <div class="row" style="margin-top:0;justify-content:space-between;">
      <div class="brand-wrap">
        <img class="brand-logo" src="/assets/openlamb-logo.png" alt="OpenLAMb logo"/>
        <div class="brand brand-label">OpenLAMb</div>
      </div>
      <button onclick="toggleSidebarCompact()">Menu</button>
    </div>
    <div class="status" id="statusBox">Control: not granted</div>
    <div class="auth-alert" id="authAlert"></div>
    <div class="small">History</div>
    <div id="history"></div>
  </aside>
  <main class="main">
    <div class="panel" id="chatControlPanel">
      <div class="row chat-topbar">
        <div class="row" style="margin-top:0;">
          <button class="primary" onclick="grantControl()">Accept Control</button>
          <button class="warn" onclick="revokeControl()">Revoke</button>
          <button class="canvas-toggle" onclick="toggleCanvas(true)">Open Canvas</button>
          <button onclick="newTaskFromUI()">New Task</button>
        </div>
        <div class="small">Delegate work in chat. Advanced controls are in Developer Details.</div>
      </div>
      <div class="row advanced-control">
        <button class="primary" onclick="grantControl()">Accept Control</button>
        <button class="warn" onclick="revokeControl()">Revoke Control</button>
        <button onclick="resumeAfterLogin()">Resume</button>
        <button onclick="resetSessionState()">Reset Session</button>
        <label class="small"><input type="checkbox" id="stepMode" onchange="setStepMode(this.checked)"/> Step mode</label>
        <label class="small"><input type="checkbox" id="manualAuthPhase" onchange="setManualAuthPhase(this.checked)" checked/> Manual auth phase</label>
        <select id="browserWorkerMode" onchange="setBrowserWorkerMode(this.value)">
          <option value="local">browser worker: local</option>
          <option value="docker">browser worker: docker</option>
        </select>
        <label class="small"><input type="checkbox" id="humanLikeInteraction" onchange="setHumanLikeInteraction(this.checked)"/> Human-like interaction</label>
        <select id="aiBackend" onchange="setAiBackend(this.value)">
          <option value="deterministic-local">deterministic-local</option>
          <option value="openai-gpt-5.4">openai-gpt-5.4</option>
          <option value="openai-gpt-5.4-mini">openai-gpt-5.4-mini</option>
          <option value="openai-gpt-5.4-nano">openai-gpt-5.4-nano</option>
        </select>
        <select id="compressionMode" onchange="setCompressionMode(this.value)">
          <option value="aggressive">compression: aggressive</option>
          <option value="normal" selected>compression: normal</option>
          <option value="strict">compression: strict</option>
        </select>
        <label class="small">min live cites:
          <input id="minLiveCites" type="number" min="1" max="20" value="3" style="width:80px" onchange="setMinLiveCites(this.value)"/>
        </label>
        <select id="artifactReuseMode" onchange="setArtifactReuseMode(this.value)">
          <option value="reuse">reuse outputs</option>
          <option value="reuse_if_recent" selected>reuse if recent</option>
          <option value="always_regenerate">always regenerate</option>
        </select>
        <label class="small">reuse hrs:
          <input id="artifactReuseMaxAgeHours" type="number" min="1" max="720" value="72" style="width:90px" onchange="setArtifactReuseMaxAgeHours(this.value)"/>
        </label>
        <label class="small"><input type="checkbox" id="useDomainFreshnessDefaults" onchange="setUseDomainFreshnessDefaults(this.checked)" checked/> use domain freshness defaults</label>
      </div>
      <div class="row advanced-control">
        <select id="freshnessPolicyDomain" onchange="loadDomainFreshnessPolicy()">
          <option value="web_research">web_research</option>
          <option value="job_market">job_market</option>
          <option value="competitor_analysis">competitor_analysis</option>
          <option value="study_pack">study_pack</option>
          <option value="payer_pricing_review">payer_pricing_review</option>
          <option value="code_workbench">code_workbench</option>
          <option value="artifact_generation">artifact_generation</option>
          <option value="email_triage">email_triage</option>
          <option value="desktop_sequence">desktop_sequence</option>
        </select>
        <select id="freshnessPolicyMode">
          <option value="reuse">reuse</option>
          <option value="reuse_if_recent">reuse_if_recent</option>
          <option value="always_regenerate">always_regenerate</option>
        </select>
        <label class="small">domain hrs:
          <input id="freshnessPolicyHours" type="number" min="1" max="720" value="72" style="width:90px"/>
        </label>
        <button onclick="saveDomainFreshnessPolicy()">Save Domain Policy</button>
        <button onclick="loadDomainFreshnessPolicy()">Reload Domain Policy</button>
      </div>
      <div class="row chat-composer-row">
        <input id="instruction" class="wide" type="text" placeholder="Example: Review Durham payer pricing, build a RAG index, create the stakeholder workbook, and show me which plans need outreach."/>
        <button class="primary" onclick="runInstruction()">Run</button>
        <button onclick="previewInstruction()">Preview</button>
        <button onclick="captureClipboardImageUi()">Capture Clipboard Image</button>
      </div>
      <div class="small advanced-control" id="freshnessPreview">Freshness preview: waiting for instruction.</div>
      <div class="row advanced-control">
        <input id="automationName" type="text" placeholder="Automation name"/>
        <button onclick="saveAutomation()">Save</button>
        <button onclick="runAutomation()">Run Saved</button>
        <button onclick="exportHistory()">Export History</button>
        <button class="warn" onclick="clearHistory()">Clear History</button>
      </div>
      <div class="row advanced-control">
        <button onclick="useTemplate('open chatgpt app then click New chat then type \\'Daily summary\\' then press enter')">Template: ChatGPT Daily</button>
        <button onclick="useTemplate('search Amazon for best price on Abu Garcia Voltiq baitcasting reel')">Template: Amazon Price</button>
        <button onclick="useTemplate('Review Durham, NC payer pricing, build a RAG index, create the stakeholder workbook, and show me which plans need outreach.')">Template: Durham Payer Review</button>
        <button onclick="useTemplate('Create a new VS Code workspace for this task, write analysis code, add smoke tests, and leave me a runnable scaffold with notes.')">Template: Code Workbench</button>
      </div>
      <div class="row advanced-control">
        <input id="appSearch" type="text" placeholder="Search installed apps"/>
        <button onclick="searchApps()">Find Apps</button>
      </div>
      <div class="row chat-progress-head"><strong>Progress</strong> <span class="small" id="progressLabel">Idle</span></div>
      <div class="progress-wrap"><div id="progressBar" class="progress-bar"></div></div>
      <div class="progress-log mono chat-progress-log" id="progressLog">No active task.</div>
    </div>

    <div class="panel auth-recovery" id="authRecoveryPanel">
      <div class="row" style="justify-content:space-between;">
        <div><strong>Auth Recovery Wizard</strong></div>
        <span class="code" id="authRecoveryCode">no_error</span>
      </div>
      <div class="small" id="authRecoveryModeLine">Current worker: local</div>
      <div class="small" id="authRecoveryRecommendLine" style="margin-top:4px;">Recommendation pending.</div>
      <div class="hint" id="authRecoveryReason">No auth issue detected.</div>
      <div class="row" style="margin-top:8px;">
        <button id="authRecoveryApplyBtn" onclick="applyAuthRecoveryRecommendation()">Apply Recommended Mode</button>
        <button onclick="focusAuthTarget()">Focus Auth Tab</button>
        <button onclick="resumeAfterLogin()">Resume</button>
        <button class="warn" onclick="resetSessionState()">Reset Session</button>
      </div>
    </div>

    <div class="panel" id="chatPanel">
      <div class="row" style="justify-content:space-between;">
        <div><strong>Assistant Feed</strong></div>
        <div class="small">Live narration + final status</div>
      </div>
      <div class="assistant-feed" id="assistantFeed">
        <div class="assistant-msg meta">Waiting for a task.</div>
      </div>
    </div>

    <div class="panel" id="worldPanel">
      <div><strong>World Model</strong></div>
      <div class="small" id="worldModelNarration">Environment narration will appear here.</div>
      <div class="small" id="worldModelNoticed" style="margin-top:6px;">What I noticed will appear here.</div>
      <div class="json-box" id="worldModelBox">No world model yet.</div>
    </div>

    <div class="panel" id="canvasPanel">
      <div class="row" style="justify-content:space-between;">
        <div class="canvas-header"><strong>Canvas / Workbench</strong></div>
        <div class="row" style="margin-top:0;">
          <a id="workCanvasOpen" href="" target="_blank" rel="noopener" style="visibility:hidden;">Open In Tab</a>
          <button onclick="toggleCanvas(false)">Close</button>
        </div>
      </div>
      <div class="canvas-section">
        <div class="small work-canvas-note" id="workCanvasNote">No active page.</div>
        <iframe id="workCanvasFrame" class="work-canvas-frame" src="about:blank" title="Live work canvas"></iframe>
      </div>
      <div class="canvas-section" id="artifactCanvasSection">
        <div class="small">Artifacts</div>
        <div class="artifact-list" id="artifactListCanvas">No artifacts yet.</div>
      </div>
      <div class="canvas-section" id="platformCanvasSection">
        <div class="small">Task Context</div>
        <div class="summary-grid" id="platformCardsCanvas"></div>
      </div>
      <div class="canvas-section" id="timelineCanvasSection">
        <div class="row" style="justify-content:space-between;">
          <div class="small">Runtime Timeline</div>
          <div class="row" style="margin-top:0;">
            <button class="timeline-filter-btn" data-filter="all" onclick="setTimelineFilter('all')">all</button>
            <button class="timeline-filter-btn" data-filter="node" onclick="setTimelineFilter('node')">node-only</button>
            <button class="timeline-filter-btn" data-filter="critic" onclick="setTimelineFilter('critic')">critic-only</button>
            <button class="timeline-filter-btn" data-filter="revision" onclick="setTimelineFilter('revision')">revisions-only</button>
          </div>
        </div>
        <div id="runtimeTimelineCanvas">No runtime timeline yet.</div>
      </div>
      <div class="canvas-section canvas-debug">
        <details>
          <summary>Developer Details</summary>
          <div id="developerDetailsMount"></div>
        </details>
      </div>
    </div>

    <div class="grid2" id="teachSchedulePanel">
      <div class="panel">
        <div><strong>Teach Recorder</strong></div>
        <div class="row">
          <input id="teachApp" type="text" placeholder="App name (e.g. chatgpt)"/>
          <button onclick="teachStart()">Start Teach</button>
          <button onclick="teachGlobalStart()">Global Hooks ON</button>
          <button onclick="teachGlobalStop()">Global Hooks OFF</button>
          <button onclick="teachStop()">Stop + Generate</button>
        </div>
        <div class="row">
          <button onclick="captureSelector()">Capture Selector @ Cursor</button>
          <button onclick="teachAddClick()">Add Click</button>
          <input id="teachTypeText" type="text" placeholder="Text to type"/>
          <button onclick="teachAddType()">Add Type</button>
        </div>
        <div class="row">
          <input id="teachHotkey" type="text" placeholder="Hotkey (e.g. ctrl+v)"/>
          <button onclick="teachAddHotkey()">Add Hotkey</button>
          <input id="teachWait" type="number" value="1" min="1" style="width:90px"/>
          <button onclick="teachAddWait()">Add Wait</button>
        </div>
        <div class="small" id="teachState">Teach idle.</div>
      </div>

      <div class="panel">
        <div><strong>Schedules / Triggers</strong></div>
        <div class="row">
          <input id="scheduleName" type="text" placeholder="Schedule name"/>
          <input id="scheduleAutomation" type="text" placeholder="Automation name"/>
        </div>
        <div class="row">
          <select id="scheduleKind">
            <option value="interval">interval</option>
            <option value="daily">daily</option>
            <option value="event">event</option>
          </select>
          <input id="scheduleValue" type="text" placeholder="value (e.g. 300 | 09:30 | on_startup)"/>
          <button onclick="addSchedule()">Add Schedule</button>
          <button onclick="triggerEvent()">Trigger Event</button>
        </div>
        <div class="small" id="scheduleState">No schedules yet.</div>
      </div>
    </div>

    <div class="panel" id="vaultPanel">
      <div><strong>Local Password Vault (Local Only)</strong></div>
      <div class="row">
        <input id="vaultService" type="text" placeholder="Service (e.g. linkedin)"/>
        <input id="vaultUsername" type="text" placeholder="Username"/>
        <input id="vaultPassword" type="text" placeholder="Password"/>
        <button onclick="vaultSave()">Save Entry</button>
        <button onclick="vaultList()">Refresh List</button>
      </div>
      <div class="row">
        <input id="vaultTags" type="text" placeholder="Tags (comma-separated)"/>
        <label class="small"><input type="checkbox" id="vaultFavorite"/> Favorite</label>
        <input id="vaultLength" type="number" value="20" min="12" max="128" style="width:90px"/>
        <button onclick="vaultGenerate()">Generate Strong Password</button>
        <button onclick="vaultFill()">Autofill Active Window</button>
      </div>
      <div class="row">
        <input id="vaultQuery" type="text" placeholder="Search vault by service"/>
        <button onclick="vaultExport()">Export Encrypted Backup</button>
        <button onclick="vaultImport()">Import Encrypted Backup</button>
      </div>
      <div class="small" id="vaultState">Vault status loading...</div>
      <div class="small" id="vaultList">No entries loaded.</div>
    </div>

    <div class="panel" id="runSummaryPanel">
      <div class="row">
        <div><strong>Run Summary</strong></div>
        <button onclick="runReliabilitySuite()">Run Reliability Suite</button>
        <button onclick="scoreLastRunBenchmark()">Score Last Run (Human Benchmark)</button>
        <button onclick="runHuman20Suite()">Run Human 20-Test Suite</button>
        <button onclick="runKiller5Suite()">Run Killer 5 Suite</button>
        <label class="small"><input type="checkbox" id="suiteIncludeDesktopSmoke"/> include desktop smoke (Notepad)</label>
        <button onclick="runNotepadSmoke()">Run Notepad Hello World Test</button>
        <label class="small"><input type="checkbox" id="suiteIncludePytest"/> include pytest</label>
        <input id="suitePytestArgs" type="text" placeholder="pytest args (optional)"/>
        <button onclick="regenerateFresh()">Regenerate Fresh</button>
        <button onclick="copyRawJson()">Copy Raw JSON</button>
        <label class="small"><input type="checkbox" id="showStrictRules" onchange="toggleStrictRules(this.checked)"/> Strict rule diagnostics</label>
        <label class="small"><input type="checkbox" id="showDetails" onchange="toggleDetails(this.checked)"/> Show technical details</label>
      </div>
      <div class="summary-head" id="summaryHead">Waiting for your instruction</div>
      <div class="summary-sub" id="summarySub">Use Preview or Run to start.</div>
      <div class="summary-grid" id="summaryCards"></div>
      <div class="summary-grid" id="platformCards"></div>
      <div class="artifact-list" id="artifactList"></div>
      <div class="progress-log" id="activityLog">No activity yet.</div>
      <div id="strictDiagWrap" class="strict-wrap" style="display:none;">
        <div class="small" style="font-weight:600;">Anti-Drift Rule Diagnostics</div>
        <div id="strictDiagBody" class="small" style="margin-top:6px;">No strict diagnostics.</div>
      </div>
      <details id="detailWrap" style="margin-top:8px;">
        <summary class="small">Raw JSON (advanced)</summary>
        <div class="json-box" id="outputRaw">No details yet.</div>
      </details>
    </div>
  </main>
</div>
<script>
const ui = { history: JSON.parse(localStorage.getItem("lam_ui_history") || "[]"), latestState: null };
let progressPollTimer = null;
let detailsVisible = false;
let strictRulesVisible = false;
let lastRaw = {};
let lastCanvasUrl = "";
let lastTaskFeedKey = "";
let timelineFilter = localStorage.getItem("lam_timeline_filter") || "all";
function persistHistory(){ localStorage.setItem("lam_ui_history", JSON.stringify(ui.history.slice(-300))); }
function toggleCanvas(forceOpen){
  const shouldOpen = (forceOpen === undefined) ? !document.body.classList.contains("canvas-open") : !!forceOpen;
  document.body.classList.toggle("canvas-open", shouldOpen);
}
function toggleSidebarCompact(){
  document.body.classList.toggle("sidebar-compact");
}
function setTimelineFilter(mode){
  timelineFilter = String(mode || "all");
  localStorage.setItem("lam_timeline_filter", timelineFilter);
  updateTimelineFilterButtons();
  renderRuntimeTimelineCanvas(lastRaw||{});
}
function updateTimelineFilterButtons(){
  document.querySelectorAll(".timeline-filter-btn").forEach(btn=>{
    const active = String(btn?.dataset?.filter || "") === timelineFilter;
    btn.classList.toggle("active", active);
  });
}
function newTaskFromUI(){
  document.getElementById("instruction").value = "";
  const feed = document.getElementById("assistantFeed");
  feed.innerHTML = `<div class="assistant-msg meta">Waiting for a task.</div>`;
  document.getElementById("progressBar").style.width = "0%";
  document.getElementById("progressLabel").innerText = "Idle";
}
function _appendAssistantFeed(html, kind){
  const feed = document.getElementById("assistantFeed");
  if(feed.children.length === 1 && (feed.textContent || "").toLowerCase().includes("waiting for a task")){
    feed.innerHTML = "";
  }
  const node = document.createElement("div");
  node.className = `assistant-msg ${kind||"agent"}`;
  node.innerHTML = html;
  feed.appendChild(node);
  feed.scrollTop = feed.scrollHeight;
}
async function submitFeedback(reason){
  try{
    await fetch("/api/feedback",{
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({
        session_id:"",
        task_id:"",
        message_id:"",
        rating:(reason==="thumbs_down"||reason==="wrong_path"||reason==="not_human_like")?-1:1,
        reason,
        comment:"",
      })
    });
  }catch(_e){}
}
function toggleDetails(v){
  detailsVisible=!!v;
  localStorage.setItem("lam_details_visible", detailsVisible ? "1":"0");
  document.getElementById("detailWrap").open = detailsVisible;
  if(detailsVisible){ document.getElementById("outputRaw").innerText=JSON.stringify(lastRaw||{},null,2); }
}
function toggleStrictRules(v){
  strictRulesVisible=!!v;
  localStorage.setItem("lam_strict_rules_visible", strictRulesVisible ? "1":"0");
  renderStrictRules(lastRaw||{});
}
function setRaw(obj){ lastRaw=obj||{}; if(detailsVisible){ document.getElementById("outputRaw").innerText=JSON.stringify(lastRaw,null,2);} }
async function copyRawJson(){
  const text = JSON.stringify(lastRaw||{}, null, 2);
  try{
    if(navigator?.clipboard?.writeText){
      await navigator.clipboard.writeText(text);
      showResponse({ok:true,mode:"copy_json",canvas:{title:"Raw JSON Copied",subtitle:"Copied run payload to clipboard.",cards:[]}})
      return;
    }
  }catch(_e){}
  const ta=document.createElement("textarea");
  ta.value=text;
  document.body.appendChild(ta);
  ta.select();
  try{ document.execCommand("copy"); }catch(_e){}
  document.body.removeChild(ta);
  showResponse({ok:true,mode:"copy_json",canvas:{title:"Raw JSON Copied",subtitle:"Copied run payload to clipboard.",cards:[]}})
}
function renderStrictRules(r){
  const wrap=document.getElementById("strictDiagWrap");
  const body=document.getElementById("strictDiagBody");
  if(!strictRulesVisible){
    wrap.style.display="none";
    return;
  }
  wrap.style.display="block";
  const diag = r?.anti_drift || {};
  const steps = Array.isArray(diag?.step_rules) ? diag.step_rules : [];
  if(!steps.length){
    body.innerHTML = "No strict diagnostics were emitted for this run.";
    return;
  }
  const failed = steps.filter(s => Array.isArray(s.failed_rules) && s.failed_rules.length > 0);
  if(!failed.length){
    body.innerHTML = `No anti-drift rule failures. Checked ${steps.length} step(s).`;
    return;
  }
  body.innerHTML = failed.map(s=>{
    const rules=(s.failed_rules||[]).map(x=>`<span class="strict-rule">${escapeHtml(x)}</span>`).join("");
    const msgs=(s.messages||[]).slice(0,4).map(m=>`<div>- ${escapeHtml(m)}</div>`).join("");
    return `<div class="strict-step"><div><strong>Step ${Number(s.step_index)+1}</strong>: ${escapeHtml(s.action||"")}</div><div class="small">${escapeHtml(s.target||"")}</div><div style="margin-top:4px;">${rules}</div><div class="small" style="margin-top:6px;">${msgs||"- No details."}</div></div>`;
  }).join("");
}
function renderSummary(r){
  const ok = !!r?.ok;
  const mode = r?.mode || "status";
  const isReliability = mode === "reliability_suite";
  const count = r?.results_count || (Array.isArray(r?.results)?r.results.length:0);
  const head = ok ? (r?.canvas?.title || "Task completed") : "Action needs attention";
  const sub = ok
    ? (r?.canvas?.subtitle || `${mode}${count?` - ${count} result(s)`:""}`)
    : (r?.error || "The action could not be completed.");
  document.getElementById("summaryHead").innerText = head;
  document.getElementById("summarySub").innerText = sub;

  const cards=[];
  if(r?.plan?.domain){ cards.push({t:"Planner",m:`${r.plan.domain} (${r.plan.steps?.length||0} steps)`}); }
  if(r?.playbook?.name){ cards.push({t:"Playbook",m:r.playbook.name}); }
  if(r?.query){ cards.push({t:"Query",m:r.query}); }
  if(count){ cards.push({t:"Results",m:String(count)}); }
  if(r?.opened_url){ cards.push({t:"Opened",m:r.opened_url}); }
  const elegance = r?.summary?.elegance_budget || r?.critics?.elegance_budget || {};
  if(typeof elegance?.remaining === "number"){
    cards.push({t:"Elegance",m:`${elegance.remaining}/${elegance.total} remaining`});
  }
  if(isReliability && r?.summary){
    cards.push({t:"Checks",m:String(r.summary.total || 0)});
    cards.push({t:"Passed",m:String(r.summary.passed || 0)});
    cards.push({t:"Failed",m:String(r.summary.failed || 0)});
    cards.push({t:"Skipped",m:String(r.summary.skipped || 0)});
  }
  if(isReliability && r?.pytest?.requested){
    cards.push({t:"Pytest",m:`${r.pytest.ok ? "pass" : "fail"} (exit ${r.pytest.exit_code ?? -1})`});
  }
  if(r?.artifacts){
    const lines = Object.entries(r.artifacts).slice(0,3).map(([k,v])=>`${k}: ${v}`);
    cards.push({t:"Artifacts",m:lines.join(" | ")});
  }
  const freshnessMode = String(r?.summary?.artifact_reuse_mode || "").trim();
  const reusedOutputs = r?.summary?.reused_existing_outputs;
  if(freshnessMode){
    const reusedLabel = (typeof reusedOutputs === "boolean") ? (reusedOutputs ? "reused" : "regenerated") : "n/a";
    cards.push({t:"Freshness",m:`${freshnessMode} | ${reusedLabel}`});
  }
  (r?.canvas?.cards||[]).slice(0,4).forEach(c=>cards.push({t:c.title||"Item",m:`${c.price||""} ${c.source?`| ${c.source}`:""}`.trim()}));
  if(cards.length===0){ cards.push({t:"Status",m:ok?"Completed":"Needs input"}); }
  const artifacts = r?.artifacts || {};
  const manifestItems = Array.isArray(r?.artifact_manifest?.items) ? r.artifact_manifest.items.filter(item => item && typeof item.path==="string" && item.path.trim().length>0) : [];
  let entries = manifestItems.length
    ? manifestItems.map(item => ({ key: String(item.key||item.title||"artifact"), path: String(item.path||""), meta: item }))
    : Object.entries(artifacts)
        .filter(([k,v])=>typeof v==="string" && v.trim().length>0)
        .map(([k,v])=>({ key: k, path: String(v||""), meta: null }));
  if(entries.length===0){
    const recentOutputs = Array.isArray(ui?.latestState?.world_model?.created_outputs)
      ? ui.latestState.world_model.created_outputs.filter(v=>typeof v==="string" && v.trim().length>0)
      : [];
    entries = recentOutputs.slice(0,6).map((path, idx)=>({ key: `recent_output_${idx+1}`, path: String(path||""), meta: null }));
    if(entries.length){
      cards.push({t:"Recent outputs", m:`${entries.length} file(s) available in session history`});
    }
  }
  if(entries.length){
    const links = entries.map(entry=>{
      const meta = entry?.meta || {};
      const state = String(meta?.validation_state || "").trim();
      const history = Array.isArray(meta?.validation_history) && meta.validation_history.length
        ? meta.validation_history.map(h=>String(h?.state||"")).filter(Boolean).join(" -> ")
        : "";
      const evidence = String(meta?.evidence_summary || "").trim();
      const detailBits = [
        state ? `state: ${state}` : "",
        history ? `history: ${history}` : "",
      ].filter(Boolean).join(" | ");
      return `<div style="margin-bottom:8px"><a href="${escapeHtml(artifactHref(entry.path))}" target="_blank" rel="noopener">${escapeHtml(entry.key)}</a>${detailBits ? `<div class="small">${escapeHtml(detailBits)}</div>` : ""}${evidence ? `<div class="small">${escapeHtml(evidence)}</div>` : ""}</div>`;
    }).join("");
    document.getElementById("artifactList").innerHTML = `<div class="small" style="margin-top:8px">Outputs</div>${links}`;
    const canvasList = document.getElementById("artifactListCanvas");
    if(canvasList){ canvasList.innerHTML = links; }
  } else {
    document.getElementById("artifactList").innerHTML = "";
    const canvasList = document.getElementById("artifactListCanvas");
    if(canvasList){ canvasList.innerText = "No artifacts yet."; }
  }
  manifestItems.slice(0,3).forEach(item=>{
    cards.push({
      t:`Artifact: ${String(item?.title || item?.key || "artifact")}`,
      m:String(item?.type || "file"),
      badge:String(item?.validation_state || "ready"),
    });
  });
  document.getElementById("summaryCards").innerHTML = cards.slice(0,6).map(c=>`<div class="summary-card"><div class="t">${escapeHtml(c.t||"")}</div><div class="m">${escapeHtml(c.m||"")}</div>${c.badge ? `<div class="badge ${escapeHtml(String(c.badge||"").replace(/[^a-z_]/gi,'_').toLowerCase())}">${escapeHtml(c.badge)}</div>` : ""}</div>`).join("");
  renderPlatformCards(r||{});
  renderRuntimeTimelineCanvas(r||{});

  const activity=[];
  if(Array.isArray(r?.narration)){ r.narration.forEach(x=>activity.push(`- ${x}`)); }
  if(isReliability && Array.isArray(r?.checks)){
    r.checks.forEach(item=>{
      const status = String(item?.status || "unknown").toUpperCase();
      activity.push(`- ${status}: ${item?.name || "check"}${item?.details ? ` (${item.details})` : ""}`);
    });
    if(r?.pytest?.requested){
      activity.push(`- PYTEST ${r.pytest.ok ? "PASS" : "FAIL"} (exit ${r.pytest.exit_code ?? -1})`);
      const tail = Array.isArray(r?.pytest?.output_tail) ? r.pytest.output_tail : [];
      tail.slice(-5).forEach(line => activity.push(`  ${line}`));
    }
  }
  if(Array.isArray(r?.decision_log)){ r.decision_log.forEach(x=>activity.push(`- ${x}`)); }
  if(freshnessMode){
    activity.push(`- Freshness policy: ${freshnessMode}`);
    if(typeof reusedOutputs === "boolean"){ activity.push(`- Output handling: ${reusedOutputs ? "reused existing artifacts" : "regenerated artifacts"}`); }
  }
  if(Array.isArray(elegance?.events) && elegance.events.length){
    elegance.events.slice(-4).forEach(ev=>activity.push(`- elegance: -${ev.cost} (${ev.reason})`));
  }
  if(r?.source_status){ Object.entries(r.source_status).slice(0,10).forEach(([k,v])=>activity.push(`- ${k}: ${v}`)); }
  if(r?.pause_reason){ activity.push(`- ${r.pause_reason}`); }
  if(activity.length===0){ activity.push(ok?"- Finished successfully.":"- Check details for error context."); }
  document.getElementById("activityLog").innerText = activity.join("\\n");
  renderAssistantFeedFromResult(r||{});
  updateWorkCanvas(String(r?.opened_url||""), r?.opened_url ? "Live page from latest result." : "No active page.");
  if(r?.opened_url || entries.length || r?.paused_for_credentials){
    toggleCanvas(true);
  }
  renderStrictRules(r||{});
}
function renderRuntimeTimelineCanvas(r){
  const mount = document.getElementById("runtimeTimelineCanvas");
  if(!mount){ return; }
  const groups = Array.isArray(r?.ui_cards?.runtime_timeline?.groups) ? r.ui_cards.runtime_timeline.groups : [];
  if(!groups.length){
    mount.innerHTML = "<div class='small'>No runtime timeline yet.</div>";
    return;
  }
  const filteredGroups = groups.map(group=>{
    const events = Array.isArray(group.events) ? group.events.filter(item=>{
      if(timelineFilter === "node"){ return !String(item?.critic||"").trim() && !String(item?.event||"").includes("revision"); }
      if(timelineFilter === "critic"){ return !!String(item?.critic||"").trim() || String(item?.event||"").includes("critic"); }
      if(timelineFilter === "revision"){ return String(item?.event||"").includes("revision"); }
      return true;
    }) : [];
    const critics = Array.isArray(group.critics) ? group.critics.filter(entry=>{
      if(timelineFilter === "node"){ return false; }
      if(timelineFilter === "critic"){ return true; }
      if(timelineFilter === "revision"){
        return Array.isArray(entry.events) && entry.events.some(item=>String(item?.event||"").includes("revision"));
      }
      return true;
    }) : [];
    return {...group, events, critics};
  }).filter(group => (group.events && group.events.length) || (group.critics && group.critics.length));
  if(!filteredGroups.length){
    mount.innerHTML = `<div class='small'>No timeline entries for filter: ${escapeHtml(timelineFilter)}</div>`;
    return;
  }
  mount.innerHTML = filteredGroups.map((group, idx)=>{
    const heading = `${group.capability || group.node_id || "graph"}${group.status ? ` | ${group.status}` : ""}`;
    const events = Array.isArray(group.events) ? group.events.map(item=>{
      const parts = [
        item.event || "",
        item.critic ? `critic=${item.critic}` : "",
        item.status ? `status=${item.status}` : "",
      ].filter(Boolean).join(" | ");
      return `<div class="timeline-event">${escapeHtml(parts)}</div>`;
    }).join("") : "";
    const critics = Array.isArray(group.critics) ? group.critics.map(entry=>{
      const items = Array.isArray(entry.events) ? entry.events.map(item=>`${item.event}${item.status ? ` (${item.status})` : ""}`).join(", ") : "";
      return `<div class="timeline-critic"><strong>${escapeHtml(entry.critic || "critic")}</strong>: ${escapeHtml(items)}</div>`;
    }).join("") : "";
    return `<details class="timeline-group" ${idx===0 ? "open" : ""}><summary>${escapeHtml(heading)}</summary><div class="timeline-group-body">${events}${critics}</div></details>`;
  }).join("");
}
function renderPlatformCards(r){
  const cards = r?.ui_cards || {};
  const sections = [];
  const pushSection = (title, body) => {
    if(!body){ return; }
    sections.push(`<details class="summary-card"><summary class="t">${escapeHtml(title)}</summary><div class="m" style="margin-top:8px;">${body}</div></details>`);
  };
  const contract = cards?.task_contract || {};
  if(contract?.goal || contract?.domain){
    pushSection("Task Contract",
      [
        contract.goal ? `<div><strong>Goal:</strong> ${escapeHtml(contract.goal)}</div>` : "",
        contract.audience ? `<div><strong>Audience:</strong> ${escapeHtml(contract.audience)}</div>` : "",
        contract.domain ? `<div><strong>Domain:</strong> ${escapeHtml(contract.domain)}</div>` : "",
        contract.geography ? `<div><strong>Geography:</strong> ${escapeHtml(contract.geography)}</div>` : "",
        Array.isArray(contract.requested_outputs) && contract.requested_outputs.length ? `<div><strong>Outputs:</strong> ${escapeHtml(contract.requested_outputs.join(", "))}</div>` : "",
        Array.isArray(contract.constraints) && contract.constraints.length ? `<div><strong>Constraints:</strong> ${escapeHtml(contract.constraints.join(" | "))}</div>` : "",
      ].filter(Boolean).join("")
    );
  }
  const manifest = cards?.artifact_manifest || {};
  if(Array.isArray(manifest?.items) && manifest.items.length){
    pushSection("Artifact Manifest",
      manifest.items.slice(0,8).map(item=>{
        const href = artifactHref(item.path || "");
        const label = item.title || item.key || "artifact";
        const meta = [
          item.type ? `type: ${item.type}` : "",
          item.validation_state ? `state: ${item.validation_state}` : "",
        ].filter(Boolean).join(" | ");
        const evidence = item.evidence_summary ? `<div class="small">${escapeHtml(item.evidence_summary)}</div>` : "";
        const history = Array.isArray(item.validation_history) && item.validation_history.length
          ? `<div class="small">history: ${escapeHtml(item.validation_history.map(h=>String(h.state||"")).join(" -> "))}</div>`
          : "";
        return `<div style="margin-bottom:10px"><a href="${escapeHtml(href)}" target="_blank" rel="noopener">${escapeHtml(label)}</a>${meta ? ` <span class="small">${escapeHtml(meta)}</span>` : ""}${evidence}${history}</div>`;
      }).join("")
    );
  }
  const critic = cards?.critic_results || {};
  if(Array.isArray(critic?.items) && critic.items.length){
    pushSection("Critic Results",
      critic.items.map(item=>{
        const state = item.passed ? "pass" : "needs work";
        const fix = item.required_fix ? ` | fix: ${item.required_fix}` : "";
        return `<div><strong>${escapeHtml(item.critic || "critic")}</strong>: ${escapeHtml(state)} (${escapeHtml(String(item.score || ""))})${fix ? escapeHtml(fix) : ""}</div>`;
      }).join("")
    );
  }
  const graph = cards?.execution_graph || {};
  if(Array.isArray(graph?.nodes) && graph.nodes.length){
    pushSection("Execution Graph",
      graph.nodes.slice(0,10).map(node=>`<div><strong>${escapeHtml(node.capability || "")}</strong>: ${escapeHtml(node.status || "")}${node.attempts ? ` (${escapeHtml(String(node.attempts))} attempt)` : ""}</div>`).join("")
    );
  }
  const timeline = cards?.runtime_timeline || {};
  if(Array.isArray(timeline?.groups) && timeline.groups.length){
    pushSection("Runtime Timeline",
      timeline.groups.map(group=>{
        const heading = `${group.capability || group.node_id || "graph"}${group.status ? ` | ${group.status}` : ""}`;
        const events = Array.isArray(group.events) ? group.events.map(item=>{
          const parts = [
            item.event || "",
            item.status ? `status=${item.status}` : "",
          ].filter(Boolean).join(" | ");
          return `<div class="small">${escapeHtml(parts)}</div>`;
        }).join("") : "";
        const critics = Array.isArray(group.critics) ? group.critics.map(entry=>{
          const items = Array.isArray(entry.events) ? entry.events.map(item=>`${item.event}${item.status ? ` (${item.status})` : ""}`).join(", ") : "";
          return `<div class="small"><strong>${escapeHtml(entry.critic || "critic")}</strong>: ${escapeHtml(items)}</div>`;
        }).join("") : "";
        return `<div style="margin-bottom:10px"><div><strong>${escapeHtml(heading)}</strong></div>${events}${critics}</div>`;
      }).join("")
    );
  } else if(Array.isArray(timeline?.items) && timeline.items.length){
    pushSection("Runtime Timeline",
      timeline.items.slice(-12).map(item=>{
        const parts = [
          item.event || "",
          item.capability || item.node_id || "",
          item.critic ? `critic=${item.critic}` : "",
          item.status ? `status=${item.status}` : "",
        ].filter(Boolean).join(" | ");
        return `<div class="small">${escapeHtml(parts)}</div>`;
      }).join("")
    );
  }
  const memory = cards?.memory_context || {};
  if((Array.isArray(memory?.used) && memory.used.length) || (Array.isArray(memory?.rejected) && memory.rejected.length)){
    const used = Array.isArray(memory.used) ? memory.used.slice(0,4).map(item=>`<div><strong>${escapeHtml(item.type || "memory")}</strong>: ${escapeHtml(JSON.stringify(item.content || {}).slice(0,120))}</div>`).join("") : "";
    const rejected = Array.isArray(memory.rejected) ? memory.rejected.slice(0,4).map(item=>`<div class="small">Rejected: ${escapeHtml(item.reason || "")}</div>`).join("") : "";
    pushSection("Memory Context", `${used}${rejected}`);
  }
  const validation = cards?.validation || {};
  const finalOutputGate = validation?.final_output_gate || cards?.final_output_gate || {};
  if(Array.isArray(validation?.items) && validation.items.length){
    pushSection("Validation",
      validation.items.map(item=>{
        const state = item.passed ? "pass" : "blocked";
        return `<div><strong>${escapeHtml(item.name || "validation")}</strong>: ${escapeHtml(state)}${item.severity ? ` | ${escapeHtml(item.severity)}` : ""}${Number(item.issue_count||0) ? ` | issues=${escapeHtml(String(item.issue_count))}` : ""}${item.repair_attempted ? " | repair attempted" : ""}</div>`;
      }).join("")
      + (finalOutputGate && Object.keys(finalOutputGate).length ? `<div class="small" data-final-output-gate="final_output_gate" style="margin-top:8px;">Gate: ${escapeHtml(String(finalOutputGate.passed ? "passed" : "blocked"))}</div>` : "")
      + ((Array.isArray(validation?.blocking_failures) && validation.blocking_failures.length) ? `<div class="small" style="margin-top:8px;">Blocking: ${escapeHtml(validation.blocking_failures.join(", "))}</div>` : "")
      + ((Array.isArray(validation?.required_repairs) && validation.required_repairs.length) ? `<div class="small" style="margin-top:8px;">Repairs: ${escapeHtml(validation.required_repairs.slice(0,4).join(" | "))}</div>` : "")
    );
  }
  const html = sections.join("");
  const mount = document.getElementById("platformCards");
  if(mount){ mount.innerHTML = html; }
  const canvasMount = document.getElementById("platformCardsCanvas");
  if(canvasMount){ canvasMount.innerHTML = html || "<div class='small'>No task context cards yet.</div>"; }
}
function _assistantMessageHtml(text, kind){
  return `<div class="assistant-msg ${escapeHtml(kind||"agent")}">${escapeHtml(String(text||""))}</div>`;
}
function renderAssistantFeedFromResult(r){
  const lines=[];
  const mode = String(r?.mode||"");
  if(mode){ lines.push(`<div class="small"><strong>${escapeHtml(mode)}</strong></div>`); }
  const narration = Array.isArray(r?.narration) ? r.narration.slice(-4) : [];
  narration.forEach(n=>lines.push(`<div>${escapeHtml(n)}</div>`));
  const decisions = Array.isArray(r?.decision_log) ? r.decision_log.slice(-4) : [];
  decisions.forEach(d=>lines.push(`<div>${escapeHtml(d)}</div>`));
  const pauseReason = String(r?.pause_reason||"").trim();
  if(pauseReason){ lines.push(`<div>${escapeHtml(pauseReason)}</div>`); }
  const ok = !!r?.ok;
  const doneLine = ok ? (r?.canvas?.title || "Task completed.") : (r?.error || "Task needs input.");
  const manifestItems = Array.isArray(r?.artifact_manifest?.items) ? r.artifact_manifest.items : [];
  if(manifestItems.length){
    const counts = {};
    manifestItems.forEach(item=>{
      const key = String(item?.validation_state || "ready");
      counts[key] = (counts[key] || 0) + 1;
    });
    const badges = Object.entries(counts).map(([state,count])=>`<span class="badge ${stateClassName(state)}">${escapeHtml(state)}: ${escapeHtml(String(count))}</span>`).join(" ");
    if(badges){ lines.push(`<div>${badges}</div>`); }
    const inlineArtifacts = renderInlineArtifactChips(manifestItems);
    if(inlineArtifacts){ lines.push(inlineArtifacts); }
  }
  lines.push(`<div><strong>${escapeHtml(doneLine)}</strong></div>`);
  lines.push(`<div class="feedback-row">
    <button onclick="submitFeedback('thumbs_up')">thumbs up</button>
    <button onclick="submitFeedback('thumbs_down')">thumbs down</button>
    <button onclick="submitFeedback('too_slow')">too slow</button>
    <button onclick="submitFeedback('wrong_path')">wrong path</button>
    <button onclick="submitFeedback('not_human_like')">not human-like</button>
    <button onclick="submitFeedback('great_result')">great result</button>
  </div>`);
  _appendAssistantFeed(lines.join(""), ok ? "agent" : "meta");
}
function renderAssistantFeedFromTask(task){
  const events = Array.isArray(task?.events) ? task.events : [];
  if(!events.length){
    _appendAssistantFeed("Waiting for first task event.", "meta");
    return;
  }
  const lines = [];
  lines.push(`<div class="small"><strong>Task:</strong> ${escapeHtml(String(task?.status||"running"))}</div>`);
  events.slice(-8).forEach(e=>{
    const msg = `[${Number(e?.progress||0)}%] ${String(e?.message||"")}`;
    lines.push(`<div>${escapeHtml(msg)}</div>`);
  });
  _appendAssistantFeed(lines.join(""), "agent");
}
function renderWorldModel(model){
  const wm = model || {};
  const narration = Array.isArray(wm?.narration) ? wm.narration : [];
  const noticed = Array.isArray(wm?.what_i_noticed) ? wm.what_i_noticed : [];
  const signals = wm?.signals || {};
  const compact = {
    signals,
    workspace: wm.workspace || {},
    task: wm.task || {},
    recent: wm.recent || {},
    candidate_targets: Array.isArray(wm?.candidate_targets) ? wm.candidate_targets : [],
    created_outputs: Array.isArray(wm?.created_outputs) ? wm.created_outputs.slice(0,8) : [],
  };
  document.getElementById("worldModelNarration").innerText = narration.length
    ? narration.join(" ")
    : "No environment narration available.";
  document.getElementById("worldModelNoticed").innerText = noticed.length
    ? `What I noticed: ${noticed.join(" | ")}`
    : "What I noticed: no notable environment signals yet.";
  document.getElementById("worldModelBox").innerText = JSON.stringify(compact, null, 2);
}
function escapeHtml(s){ return String(s||"").replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])); }
function stateClassName(state){
  return escapeHtml(String(state||"").replace(/[^a-z_]/gi,'_').toLowerCase());
}
function artifactDisplayName(item){
  return String(item?.title || item?.key || "artifact");
}
function renderInlineArtifactChips(items){
  const rows = Array.isArray(items) ? items.filter(item => item && typeof item.path === "string" && item.path.trim().length > 0).slice(0,4) : [];
  if(!rows.length){ return ""; }
  return `<div class="artifact-chip-list">${rows.map(item=>{
    const state = String(item?.validation_state || "ready");
    const evidence = String(item?.evidence_summary || "").trim();
    return `<div class="artifact-chip"><div class="artifact-chip-head"><span class="artifact-chip-title">${escapeHtml(artifactDisplayName(item))}</span><span class="badge ${stateClassName(state)}">${escapeHtml(state)}</span></div>${evidence ? `<div class="small">${escapeHtml(evidence)}</div>` : ""}<div class="artifact-chip-actions"><a class="artifact-open" href="${escapeHtml(artifactHref(item.path || ""))}" target="_blank" rel="noopener">Open</a></div></div>`;
  }).join("")}</div>`;
}
function isLikelyLocalPath(path){
  const p = String(path||"").trim();
  if(!p){ return false; }
  if(p.startsWith("http://") || p.startsWith("https://") || p.startsWith("file:///")){ return false; }
  if(p.startsWith("data:") || p.startsWith("about:")){ return false; }
  if(/^[A-Za-z0-9.-]+\\.[A-Za-z]{2,}(?:[/?#].*)?$/.test(p)){ return false; }
  if(/^[A-Za-z]:[\\\\/]/.test(p)){ return true; }
  if(p.startsWith("./") || p.startsWith("../") || p.startsWith("/")){ return true; }
  return p.includes("\\\\") || p.includes("/");
}
function artifactHref(path){
  const p = String(path||"").trim();
  if(!p){ return ""; }
  if(p.startsWith("http://") || p.startsWith("https://")){ return p; }
  if(p.startsWith("file:///")){
    try{
      const withoutScheme = p.slice("file:///".length);
      const normalized = decodeURIComponent(withoutScheme).replace(/\\//g, "\\\\");
      return `/api/artifact?path=${encodeURIComponent(normalized)}`;
    }catch(_e){
      return `/api/artifact?path=${encodeURIComponent(p)}`;
    }
  }
  return `/api/artifact?path=${encodeURIComponent(p)}`;
}
function toUri(path){
  const p = String(path||"");
  if(p.startsWith("http://") || p.startsWith("https://")) return p;
  if(isLikelyLocalPath(p)) return artifactHref(p);
  if(p.startsWith("file:///")) return artifactHref(p);
  return p;
}
function updateWorkCanvas(rawUrl, note){
  const frame=document.getElementById("workCanvasFrame");
  const link=document.getElementById("workCanvasOpen");
  const noteEl=document.getElementById("workCanvasNote");
  const url=toUri(rawUrl||"");
  if(url){
    if(lastCanvasUrl!==url){
      frame.src=url;
      lastCanvasUrl=url;
    }
    link.href=url;
    link.style.visibility="visible";
    noteEl.innerText = note || `Showing: ${url}`;
    return;
  }
  if(lastCanvasUrl){
    frame.src="about:blank";
    lastCanvasUrl="";
  }
  link.removeAttribute("href");
  link.style.visibility="hidden";
  noteEl.innerText = note || "No active page.";
}
function renderAuthRecoveryWizard(s){
  const panel=document.getElementById("authRecoveryPanel");
  const rec=s?.auth_recovery||{};
  const show = !!(rec?.show || s?.paused_for_credentials || s?.auth_loop_blocked);
  if(!show){
    panel.style.display="none";
    return;
  }
  panel.style.display="block";
  const code = String(rec?.error_code||"credential_missing");
  const currentMode = String(rec?.current_mode || s?.browser_worker_mode || "local");
  const recommendedMode = String(rec?.recommended_mode || currentMode || "local");
  const confidence = String(rec?.confidence || "low").toUpperCase();
  const reason = String(rec?.reason || s?.pause_reason || "Complete authentication, then resume once.");
  document.getElementById("authRecoveryCode").innerText = code;
  document.getElementById("authRecoveryModeLine").innerText = `Current worker: ${currentMode}`;
  document.getElementById("authRecoveryRecommendLine").innerText = `Recommended worker: ${recommendedMode} (${confidence} confidence)`;
  document.getElementById("authRecoveryReason").innerText = reason;
  const btn=document.getElementById("authRecoveryApplyBtn");
  btn.dataset.mode = recommendedMode;
  btn.disabled = !recommendedMode || (recommendedMode === currentMode);
}
async function applyAuthRecoveryRecommendation(){
  const btn=document.getElementById("authRecoveryApplyBtn");
  const mode=String(btn?.dataset?.mode||"").trim().toLowerCase();
  if(mode !== "local" && mode !== "docker"){ return; }
  await setBrowserWorkerMode(mode);
}
async function refreshState(){
  const s=await fetch("/api/state").then(r=>r.json());
  ui.latestState = s;
  let t=s.control_granted?"Control: granted":"Control: not granted";
  const preflight = s.preflight || {};
  if(preflight.required){
    t += preflight.green ? " | Preflight: GREEN" : " | Preflight: BLOCKED";
  }
  if(s.browser_worker_mode){ t += ` | Worker: ${s.browser_worker_mode}`; }
  if(s.human_like_interaction){ t += " | Human-like: ON"; }
  if(s.paused_for_credentials) t+=" | Paused";
  if(s.has_pending_plan) t+=" | Pending sequence";
  if(s.pause_reason) t+=" | "+s.pause_reason;
  document.getElementById("statusBox").innerText=t;
  const auth=document.getElementById("authAlert");
  if(s.paused_for_credentials){
    const link = s.pending_auth_url ? `<div style="margin-top:6px;"><button onclick="focusAuthTarget()" style="background:#111827;color:#fee2e2;border:1px solid #ef4444;padding:6px 10px;border-radius:8px;cursor:pointer;">Focus auth tab</button></div>` : "";
    const loopGuard = s.auth_loop_blocked
      ? `<div class="small" style="color:#fecaca;margin-top:6px;">Loop guard active after ${Number(s.auth_loop_count||0)} repeated retries. Use Focus auth, then Resume once.</div>`
      : "";
    auth.style.display="block";
    auth.innerHTML = `<strong>Action Required: Authentication Needed</strong><div class="small" style="color:#fecaca;margin-top:4px;">${escapeHtml(s.pause_reason||"Complete sign-in, then click Resume.")}</div>${loopGuard}${link}<div class="small" style="margin-top:6px;">After you finish auth, click <strong>Resume</strong>.</div>`;
  } else {
    auth.style.display="none";
    auth.innerHTML="";
  }
  document.getElementById("stepMode").checked=!!s.step_mode;
  document.getElementById("manualAuthPhase").checked=!!s.manual_auth_phase;
  document.getElementById("browserWorkerMode").value=s.browser_worker_mode||"local";
  document.getElementById("humanLikeInteraction").checked=!!s.human_like_interaction;
  document.getElementById("aiBackend").value=s.ai_backend||"deterministic-local";
  document.getElementById("compressionMode").value=s.compression_mode||"normal";
  document.getElementById("minLiveCites").value=String(s.min_live_non_curated_citations||3);
  document.getElementById("artifactReuseMode").value=s.artifact_reuse_mode||"reuse_if_recent";
  document.getElementById("artifactReuseMaxAgeHours").value=String(s.artifact_reuse_max_age_hours||72);
  document.getElementById("useDomainFreshnessDefaults").checked=!!s.use_domain_freshness_defaults;
  const teach=s.teach||{};
  document.getElementById("teachState").innerText=`${teach.active?'Recording':'Idle'} | events: ${teach.event_count||0}${s.global_teach_active?' | global hooks active':''}`;
  const jobs=(s.schedules||[]).length, recent=(s.schedule_history||[]).length;
  document.getElementById("scheduleState").innerText=`${jobs} schedule(s) configured | ${recent} recent run(s)`;
  const vs=s.vault_status||{};
  document.getElementById("vaultState").innerText=`Vault: ${vs.entries||0} entries | ${vs.dpapi_available?'DPAPI secured':'local encryption fallback'}`;
  renderTask(s.task||{});
  renderWorldModel(s.world_model||{});
  const pausedUrl = s.paused_for_credentials ? String(s.pending_auth_url||"") : "";
  const resultUrl = String(lastRaw?.opened_url||"");
  const canvasUrl = pausedUrl || resultUrl;
  const note = pausedUrl
    ? "Auth target. Some sites block embedding; use Open In Tab if blank."
    : (canvasUrl ? "Live page from latest result." : "No active page.");
  updateWorkCanvas(canvasUrl, note);
  renderAuthRecoveryWizard(s);
}
function renderHistory(){
  const el=document.getElementById("history"); el.innerHTML="";
  [...ui.history].reverse().forEach((item,idx)=>{
    const d=document.createElement("div"); d.className="history-item";
    const canRerun = !!(item && item.instruction);
    const policySelectId = `rerunPolicy_${idx}`;
    d.innerHTML=`<div style="font-weight:600">${item.instruction||item.mode||"Run"}</div><div class="small">${item.app_name||""} ${item.opened_url||""}</div>${canRerun?`<div class="row" style="margin-top:6px"><button onclick="rerunHistory(${idx}); event.stopPropagation();">Re-run</button><select id="${policySelectId}" onclick="event.stopPropagation();" onchange="event.stopPropagation();" title="Freshness policy"><option value="reuse_if_recent">reuse if recent</option><option value="reuse">reuse</option><option value="always_regenerate">always regenerate</option></select></div>`:''}`;
    d.onclick=()=>{ renderSummary(item); setRaw(item); };
    el.appendChild(d);
    if(canRerun){
      const select=document.getElementById(policySelectId);
      if(select){
        const current = document.getElementById("artifactReuseMode");
        select.value = (current && current.value) ? current.value : "reuse_if_recent";
      }
    }
  });
}
async function rerunHistory(revIndex){
  const items=[...ui.history].reverse();
  const item=items[revIndex];
  if(!item || !item.instruction){ return; }
  const ai_backend=document.getElementById("aiBackend").value;
  const min_live_non_curated_citations=parseInt(document.getElementById("minLiveCites").value||"3",10);
  const manual_auth_phase=!!document.getElementById("manualAuthPhase").checked;
  const browser_worker_mode=document.getElementById("browserWorkerMode").value||"local";
  const human_like_interaction=!!document.getElementById("humanLikeInteraction").checked;
  const use_domain_freshness_defaults=!!document.getElementById("useDomainFreshnessDefaults").checked;
  const selector = document.getElementById(`rerunPolicy_${revIndex}`);
  const artifact_reuse_mode=(selector && selector.value) ? selector.value : (document.getElementById("artifactReuseMode").value||"reuse_if_recent");
  const artifact_reuse_max_age_hours=parseInt(document.getElementById("artifactReuseMaxAgeHours").value||"72",10);
  const r=await fetch("/api/instruct_async",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({instruction:item.instruction,ai_backend,min_live_non_curated_citations,manual_auth_phase,browser_worker_mode,human_like_interaction,use_domain_freshness_defaults,artifact_reuse_mode,artifact_reuse_max_age_hours})}).then(r=>r.json());
  if(!r.ok){ showResponse(r); await refreshState(); return; }
  startTaskPolling(r.task_id, { instruction:item.instruction, ai_backend, min_live_non_curated_citations, manual_auth_phase, browser_worker_mode, human_like_interaction, use_domain_freshness_defaults, artifact_reuse_mode, artifact_reuse_max_age_hours, confirm_risky:false });
}
async function grantControl(){
  await fetch("/api/control",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({accept:true})}).then(r=>r.json());
  _appendAssistantFeed("Control granted. I can execute tasks now.", "meta");
  await refreshState();
}
async function revokeControl(){
  await fetch("/api/control",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({accept:false})}).then(r=>r.json());
  _appendAssistantFeed("Control revoked.", "meta");
  await refreshState();
}
async function setStepMode(v){ await fetch("/api/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({step_mode:!!v})}); await refreshState(); }
async function setManualAuthPhase(v){ await fetch("/api/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({manual_auth_phase:!!v})}); await refreshState(); }
async function setBrowserWorkerMode(v){ await fetch("/api/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({browser_worker_mode:v})}); await refreshState(); }
async function setHumanLikeInteraction(v){ await fetch("/api/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({human_like_interaction:!!v})}); await refreshState(); }
async function setAiBackend(v){ await fetch("/api/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({ai_backend:v})}); await refreshState(); }
async function setCompressionMode(v){ await fetch("/api/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({compression_mode:v})}); await refreshState(); }
async function setMinLiveCites(v){ const n=Math.max(1,Math.min(20,parseInt(v||"3",10)||3)); await fetch("/api/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({min_live_non_curated_citations:n})}); await refreshState(); }
async function setArtifactReuseMode(v){ await fetch("/api/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({artifact_reuse_mode:v})}); await refreshState(); }
async function setArtifactReuseMaxAgeHours(v){ const n=Math.max(1,Math.min(720,parseInt(v||"72",10)||72)); await fetch("/api/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({artifact_reuse_max_age_hours:n})}); await refreshState(); }
async function setUseDomainFreshnessDefaults(v){ await fetch("/api/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({use_domain_freshness_defaults:!!v})}); await refreshState(); }
async function refreshFreshnessPreview(){
  const instruction=(document.getElementById("instruction").value||"").trim();
  if(!instruction){
    document.getElementById("freshnessPreview").innerText="Freshness preview: waiting for instruction.";
    return;
  }
  const artifact_reuse_mode=document.getElementById("artifactReuseMode").value||"reuse_if_recent";
  const artifact_reuse_max_age_hours=parseInt(document.getElementById("artifactReuseMaxAgeHours").value||"72",10);
  const use_domain_freshness_defaults=!!document.getElementById("useDomainFreshnessDefaults").checked;
  const r=await fetch("/api/policy/freshness/preview",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({instruction,artifact_reuse_mode,artifact_reuse_max_age_hours,use_domain_freshness_defaults})}).then(r=>r.json());
  if(r?.ok){
    document.getElementById("freshnessPreview").innerText=`Freshness preview: domain=${r.domain} | mode=${r.mode} | hrs=${r.max_age_hours} | source=${r.source}`;
  } else {
    document.getElementById("freshnessPreview").innerText="Freshness preview: unavailable.";
  }
}
async function loadDomainFreshnessPolicy(){
  const domain=document.getElementById("freshnessPolicyDomain").value;
  const r=await fetch("/api/policy/freshness/get",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({domain})}).then(r=>r.json());
  if(r?.ok){
    document.getElementById("freshnessPolicyMode").value=r.mode||"reuse_if_recent";
    document.getElementById("freshnessPolicyHours").value=String(r.max_age_hours||72);
  } else {
    showResponse(r||{ok:false,error:"Failed to load freshness policy"});
  }
}
async function saveDomainFreshnessPolicy(){
  const domain=document.getElementById("freshnessPolicyDomain").value;
  const mode=document.getElementById("freshnessPolicyMode").value;
  const max_age_hours=Math.max(1,Math.min(720,parseInt(document.getElementById("freshnessPolicyHours").value||"72",10)||72));
  const r=await fetch("/api/policy/freshness/set",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({domain,mode,max_age_hours})}).then(r=>r.json());
  showResponse(r);
  await refreshState();
}
async function resumeAfterLogin(){
  const r=await fetch("/api/session/resume",{method:"POST",headers:{"Content-Type":"application/json"},body:"{}"}).then(r=>r.json());
  if(r?.task_id){
    const ai_backend=document.getElementById("aiBackend").value;
    const min_live_non_curated_citations=parseInt(document.getElementById("minLiveCites").value||"3",10);
    const browser_worker_mode=document.getElementById("browserWorkerMode").value||"local";
    const human_like_interaction=!!document.getElementById("humanLikeInteraction").checked;
    startTaskPolling(r.task_id, { instruction:r.instruction||"", ai_backend, min_live_non_curated_citations, manual_auth_phase:false, browser_worker_mode, human_like_interaction, confirm_risky:false });
    await refreshState();
    return;
  }
  handleResult(r); await refreshState();
}
async function focusAuthTarget(){
  const r=await fetch("/api/session/focus_auth",{method:"POST",headers:{"Content-Type":"application/json"},body:"{}"}).then(r=>r.json());
  handleResult(r);
  await refreshState();
}
async function resetSessionState(){
  const r=await fetch("/api/session/reset",{method:"POST",headers:{"Content-Type":"application/json"},body:"{}"}).then(r=>r.json());
  handleResult(r);
  await refreshState();
}
async function runInstruction(){
  const instruction=document.getElementById("instruction").value.trim(); if(!instruction) return;
  _appendAssistantFeed(escapeHtml(instruction), "user");
  lastTaskFeedKey = "";
  const ai_backend=document.getElementById("aiBackend").value;
  const min_live_non_curated_citations=parseInt(document.getElementById("minLiveCites").value||"3",10);
  const manual_auth_phase=!!document.getElementById("manualAuthPhase").checked;
  const browser_worker_mode=document.getElementById("browserWorkerMode").value||"local";
  const human_like_interaction=!!document.getElementById("humanLikeInteraction").checked;
  const use_domain_freshness_defaults=!!document.getElementById("useDomainFreshnessDefaults").checked;
  const artifact_reuse_mode=document.getElementById("artifactReuseMode").value||"reuse_if_recent";
  const artifact_reuse_max_age_hours=parseInt(document.getElementById("artifactReuseMaxAgeHours").value||"72",10);
  await refreshFreshnessPreview();
  const r=await fetch("/api/instruct_async",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({instruction,ai_backend,min_live_non_curated_citations,manual_auth_phase,browser_worker_mode,human_like_interaction,use_domain_freshness_defaults,artifact_reuse_mode,artifact_reuse_max_age_hours})}).then(r=>r.json());
  if(!r.ok){ showResponse(r); await refreshState(); return; }
  startTaskPolling(r.task_id, { instruction, ai_backend, min_live_non_curated_citations, manual_auth_phase, browser_worker_mode, human_like_interaction, use_domain_freshness_defaults, artifact_reuse_mode, artifact_reuse_max_age_hours, confirm_risky:false });
}
async function captureClipboardImageUi(){
  const instruction="Capture the current clipboard image and save it as an artifact package with base64 output.";
  _appendAssistantFeed(escapeHtml(instruction), "user");
  const r=await fetch("/api/clipboard/capture",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({instruction})}).then(r=>r.json());
  showResponse(r);
  await refreshState();
}
async function previewInstruction(){ const instruction=document.getElementById("instruction").value.trim(); if(!instruction)return; const r=await fetch("/api/preview",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({instruction})}).then(r=>r.json()); showResponse(r); await refreshState(); }
async function saveAutomation(){ const name=document.getElementById("automationName").value.trim(); const instruction=document.getElementById("instruction").value.trim(); const r=await fetch("/api/automation/save",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({name,instruction})}).then(r=>r.json()); showResponse(r); await refreshState(); }
async function runAutomation(){
  const name=document.getElementById("automationName").value.trim();
  const ai_backend=document.getElementById("aiBackend").value;
  const min_live_non_curated_citations=parseInt(document.getElementById("minLiveCites").value||"3",10);
  const manual_auth_phase=!!document.getElementById("manualAuthPhase").checked;
  const browser_worker_mode=document.getElementById("browserWorkerMode").value||"local";
  const human_like_interaction=!!document.getElementById("humanLikeInteraction").checked;
  const use_domain_freshness_defaults=!!document.getElementById("useDomainFreshnessDefaults").checked;
  const artifact_reuse_mode=document.getElementById("artifactReuseMode").value||"reuse_if_recent";
  const artifact_reuse_max_age_hours=parseInt(document.getElementById("artifactReuseMaxAgeHours").value||"72",10);
  const r=await fetch("/api/automation/run_async",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({name,ai_backend,min_live_non_curated_citations,manual_auth_phase,browser_worker_mode,human_like_interaction,use_domain_freshness_defaults,artifact_reuse_mode,artifact_reuse_max_age_hours})}).then(r=>r.json());
  if(!r.ok){ showResponse(r); await refreshState(); return; }
  startTaskPolling(r.task_id, { instruction:r.instruction||"", ai_backend, min_live_non_curated_citations, manual_auth_phase, browser_worker_mode, human_like_interaction, use_domain_freshness_defaults, artifact_reuse_mode, artifact_reuse_max_age_hours, confirm_risky:false });
}
async function regenerateFresh(){
  const fromInput=document.getElementById("instruction").value.trim();
  const fromLast=String(lastRaw?.instruction||"").trim();
  const instruction=fromInput||fromLast;
  if(!instruction){ showResponse({ok:false,error:"No instruction available to regenerate."}); return; }
  const ai_backend=document.getElementById("aiBackend").value;
  const min_live_non_curated_citations=parseInt(document.getElementById("minLiveCites").value||"3",10);
  const manual_auth_phase=!!document.getElementById("manualAuthPhase").checked;
  const browser_worker_mode=document.getElementById("browserWorkerMode").value||"local";
  const human_like_interaction=!!document.getElementById("humanLikeInteraction").checked;
  const use_domain_freshness_defaults=!!document.getElementById("useDomainFreshnessDefaults").checked;
  const artifact_reuse_mode="always_regenerate";
  const artifact_reuse_max_age_hours=parseInt(document.getElementById("artifactReuseMaxAgeHours").value||"72",10);
  const r=await fetch("/api/instruct_async",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({instruction,ai_backend,min_live_non_curated_citations,manual_auth_phase,browser_worker_mode,human_like_interaction,use_domain_freshness_defaults,artifact_reuse_mode,artifact_reuse_max_age_hours})}).then(r=>r.json());
  if(!r.ok){ showResponse(r); await refreshState(); return; }
  startTaskPolling(r.task_id, { instruction, ai_backend, min_live_non_curated_citations, manual_auth_phase, browser_worker_mode, human_like_interaction, use_domain_freshness_defaults, artifact_reuse_mode, artifact_reuse_max_age_hours, confirm_risky:false });
}
async function runReliabilitySuite(){
  const include_pytest = !!document.getElementById("suiteIncludePytest").checked;
  const include_desktop_smoke = !!document.getElementById("suiteIncludeDesktopSmoke").checked;
  const rawArgs = (document.getElementById("suitePytestArgs").value || "").trim();
  const pytest_args = rawArgs ? rawArgs.split(/\\s+/).filter(Boolean) : [];
  const r=await fetch("/api/reliability/run",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({include_pytest,include_desktop_smoke,pytest_args})}).then(r=>r.json());
  if(!r.ok){ showResponse(r); return; }
  showResponse(r);
  startTaskPolling(r.task_id, { confirm_risky:false });
}
async function scoreLastRunBenchmark(){
  const r=await fetch("/api/benchmark/score_last_run",{method:"POST",headers:{"Content-Type":"application/json"},body:"{}"}).then(r=>r.json());
  showResponse(r);
  await refreshState();
}
async function runHuman20Suite(){
  const r=await fetch("/api/benchmark/run_20_suite",{method:"POST",headers:{"Content-Type":"application/json"},body:"{}"}).then(r=>r.json());
  if(!r.ok){ showResponse(r); await refreshState(); return; }
  showResponse(r);
  startTaskPolling(r.task_id, { confirm_risky:false });
}
async function runKiller5Suite(){
  const r=await fetch("/api/benchmark/run_killer_suite",{method:"POST",headers:{"Content-Type":"application/json"},body:"{}"}).then(r=>r.json());
  if(!r.ok){ showResponse(r); await refreshState(); return; }
  showResponse(r);
  startTaskPolling(r.task_id, { confirm_risky:false });
}
async function runNotepadSmoke(){
  const r=await fetch("/api/smoke/notepad",{method:"POST",headers:{"Content-Type":"application/json"},body:"{}"}).then(r=>r.json());
  if(!r.ok){ showResponse(r); await refreshState(); return; }
  showResponse(r);
  startTaskPolling(r.task_id, { confirm_risky:false });
}
async function exportHistory(){ const txt=await fetch("/api/history/export").then(r=>r.text()); const blob=new Blob([txt],{type:"application/json"}); const a=document.createElement("a"); a.href=URL.createObjectURL(blob); a.download="lam-history-export.json"; a.click(); }
async function clearHistory(){
  if(!confirm("Clear local and server history?")) return;
  await fetch("/api/history/clear",{method:"POST",headers:{"Content-Type":"application/json"},body:"{}"});
  ui.history = [];
  persistHistory();
  renderHistory();
  showResponse({ok:true,mode:"history",canvas:{title:"History Cleared",subtitle:"Previous runs removed from this interface.",cards:[]}})
  await refreshState();
}
async function searchApps(){ const q=document.getElementById("appSearch").value.trim(); const r=await fetch("/api/apps/search",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({query:q})}).then(r=>r.json()); showResponse(r); }
async function vaultSave(){
  const payload={
    service:document.getElementById("vaultService").value.trim(),
    username:document.getElementById("vaultUsername").value,
    password:document.getElementById("vaultPassword").value,
    tags:(document.getElementById("vaultTags").value||"").split(",").map(x=>x.trim()).filter(Boolean),
    favorite:!!document.getElementById("vaultFavorite").checked
  };
  const r=await fetch("/api/vault/save",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)}).then(r=>r.json());
  showResponse(r); await vaultList(); await refreshState();
}
async function vaultList(){
  const q=document.getElementById("vaultQuery").value.trim();
  const r=await fetch("/api/vault/list",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({query:q})}).then(r=>r.json());
  const entries=(r.entries||[]);
  if(!entries.length){ document.getElementById("vaultList").innerText="No matching entries."; return; }
  document.getElementById("vaultList").innerText = entries.slice(0,8).map(e=>`${e.service} | ${e.username_masked}${e.favorite?' | favorite':''}`).join("\\n");
}
async function vaultGenerate(){
  const length=parseInt(document.getElementById("vaultLength").value||"20",10);
  const r=await fetch("/api/vault/generate",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({length})}).then(r=>r.json());
  if(r.ok){ document.getElementById("vaultPassword").value=r.password||""; }
  showResponse(r);
}
async function vaultFill(){
  const service=document.getElementById("vaultService").value.trim() || document.getElementById("vaultQuery").value.trim();
  const submit = confirm("Press OK to autofill and submit (Enter), Cancel to autofill only.");
  const r=await fetch("/api/vault/fill",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({service,submit})}).then(r=>r.json());
  showResponse(r); await refreshState();
}
async function vaultExport(){
  const path=prompt("Export encrypted backup path","data/interface/vault_export.lamvault");
  if(!path) return;
  const r=await fetch("/api/vault/export",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({path})}).then(r=>r.json());
  showResponse(r);
}
async function vaultImport(){
  const path=prompt("Import encrypted backup path","data/interface/vault_export.lamvault");
  if(!path) return;
  const r=await fetch("/api/vault/import",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({path,merge:true})}).then(r=>r.json());
  showResponse(r); await vaultList(); await refreshState();
}
function useTemplate(text){ document.getElementById("instruction").value=text; }
async function captureSelector(){ const r=await fetch("/api/selector/capture",{method:"POST",headers:{"Content-Type":"application/json"},body:"{}"}).then(r=>r.json()); showResponse(r); await refreshState(); }
async function teachStart(){ const app_name=document.getElementById("teachApp").value.trim(); const r=await fetch("/api/teach/start",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({app_name})}).then(r=>r.json()); showResponse(r); await refreshState(); }
async function teachGlobalStart(){ const r=await fetch("/api/teach/global_start",{method:"POST",headers:{"Content-Type":"application/json"},body:"{}"}).then(r=>r.json()); showResponse(r); await refreshState(); }
async function teachGlobalStop(){ const r=await fetch("/api/teach/global_stop",{method:"POST",headers:{"Content-Type":"application/json"},body:"{}"}).then(r=>r.json()); showResponse(r); await refreshState(); }
async function teachAddClick(){ const r=await fetch("/api/teach/add_click",{method:"POST",headers:{"Content-Type":"application/json"},body:"{}"}).then(r=>r.json()); showResponse(r); await refreshState(); }
async function teachAddType(){ const text=document.getElementById("teachTypeText").value; const r=await fetch("/api/teach/add_type",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({text})}).then(r=>r.json()); showResponse(r); await refreshState(); }
async function teachAddHotkey(){ const keys=document.getElementById("teachHotkey").value; const r=await fetch("/api/teach/add_hotkey",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({keys})}).then(r=>r.json()); showResponse(r); await refreshState(); }
async function teachAddWait(){ const seconds=parseInt(document.getElementById("teachWait").value||"1",10); const r=await fetch("/api/teach/add_wait",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({seconds})}).then(r=>r.json()); showResponse(r); await refreshState(); }
async function teachStop(){ const r=await fetch("/api/teach/stop",{method:"POST",headers:{"Content-Type":"application/json"},body:"{}"}).then(r=>r.json()); handleResult(r); if(r.ok && r.instruction){ document.getElementById("instruction").value=r.instruction; } await refreshState(); }
async function addSchedule(){
  const name=document.getElementById("scheduleName").value.trim(), automation_name=document.getElementById("scheduleAutomation").value.trim();
  const kind=document.getElementById("scheduleKind").value, value=document.getElementById("scheduleValue").value.trim();
  const r=await fetch("/api/schedules/add",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({name,automation_name,kind,value})}).then(r=>r.json());
  showResponse(r); await refreshState();
}
async function triggerEvent(){
  const value=document.getElementById("scheduleValue").value.trim()||"manual";
  const r=await fetch("/api/schedules/trigger",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({event:value})}).then(r=>r.json());
  showResponse(r); await refreshState();
}
function showResponse(r){ renderSummary(r||{}); setRaw(r||{}); }
function handleResult(r){ showResponse(r); if(r.ok){ ui.history.push(r); persistHistory(); renderHistory(); } }
function renderTask(task){
  const pct = Math.max(0, Math.min(100, parseInt(task.progress||0,10)||0));
  document.getElementById("progressBar").style.width=`${pct}%`;
  document.getElementById("progressLabel").innerText = task.message || (task.status||"Idle");
  const events = task.events || [];
  document.getElementById("progressLog").innerText = events.length
    ? events.map(e=>`[${new Date((e.ts||0)*1000).toLocaleTimeString()}] ${e.progress||0}% - ${e.message||""}`).join("\\n")
    : "No active task.";
  if(String(task.status||"") === "running"){
    document.getElementById("summaryHead").innerText = "Working...";
    document.getElementById("summarySub").innerText = task.message || "Executing plan";
    const live = events.slice(-8).map(e=>`[${new Date((e.ts||0)*1000).toLocaleTimeString()}] ${e.message||""}`).join("\\n");
    document.getElementById("activityLog").innerText = live || "Waiting for first task event.";
    const feedKey = `${String(task.message||"")}|${pct}|${events.length}`;
    if(feedKey !== lastTaskFeedKey){
      lastTaskFeedKey = feedKey;
      renderAssistantFeedFromTask(task||{});
    }
  }
}
function stopTaskPolling(){ if(progressPollTimer){ clearInterval(progressPollTimer); progressPollTimer=null; } }
function startTaskPolling(taskId, rerunPayload){
  stopTaskPolling();
  const tick = async ()=>{
    const t = await fetch(`/api/task?id=${encodeURIComponent(taskId)}`).then(r=>r.json());
    renderTask(t.task||{});
    if((t.task||{}).status === "done"){
      stopTaskPolling();
      let result=(t.task||{}).result||{};
      if(result?.mode === "reliability_suite"){
        const suite = await fetch("/api/reliability/result").then(r=>r.json());
        if(suite?.result){ result = suite.result; }
      }
      if(result.requires_confirmation){
        if(confirm("Risky actions detected. Confirm execution?")){
          const c=await fetch("/api/instruct_async",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({...rerunPayload,confirm_risky:true})}).then(r=>r.json());
          if(c.ok){ startTaskPolling(c.task_id, {...rerunPayload,confirm_risky:true}); return; }
          handleResult(c); await refreshState(); return;
        }
      }
      handleResult(result); await refreshState();
      return;
    }
    if((t.task||{}).status === "error"){
      stopTaskPolling();
      handleResult({ok:false,error:(t.task||{}).error||"Task failed"}); await refreshState();
    }
  };
  progressPollTimer = setInterval(tick, 700);
  tick();
}
window.onload=async()=>{
  const mount = document.getElementById("developerDetailsMount");
  const worldPanel = document.getElementById("worldPanel");
  const runPanel = document.getElementById("runSummaryPanel");
  const devDetails = document.querySelector("#canvasPanel .canvas-debug details");
  if(mount && worldPanel){ mount.appendChild(worldPanel); }
  if(mount && runPanel){ mount.appendChild(runPanel); }
  const syncDeveloperPanels = ()=>{
    const show = !!(devDetails && devDetails.open);
    if(worldPanel){ worldPanel.style.display = show ? "block" : "none"; }
    if(runPanel){ runPanel.style.display = show ? "block" : "none"; }
  };
  if(devDetails){ devDetails.addEventListener("toggle", syncDeveloperPanels); }
  syncDeveloperPanels();
  detailsVisible = localStorage.getItem("lam_details_visible")==="1";
  strictRulesVisible = localStorage.getItem("lam_strict_rules_visible")==="1";
  const showDetailsBox = document.getElementById("showDetails");
  const showStrictBox = document.getElementById("showStrictRules");
  if(showDetailsBox){ showDetailsBox.checked = detailsVisible; }
  if(showStrictBox){ showStrictBox.checked = strictRulesVisible; }
  toggleDetails(detailsVisible);
  toggleStrictRules(strictRulesVisible);
  updateTimelineFilterButtons();
  renderHistory();
  await refreshState();
  const instructionInput=document.getElementById("instruction");
  if(instructionInput){
    instructionInput.addEventListener("keydown", (e)=>{
      if(e.key === "Enter" && !e.shiftKey){
        e.preventDefault();
        void runInstruction();
      }
    });
    instructionInput.addEventListener("input", ()=>{ void refreshFreshnessPreview(); });
  }
  await refreshFreshnessPreview();
  await loadDomainFreshnessPolicy();
  await vaultList();
};
</script>
</body>
</html>
"""


class _Handler(BaseHTTPRequestHandler):
    state: UiState

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)
        if path in {"/", "/index.html"}:
            self._send_text(200, HTML_PAGE, "text/html; charset=utf-8")
            return
        if path == "/assets/openlamb-logo.png":
            logo_path = Path("docs/assets/openlamb-logo.png")
            if not logo_path.exists():
                self._send_json(404, {"error": "not_found"})
                return
            self._send_bytes(200, logo_path.read_bytes(), "image/png")
            return
        if path == "/api/state":
            self._send_json(200, self.state.snapshot())
            return
        if path == "/api/history":
            self._send_json(200, {"history": self.state.snapshot()["history"]})
            return
        if path == "/api/history/export":
            snap = self.state.snapshot()
            data = json.dumps({"exported_at": time.time(), "history": snap["history"]}, indent=2)
            self._send_text(200, data, "application/json")
            return
        if path == "/api/artifact":
            requested = (qs.get("path", [""])[0] or "").strip()
            if not requested:
                self._send_json(400, {"error": "missing_path"})
                return
            workspace_root = Path.cwd().resolve()
            try:
                requested_path = Path(requested)
                resolved = (workspace_root / requested_path).resolve() if not requested_path.is_absolute() else requested_path.resolve()
            except Exception:
                self._send_json(400, {"error": "invalid_path"})
                return
            if not _is_path_within(resolved, workspace_root):
                self._send_json(403, {"error": "path_outside_workspace"})
                return
            if not resolved.exists():
                self._send_json(404, {"error": "not_found"})
                return
            if resolved.is_dir():
                listing = _render_directory_listing_html(resolved, workspace_root)
                self._send_text(200, listing, "text/html; charset=utf-8")
                return
            try:
                payload = resolved.read_bytes()
            except Exception:
                self._send_json(500, {"error": "read_failed"})
                return
            self._send_bytes(200, payload, _guess_content_type(resolved))
            return
        if path == "/api/task":
            task_id = (qs.get("id", [""])[0] or "").strip()
            with self.state.lock:
                task = dict(self.state.tasks.get(task_id, {})) if task_id else {}
            self._send_json(200, {"ok": bool(task), "task": task})
            return
        if path == "/api/reliability/result":
            with self.state.lock:
                task_id = self.state.reliability_suite_task_id
                task = dict(self.state.tasks.get(task_id, {})) if task_id else {}
                result = dict(self.state.reliability_suite_result)
            self._send_json(200, {"ok": bool(result), "task_id": task_id, "task": task, "result": result})
            return
        self._send_json(404, {"error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802
        payload = self._read_json()
        if self.path == "/api/control":
            accept = bool(payload.get("accept", False))
            with self.state.lock:
                self.state.control_granted = accept
                self.state.control_granted_at = time.time() if accept else 0.0
                if not accept:
                    self.state.paused_for_credentials = False
                    self.state.pause_reason = ""
                    self.state.pending_plan = {}
                    self.state.pending_auth_instruction = ""
                    self.state.pending_auth_url = ""
                    self.state.pending_auth_session_id = ""
            self._send_json(200, self.state.snapshot())
            return

        if self.path == "/api/settings":
            with self.state.lock:
                self.state.step_mode = bool(payload.get("step_mode", self.state.step_mode))
                self.state.manual_auth_phase = bool(payload.get("manual_auth_phase", self.state.manual_auth_phase))
                self.state.browser_worker_mode = normalize_browser_worker_mode(
                    str(payload.get("browser_worker_mode", self.state.browser_worker_mode))
                )
                self.state.human_like_interaction = bool(
                    payload.get("human_like_interaction", self.state.human_like_interaction)
                )
                self.state.ai_backend = normalize_backend(str(payload.get("ai_backend", self.state.ai_backend)))
                mode = str(payload.get("compression_mode", self.state.compression_mode)).strip().lower()
                if mode not in {"aggressive", "normal", "strict"}:
                    mode = self.state.compression_mode
                self.state.compression_mode = mode
                self.state.recorder.set_compression_mode(self.state.compression_mode)
                min_live = payload.get("min_live_non_curated_citations", self.state.min_live_non_curated_citations)
                try:
                    self.state.min_live_non_curated_citations = max(1, min(20, int(min_live)))
                except Exception:
                    pass
                reuse_mode = str(payload.get("artifact_reuse_mode", self.state.artifact_reuse_mode)).strip().lower()
                if reuse_mode not in {"reuse", "reuse_if_recent", "always_regenerate"}:
                    reuse_mode = self.state.artifact_reuse_mode
                self.state.artifact_reuse_mode = reuse_mode
                reuse_hours = payload.get("artifact_reuse_max_age_hours", self.state.artifact_reuse_max_age_hours)
                try:
                    self.state.artifact_reuse_max_age_hours = max(1, min(720, int(reuse_hours)))
                except Exception:
                    pass
                self.state.use_domain_freshness_defaults = bool(
                    payload.get("use_domain_freshness_defaults", self.state.use_domain_freshness_defaults)
                )
                _save_user_defaults_locked(self.state)
            self._send_json(200, self.state.snapshot())
            return

        if self.path == "/api/policy/freshness/get":
            domain = str(payload.get("domain", "")).strip()
            defaults = _load_policy_freshness_defaults()
            domains = defaults.get("domains", {}) if isinstance(defaults.get("domains"), dict) else {}
            cfg = domains.get(domain, {}) if isinstance(domains.get(domain), dict) else {}
            mode = str(cfg.get("artifact_reuse_mode", "reuse_if_recent"))
            hours = int(cfg.get("artifact_reuse_max_age_hours", 72))
            self._send_json(200, {"ok": True, "domain": domain, "mode": mode, "max_age_hours": hours, "enabled": bool(defaults.get("enabled", True))})
            return

        if self.path == "/api/policy/freshness/preview":
            instruction = str(payload.get("instruction", "")).strip()
            requested_mode = str(payload.get("artifact_reuse_mode", self.state.artifact_reuse_mode))
            requested_hours = payload.get("artifact_reuse_max_age_hours", self.state.artifact_reuse_max_age_hours)
            use_defaults = bool(payload.get("use_domain_freshness_defaults", self.state.use_domain_freshness_defaults))
            mode, hours, domain, source = _resolve_domain_freshness_defaults(
                instruction=instruction,
                requested_mode=requested_mode,
                requested_hours=requested_hours,
                use_domain_defaults=use_defaults,
            )
            self._send_json(
                200,
                {
                    "ok": True,
                    "instruction": instruction,
                    "domain": domain,
                    "mode": mode,
                    "max_age_hours": hours,
                    "source": source,
                    "use_domain_freshness_defaults": use_defaults,
                },
            )
            return

        if self.path == "/api/policy/freshness/set":
            domain = str(payload.get("domain", "")).strip()
            mode = str(payload.get("mode", "")).strip().lower()
            max_age_hours = payload.get("max_age_hours", 72)
            result = _set_policy_freshness_domain(domain=domain, mode=mode, max_age_hours=max_age_hours)
            status = 200 if bool(result.get("ok", False)) else 400
            self._send_json(status, result)
            return

        if self.path == "/api/history/clear":
            with self.state.lock:
                self.state.history = []
                _save_history(self.state.history)
            self._send_json(200, {"ok": True, "cleared": True})
            return

        if self.path == "/api/feedback":
            entry = {
                "session_id": str(payload.get("session_id", "")),
                "task_id": str(payload.get("task_id", "")),
                "message_id": str(payload.get("message_id", "")),
                "rating": int(payload.get("rating", 0) or 0),
                "reason": str(payload.get("reason", "")),
                "comment": str(payload.get("comment", "")),
                "timestamp": float(payload.get("ts", time.time()) or time.time()),
            }
            _append_feedback(entry)
            self._send_json(200, {"ok": True, "saved": True})
            return

        if self.path == "/api/apps/search":
            q = str(payload.get("query", ""))
            self._send_json(200, {"ok": True, "apps": list_installed_apps(query=q, limit=50)})
            return

        if self.path == "/api/vault/status":
            self._send_json(200, self.state.vault.status())
            return

        if self.path == "/api/vault/list":
            query = str(payload.get("query", "")).strip()
            tag = str(payload.get("tag", "")).strip().lower()
            favorite_only = bool(payload.get("favorite_only", False))
            self._send_json(200, {"ok": True, "entries": self.state.vault.list_entries(query=query, tag=tag, favorite_only=favorite_only)})
            return

        if self.path == "/api/vault/save":
            service = str(payload.get("service", "")).strip()
            username = str(payload.get("username", ""))
            password = str(payload.get("password", ""))
            if not service or not username or not password:
                self._send_json(400, {"ok": False, "error": "service, username, password are required"})
                return
            tags = payload.get("tags", [])
            if not isinstance(tags, list):
                tags = []
            result = self.state.vault.put_entry(
                service=service,
                username=username,
                password=password,
                notes=str(payload.get("notes", "")),
                tags=[str(x) for x in tags],
                favorite=bool(payload.get("favorite", False)),
                entry_id=str(payload.get("id", "")).strip() or None,
            )
            self._send_json(200, result)
            return

        if self.path == "/api/vault/delete":
            entry_id = str(payload.get("id", "")).strip()
            self._send_json(200, self.state.vault.delete_entry(entry_id))
            return

        if self.path == "/api/vault/generate":
            result = self.state.vault.generate_password(
                length=int(payload.get("length", 20)),
                include_upper=bool(payload.get("include_upper", True)),
                include_lower=bool(payload.get("include_lower", True)),
                include_digits=bool(payload.get("include_digits", True)),
                include_symbols=bool(payload.get("include_symbols", True)),
                exclude_ambiguous=bool(payload.get("exclude_ambiguous", True)),
            )
            self._send_json(200, result)
            return

        if self.path == "/api/vault/fill":
            service = str(payload.get("service", "")).strip()
            submit = bool(payload.get("submit", False))
            with self.state.lock:
                granted = self.state.control_granted
            if not granted:
                self._send_json(403, {"ok": False, "error": "Control not granted. Click Accept Control first."})
                return
            resolved = self.state.vault.find_entry_by_service(service)
            if not resolved.get("ok"):
                self._send_json(404, resolved)
                return
            entry = resolved.get("entry", {})
            try:
                adapter = UIAAdapter(allow_input_fallback=True, dry_run=False)
                adapter.type({}, str(entry.get("username", "")))
                adapter.hotkey("TAB")
                adapter.type({}, str(entry.get("password", "")))
                if submit:
                    adapter.hotkey("ENTER")
                self.state.vault.touch_used(str(entry.get("id", "")))
            except Exception as exc:  # pylint: disable=broad-exception-caught
                self._send_json(500, {"ok": False, "error": str(exc)})
                return
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": entry.get("service", ""),
                    "username_masked": (str(entry.get("username", ""))[:2] + "***") if entry.get("username") else "",
                    "submitted": submit,
                },
            )
            return

        if self.path == "/api/vault/export":
            out_path = str(payload.get("path", "data/interface/vault_export.lamvault"))
            self._send_json(200, self.state.vault.export_encrypted(out_path))
            return

        if self.path == "/api/vault/import":
            in_path = str(payload.get("path", ""))
            merge = bool(payload.get("merge", True))
            self._send_json(200, self.state.vault.import_encrypted(in_path, merge=merge))
            return

        if self.path == "/api/preview":
            instruction = str(payload.get("instruction", "")).strip()
            self._send_json(200, preview_instruction(instruction))
            return

        if self.path == "/api/clipboard/capture":
            instruction = str(payload.get("instruction", "")).strip() or "Capture the current clipboard image and save it as an artifact package with base64 output."
            with self.state.lock:
                granted = self.state.control_granted
                step_mode = self.state.step_mode
                manual_auth_phase = self.state.manual_auth_phase
                browser_worker_mode = self.state.browser_worker_mode
                human_like_interaction = self.state.human_like_interaction
                ai_backend = self.state.ai_backend
                min_live = self.state.min_live_non_curated_citations
                reuse_mode = self.state.artifact_reuse_mode
                reuse_hours = self.state.artifact_reuse_max_age_hours
                use_domain_defaults = self.state.use_domain_freshness_defaults
            if not granted:
                self._send_json(400, {"ok": False, "error": "Control not granted. Click Accept Control first."})
                return
            reuse_mode, reuse_hours, _domain, _source = _resolve_domain_freshness_defaults(
                instruction=instruction,
                requested_mode=reuse_mode,
                requested_hours=reuse_hours,
                use_domain_defaults=use_domain_defaults,
            )
            result = execute_instruction(
                instruction=instruction,
                control_granted=True,
                step_mode=step_mode,
                confirm_risky=False,
                ai_backend=ai_backend,
                min_live_non_curated_citations=min_live,
                manual_auth_phase=manual_auth_phase,
                browser_worker_mode=browser_worker_mode,
                human_like_interaction=human_like_interaction,
                artifact_reuse_mode=reuse_mode,
                artifact_reuse_max_age_hours=reuse_hours,
            )
            with self.state.lock:
                self.state.history.append(dict(result))
                self.state.history = self.state.history[-300:]
                _save_history(self.state.history)
            self._send_json(200, result)
            return

        if self.path == "/api/reliability/run":
            include_pytest = bool(payload.get("include_pytest", False))
            include_desktop_smoke = bool(payload.get("include_desktop_smoke", False))
            pytest_args = payload.get("pytest_args", [])
            timeout_seconds = int(payload.get("pytest_timeout_seconds", 300))
            if not isinstance(pytest_args, list):
                pytest_args = []
            task_id = _start_reliability_suite_task(
                state=self.state,
                include_pytest=include_pytest,
                include_desktop_smoke=include_desktop_smoke,
                pytest_args=[str(arg) for arg in pytest_args],
                pytest_timeout_seconds=max(30, min(3600, timeout_seconds)),
            )
            self._send_json(
                200,
                {
                    "ok": True,
                    "task_id": task_id,
                    "mode": "reliability_suite",
                    "canvas": {
                        "title": "Reliability Suite Started",
                        "subtitle": "Running scenario checks now.",
                        "cards": [],
                    },
                },
            )
            return

        if self.path == "/api/benchmark/score_last_run":
            with self.state.lock:
                latest = dict(self.state.history[-1] or {}) if self.state.history else {}
            if not latest:
                self._send_json(
                    400,
                    {
                        "ok": False,
                        "error": "No run result available to benchmark.",
                        "mode": "human_operator_benchmark",
                    },
                )
                return
            bench = benchmark_from_last_run(result=latest)
            with self.state.lock:
                self.state.benchmark_last_result = dict(bench)
                self.state.history.append(bench)
                self.state.history = self.state.history[-300:]
                _save_history(self.state.history)
            self._send_json(200, bench)
            return

        if self.path == "/api/benchmark/run_20_suite":
            task_id = _start_human_operator_20_suite_task(self.state)
            self._send_json(
                200,
                {
                    "ok": True,
                    "task_id": task_id,
                    "mode": "human_operator_20_test_suite",
                    "canvas": {
                        "title": "Human 20-Test Suite Started",
                        "subtitle": "Running scenarios sequentially with hard stop on first failure.",
                        "cards": [],
                    },
                },
            )
            return

        if self.path == "/api/benchmark/run_killer_suite":
            task_id = _start_human_operator_killer_suite_task(self.state)
            self._send_json(
                200,
                {
                    "ok": True,
                    "task_id": task_id,
                    "mode": "human_operator_killer_5_suite",
                    "canvas": {
                        "title": "Human Killer 5 Suite Started",
                        "subtitle": "Running K1..K5 sequentially with hard stop on first failure.",
                        "cards": [],
                    },
                },
            )
            return

        if self.path == "/api/smoke/notepad":
            with self.state.lock:
                granted = self.state.control_granted
            if not granted:
                self._send_json(403, {"ok": False, "error": "Control not granted. Click Accept Control first."})
                return
            task_id = _start_notepad_smoke_task(self.state)
            self._send_json(
                200,
                {
                    "ok": True,
                    "task_id": task_id,
                    "mode": "notepad_smoke",
                    "canvas": {
                        "title": "Notepad Smoke Started",
                        "subtitle": "Opening Notepad and typing hello world.",
                        "cards": [],
                    },
                },
            )
            return

        if self.path == "/api/selector/capture":
            cap = capture_selector_at_cursor().to_dict()
            with self.state.lock:
                if cap.get("ok"):
                    self.state.last_selector = cap.get("selector", {}) or {}
            self._send_json(200, cap)
            return

        if self.path == "/api/teach/start":
            app_name = str(payload.get("app_name", "")).strip()
            with self.state.lock:
                self.state.recorder.set_compression_mode(self.state.compression_mode)
            self._send_json(200, self.state.recorder.start(app_name=app_name))
            return

        if self.path == "/api/teach/global_start":
            if self.state.global_hooks is None:
                self.state.global_hooks = GlobalTeachHooks(self.state.recorder)
            self._send_json(200, self.state.global_hooks.start())
            return

        if self.path == "/api/teach/global_stop":
            if self.state.global_hooks is None:
                self._send_json(200, {"ok": True, "active": False})
                return
            self._send_json(200, self.state.global_hooks.stop())
            return

        if self.path == "/api/teach/add_click":
            with self.state.lock:
                selector = dict(self.state.last_selector)
            if not selector:
                cap = capture_selector_at_cursor().to_dict()
                if cap.get("ok"):
                    selector = cap.get("selector", {}) or {}
                    with self.state.lock:
                        self.state.last_selector = selector
            self._send_json(200, self.state.recorder.capture_click(selector))
            return

        if self.path == "/api/teach/add_type":
            self._send_json(200, self.state.recorder.capture_type(str(payload.get("text", ""))))
            return

        if self.path == "/api/teach/add_hotkey":
            self._send_json(200, self.state.recorder.capture_hotkey(str(payload.get("keys", ""))))
            return

        if self.path == "/api/teach/add_wait":
            seconds = int(payload.get("seconds", 1))
            self._send_json(200, self.state.recorder.capture_wait(seconds))
            return

        if self.path == "/api/teach/stop":
            result = self.state.recorder.stop()
            if result.get("ok") and result.get("instruction"):
                with self.state.lock:
                    self.state.history.append(result)
                    self.state.history = self.state.history[-300:]
                    _save_history(self.state.history)
            self._send_json(200, result)
            return

        if self.path == "/api/automation/save":
            name = str(payload.get("name", "")).strip()
            instruction = str(payload.get("instruction", "")).strip()
            if not name or not instruction:
                self._send_json(400, {"ok": False, "error": "name and instruction are required"})
                return
            with self.state.lock:
                self.state.saved_automations[name] = instruction
                _save_automations(self.state.saved_automations)
            self._send_json(200, {"ok": True, "saved": name, "instruction": instruction})
            return

        if self.path == "/api/automation/run":
            name = str(payload.get("name", "")).strip()
            instruction = str(payload.get("instruction", "")).strip()
            ai_backend = normalize_backend(str(payload.get("ai_backend", "")))
            min_live = int(payload.get("min_live_non_curated_citations", 3))
            if not instruction:
                with self.state.lock:
                    instruction = self.state.saved_automations.get(name, "")
            payload = {
                "instruction": instruction,
                "confirm_risky": bool(payload.get("confirm_risky", False)),
                "ai_backend": ai_backend,
                "min_live_non_curated_citations": min_live,
            }
            self.path = "/api/instruct"

        if self.path == "/api/automation/run_async":
            name = str(payload.get("name", "")).strip()
            instruction = str(payload.get("instruction", "")).strip()
            ai_backend = normalize_backend(str(payload.get("ai_backend", "")))
            min_live = int(payload.get("min_live_non_curated_citations", 3))
            manual_auth_phase = bool(payload.get("manual_auth_phase", self.state.manual_auth_phase))
            browser_worker_mode = normalize_browser_worker_mode(
                str(payload.get("browser_worker_mode", self.state.browser_worker_mode))
            )
            human_like_interaction = bool(payload.get("human_like_interaction", self.state.human_like_interaction))
            artifact_reuse_mode = str(payload.get("artifact_reuse_mode", self.state.artifact_reuse_mode)).strip().lower()
            if artifact_reuse_mode not in {"reuse", "reuse_if_recent", "always_regenerate"}:
                artifact_reuse_mode = self.state.artifact_reuse_mode
            try:
                artifact_reuse_max_age_hours = max(1, min(720, int(payload.get("artifact_reuse_max_age_hours", self.state.artifact_reuse_max_age_hours))))
            except Exception:
                artifact_reuse_max_age_hours = self.state.artifact_reuse_max_age_hours
            with self.state.lock:
                if not instruction:
                    instruction = self.state.saved_automations.get(name, "")
                preflight_error = _preflight_gate_error_locked(self.state)
            if preflight_error:
                self._send_json(412, _preflight_block_response(preflight_error))
                return
            if not instruction:
                self._send_json(404, {"ok": False, "error": f"Automation '{name}' not found."})
                return
            task_id = _start_instruction_task(
                state=self.state,
                instruction=instruction,
                confirm_risky=bool(payload.get("confirm_risky", False)),
                ai_backend=ai_backend,
                min_live_non_curated_citations=min_live,
                manual_auth_phase=manual_auth_phase,
                browser_worker_mode=browser_worker_mode,
                human_like_interaction=human_like_interaction,
                use_domain_freshness_defaults=bool(payload.get("use_domain_freshness_defaults", self.state.use_domain_freshness_defaults)),
                artifact_reuse_mode=artifact_reuse_mode,
                artifact_reuse_max_age_hours=artifact_reuse_max_age_hours,
            )
            self._send_json(200, {"ok": True, "task_id": task_id, "instruction": instruction})
            return

        if self.path == "/api/schedules/add":
            if not self.state.scheduler:
                self._send_json(500, {"ok": False, "error": "Scheduler unavailable"})
                return
            job = self.state.scheduler.add_job(
                name=str(payload.get("name", "")),
                automation_name=str(payload.get("automation_name", "")),
                kind=str(payload.get("kind", "interval")),
                value=str(payload.get("value", "")),
            )
            self._send_json(200, {"ok": True, "job": job.to_dict()})
            return

        if self.path == "/api/schedules/delete":
            if not self.state.scheduler:
                self._send_json(500, {"ok": False, "error": "Scheduler unavailable"})
                return
            job_id = str(payload.get("id", ""))
            self._send_json(200, {"ok": self.state.scheduler.delete_job(job_id)})
            return

        if self.path == "/api/schedules/trigger":
            if not self.state.scheduler:
                self._send_json(500, {"ok": False, "error": "Scheduler unavailable"})
                return
            event_name = str(payload.get("event", "manual")).strip().lower()
            self.state.scheduler.trigger_event(event_name)
            self._send_json(200, {"ok": True, "triggered": event_name})
            return

        if self.path == "/api/session/resume":
            with self.state.lock:
                pending = dict(self.state.pending_plan)
                auth_instruction = str(self.state.pending_auth_instruction or "")
                auth_url = str(self.state.pending_auth_url or "")
                auth_session_id = str(self.state.pending_auth_session_id or "")
                step_mode = self.state.step_mode
                human_like_interaction = self.state.human_like_interaction
                self.state.paused_for_credentials = False
                self.state.pause_reason = ""
            if pending:
                result = resume_pending_plan(
                    pending,
                    step_mode=step_mode,
                    human_like_interaction=human_like_interaction,
                )
                with self.state.lock:
                    self.state.pending_plan = result.get("pending_plan") or {}
                    self.state.paused_for_credentials = bool(result.get("paused_for_credentials", False))
                    self.state.pause_reason = str(result.get("pause_reason", "")) if self.state.paused_for_credentials else ""
                    if self.state.paused_for_credentials:
                        self.state.pending_auth_instruction = auth_instruction or self.state.pending_auth_instruction
                        next_auth_url = str(result.get("opened_url", "") or auth_url)
                        if _looks_like_gmail_auth_instruction(self.state.pending_auth_instruction):
                            next_auth_url = _sanitize_gmail_auth_url(next_auth_url)
                        self.state.pending_auth_url = next_auth_url
                        self.state.pending_auth_session_id = str(result.get("auth_session_id", "") or auth_session_id)
                    else:
                        self.state.pending_auth_instruction = ""
                        self.state.pending_auth_url = ""
                        self.state.pending_auth_session_id = ""
                    _apply_auth_loop_tracking_locked(self.state, result)
                    if result.get("ok"):
                        self.state.history.append(result)
                        self.state.history = self.state.history[-300:]
                        _save_history(self.state.history)
                self._send_json(200, result)
                return
            if auth_instruction:
                with self.state.lock:
                    if self.state.auth_loop_blocked:
                        blocked = _auth_loop_block_response(self.state)
                        self.state.paused_for_credentials = True
                        self.state.pause_reason = str(blocked.get("pause_reason", ""))
                        self._send_json(429, blocked)
                        return
                task_id = _start_instruction_task(
                    state=self.state,
                    instruction=auth_instruction,
                    confirm_risky=False,
                    ai_backend=self.state.ai_backend,
                    min_live_non_curated_citations=self.state.min_live_non_curated_citations,
                    manual_auth_phase=False,
                    browser_worker_mode=self.state.browser_worker_mode,
                    human_like_interaction=self.state.human_like_interaction,
                    use_domain_freshness_defaults=self.state.use_domain_freshness_defaults,
                    artifact_reuse_mode=self.state.artifact_reuse_mode,
                    artifact_reuse_max_age_hours=self.state.artifact_reuse_max_age_hours,
                    auth_session_id=auth_session_id,
                )
                self._send_json(200, {"ok": True, "task_id": task_id, "instruction": auth_instruction})
                return
            self._send_json(200, {"ok": True, "message": "No pending sequence."})
            return

        if self.path == "/api/session/focus_auth":
            with self.state.lock:
                auth_session_id = str(self.state.pending_auth_session_id or "")
                auth_url = str(self.state.pending_auth_url or "https://mail.google.com/")
                auth_instruction = str(self.state.pending_auth_instruction or "")
                # User took explicit recovery action; allow another resume attempt.
                self.state.auth_loop_blocked = False
            auth_url = _sanitize_focus_auth_url(auth_url, instruction=auth_instruction)
            focused = focus_auth_session(auth_session_id=auth_session_id, fallback_url=auth_url, allow_reopen=True)
            if focused.get("ok"):
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "mode": "auth_focus",
                        "opened_url": focused.get("opened_url", auth_url),
                        "canvas": {
                            "title": "Auth Tab Focused",
                            "subtitle": "Complete login if needed, then click Resume.",
                            "cards": [],
                        },
                    },
                )
                return
            if auth_url:
                webbrowser.open(auth_url, new=2)
            self._send_json(
                200,
                {
                    "ok": False,
                    "mode": "auth_focus",
                    "opened_url": auth_url,
                    "error": focused.get("error", "auth_session_not_found"),
                    "canvas": {
                        "title": "Auth Window Opened",
                        "subtitle": "Complete login in the opened tab, then click Resume.",
                        "cards": [],
                    },
                },
            )
            return

        if self.path == "/api/session/reset":
            with self.state.lock:
                self.state.paused_for_credentials = False
                self.state.pause_reason = ""
                self.state.pending_plan = {}
                self.state.pending_auth_instruction = ""
                self.state.pending_auth_url = ""
                self.state.pending_auth_session_id = ""
                self.state.auth_loop_signature = ""
                self.state.auth_loop_count = 0
                self.state.auth_loop_blocked = False
            self._send_json(
                200,
                {
                    "ok": True,
                    "mode": "session_reset",
                    "message": "Paused/auth session state cleared.",
                    "canvas": {
                        "title": "Session Reset",
                        "subtitle": "Auth checkpoint state cleared.",
                        "cards": [],
                    },
                },
            )
            return

        if self.path == "/api/instruct_async":
            instruction = str(payload.get("instruction", "")).strip()
            confirm_risky = bool(payload.get("confirm_risky", False))
            ai_backend = normalize_backend(str(payload.get("ai_backend", "")))
            min_live = int(payload.get("min_live_non_curated_citations", 3))
            manual_auth_phase = bool(payload.get("manual_auth_phase", self.state.manual_auth_phase))
            browser_worker_mode = normalize_browser_worker_mode(
                str(payload.get("browser_worker_mode", self.state.browser_worker_mode))
            )
            human_like_interaction = bool(payload.get("human_like_interaction", self.state.human_like_interaction))
            artifact_reuse_mode = str(payload.get("artifact_reuse_mode", self.state.artifact_reuse_mode)).strip().lower()
            if artifact_reuse_mode not in {"reuse", "reuse_if_recent", "always_regenerate"}:
                artifact_reuse_mode = self.state.artifact_reuse_mode
            try:
                artifact_reuse_max_age_hours = max(1, min(720, int(payload.get("artifact_reuse_max_age_hours", self.state.artifact_reuse_max_age_hours))))
            except Exception:
                artifact_reuse_max_age_hours = self.state.artifact_reuse_max_age_hours
            with self.state.lock:
                preflight_error = _preflight_gate_error_locked(self.state)
            if preflight_error:
                self._send_json(412, _preflight_block_response(preflight_error))
                return
            task_id = _start_instruction_task(
                state=self.state,
                instruction=instruction,
                confirm_risky=confirm_risky,
                ai_backend=ai_backend,
                min_live_non_curated_citations=min_live,
                manual_auth_phase=manual_auth_phase,
                browser_worker_mode=browser_worker_mode,
                human_like_interaction=human_like_interaction,
                use_domain_freshness_defaults=bool(payload.get("use_domain_freshness_defaults", self.state.use_domain_freshness_defaults)),
                artifact_reuse_mode=artifact_reuse_mode,
                artifact_reuse_max_age_hours=artifact_reuse_max_age_hours,
            )
            self._send_json(200, {"ok": True, "task_id": task_id})
            return

        if self.path == "/api/instruct":
            instruction = str(payload.get("instruction", "")).strip()
            confirm_risky = bool(payload.get("confirm_risky", False))
            with self.state.lock:
                granted = self.state.control_granted
                paused = self.state.paused_for_credentials
                preflight_error = _preflight_gate_error_locked(self.state)
                step_mode = self.state.step_mode
                manual_auth_phase = self.state.manual_auth_phase
                browser_worker_mode = self.state.browser_worker_mode
                human_like_interaction = self.state.human_like_interaction
                auth_session_id = str(self.state.pending_auth_session_id or "")
                ai_backend = normalize_backend(str(payload.get("ai_backend", self.state.ai_backend)))
                min_live = max(1, min(20, int(payload.get("min_live_non_curated_citations", self.state.min_live_non_curated_citations))))
                artifact_reuse_mode = str(payload.get("artifact_reuse_mode", self.state.artifact_reuse_mode)).strip().lower()
                if artifact_reuse_mode not in {"reuse", "reuse_if_recent", "always_regenerate"}:
                    artifact_reuse_mode = self.state.artifact_reuse_mode
                try:
                    artifact_reuse_max_age_hours = max(1, min(720, int(payload.get("artifact_reuse_max_age_hours", self.state.artifact_reuse_max_age_hours))))
                except Exception:
                    artifact_reuse_max_age_hours = self.state.artifact_reuse_max_age_hours
                use_domain_freshness_defaults = bool(
                    payload.get("use_domain_freshness_defaults", self.state.use_domain_freshness_defaults)
                )
            artifact_reuse_mode, artifact_reuse_max_age_hours, _domain, _source = _resolve_domain_freshness_defaults(
                instruction=instruction,
                requested_mode=artifact_reuse_mode,
                requested_hours=artifact_reuse_max_age_hours,
                use_domain_defaults=use_domain_freshness_defaults,
            )
            if preflight_error:
                self._send_json(412, _preflight_block_response(preflight_error))
                return
            if paused:
                self._send_json(409, {"ok": False, "error": "Session paused for credential entry. Click Resume."})
                return
            result = execute_instruction(
                instruction=instruction,
                control_granted=granted,
                step_mode=step_mode,
                confirm_risky=confirm_risky,
                ai_backend=ai_backend,
                min_live_non_curated_citations=min_live,
                manual_auth_phase=manual_auth_phase,
                auth_session_id=auth_session_id,
                browser_worker_mode=browser_worker_mode,
                human_like_interaction=human_like_interaction,
                artifact_reuse_mode=artifact_reuse_mode,
                artifact_reuse_max_age_hours=artifact_reuse_max_age_hours,
            )
            with self.state.lock:
                if result.get("ok"):
                    self.state.history.append(result)
                    self.state.history = self.state.history[-300:]
                    _save_history(self.state.history)
                self.state.pending_plan = result.get("pending_plan") or {}
                self.state.paused_for_credentials = bool(result.get("paused_for_credentials", False))
                self.state.pause_reason = str(result.get("pause_reason", "")) if self.state.paused_for_credentials else ""
                if self.state.paused_for_credentials:
                    self.state.pending_auth_instruction = instruction
                    self.state.pending_auth_url = _sanitize_focus_auth_url(
                        str(result.get("opened_url", "") or ""),
                        instruction=instruction,
                    )
                    self.state.pending_auth_session_id = str(result.get("auth_session_id", "") or "")
                else:
                    self.state.pending_auth_instruction = ""
                    self.state.pending_auth_url = ""
                    self.state.pending_auth_session_id = ""
                _apply_auth_loop_tracking_locked(self.state, result)
            self._send_json(200, result)
            return

        self._send_json(404, {"error": "not_found"})

    def _read_json(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        raw = self.rfile.read(length).decode("utf-8", errors="ignore")
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}

    def _send_json(self, status: int, payload: Dict[str, Any]) -> None:
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_text(self, status: int, payload: str, content_type: str) -> None:
        data = payload.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_bytes(self, status: int, payload: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt: str, *args: Any) -> None:
        _ = (fmt, args)


def run_ui_server(host: str = "127.0.0.1", port: int = 8795, open_browser: bool = True) -> None:
    state = UiState(saved_automations=_load_automations(), history=_load_history())
    state.global_hooks = GlobalTeachHooks(state.recorder)
    _apply_user_defaults(state)
    # Chat-first product mode: do not block normal delegation behind reliability gates.
    state.preflight_required = False

    def scheduler_run(job: ScheduleJob) -> Dict[str, Any]:
        with state.lock:
            instruction = state.saved_automations.get(job.automation_name, "")
            granted = state.control_granted
            preflight_error = _preflight_gate_error_locked(state)
            step_mode = state.step_mode
            manual_auth_phase = state.manual_auth_phase
            browser_worker_mode = state.browser_worker_mode
            human_like_interaction = state.human_like_interaction
            ai_backend = state.ai_backend
            min_live = state.min_live_non_curated_citations
            reuse_mode = state.artifact_reuse_mode
            reuse_hours = state.artifact_reuse_max_age_hours
            use_domain_defaults = state.use_domain_freshness_defaults
        if not granted:
            return {"ok": False, "error": "Control not granted; scheduled run skipped."}
        if preflight_error:
            return _preflight_block_response(preflight_error)
        if not instruction:
            return {"ok": False, "error": f"Automation '{job.automation_name}' not found."}
        reuse_mode, reuse_hours, _domain, _source = _resolve_domain_freshness_defaults(
            instruction=instruction,
            requested_mode=reuse_mode,
            requested_hours=reuse_hours,
            use_domain_defaults=use_domain_defaults,
        )
        result = execute_instruction(
            instruction=instruction,
            control_granted=True,
            step_mode=step_mode,
            confirm_risky=True,
            ai_backend=ai_backend,
            min_live_non_curated_citations=min_live,
            manual_auth_phase=manual_auth_phase,
            browser_worker_mode=browser_worker_mode,
            human_like_interaction=human_like_interaction,
            artifact_reuse_mode=reuse_mode,
            artifact_reuse_max_age_hours=reuse_hours,
        )
        with state.lock:
            if result.get("ok"):
                state.history.append({"mode": "scheduled_run", "job": job.to_dict(), "result": result})
                state.history = state.history[-300:]
                _save_history(state.history)
            state.pending_plan = result.get("pending_plan") or state.pending_plan
            if result.get("paused_for_credentials"):
                state.paused_for_credentials = True
                state.pause_reason = str(result.get("pause_reason", ""))
                state.pending_auth_instruction = instruction
                state.pending_auth_url = _sanitize_focus_auth_url(
                    str(result.get("opened_url", "") or ""),
                    instruction=instruction,
                )
                state.pending_auth_session_id = str(result.get("auth_session_id", "") or "")
            else:
                state.paused_for_credentials = False
                state.pause_reason = ""
                state.pending_auth_instruction = ""
                state.pending_auth_url = ""
                state.pending_auth_session_id = ""
            _apply_auth_loop_tracking_locked(state, result)
        return result

    scheduler = ScheduleEngine(
        storage_path="data/interface/schedules.json",
        run_callback=scheduler_run,
    )
    scheduler.start()
    state.scheduler = scheduler

    class Handler(_Handler):
        pass

    Handler.state = state
    server = ThreadingHTTPServer((host, port), Handler)
    url = f"http://{host}:{port}"
    if open_browser:
        webbrowser.open(url, new=2)
    print(f"LAM UI running at {url}")
    try:
        server.serve_forever()
    finally:
        if state.global_hooks:
            state.global_hooks.stop()
        scheduler.stop()


def _automations_path() -> Path:
    path = Path("data/interface/automations.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _load_automations() -> Dict[str, str]:
    path = _automations_path()
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_automations(data: Dict[str, str]) -> None:
    _automations_path().write_text(json.dumps(data, indent=2), encoding="utf-8")


def _history_path() -> Path:
    path = Path("data/interface/history.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _load_history() -> List[Dict[str, Any]]:
    path = _history_path()
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(raw, list):
            return raw[-300:]
        return []
    except Exception:
        return []


def _save_history(history: List[Dict[str, Any]]) -> None:
    _history_path().write_text(json.dumps(history[-300:], indent=2), encoding="utf-8")


def _feedback_path() -> Path:
    path = Path("data/interface/feedback.jsonl")
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _append_feedback(entry: Dict[str, Any]) -> None:
    safe = {
        "session_id": str(entry.get("session_id", "")),
        "task_id": str(entry.get("task_id", "")),
        "message_id": str(entry.get("message_id", "")),
        "rating": int(entry.get("rating", 0) or 0),
        "reason": str(entry.get("reason", "")),
        "comment": str(entry.get("comment", "")),
        "timestamp": float(entry.get("timestamp", time.time()) or time.time()),
    }
    line = json.dumps(safe, ensure_ascii=False)
    _feedback_path().open("a", encoding="utf-8").write(line + "\n")


def _is_path_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except Exception:
        return False


def _guess_content_type(path: Path) -> str:
    guessed, _enc = mimetypes.guess_type(str(path))
    if guessed:
        return guessed
    suffix = path.suffix.lower()
    if suffix in {".md", ".txt", ".log", ".yaml", ".yml"}:
        return "text/plain; charset=utf-8"
    if suffix == ".csv":
        return "text/csv; charset=utf-8"
    if suffix == ".json":
        return "application/json"
    return "application/octet-stream"


def _render_directory_listing_html(dir_path: Path, workspace_root: Path) -> str:
    rel = str(dir_path.resolve().relative_to(workspace_root.resolve())) if _is_path_within(dir_path, workspace_root) else str(dir_path.resolve())
    children = sorted(
        list(dir_path.iterdir()),
        key=lambda p: (not p.is_dir(), p.name.lower()),
    )[:400]
    rows: List[str] = []
    parent = dir_path.parent.resolve()
    if _is_path_within(parent, workspace_root):
        parent_href = f"/api/artifact?path={quote(str(parent))}"
        rows.append(f"<div><a href=\"{parent_href}\">.. (parent)</a></div>")
    for child in children:
        name = child.name + ("/" if child.is_dir() else "")
        href = f"/api/artifact?path={quote(str(child.resolve()))}"
        rows.append(f"<div><a href=\"{href}\">{html_lib.escape(name)}</a></div>")
    body = "\n".join(rows) if rows else "<div>No files.</div>"
    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Artifact Directory</title>
<style>
body{{font-family:'Segoe UI',Tahoma,sans-serif;background:#f8fafc;color:#0f172a;margin:0}}
.wrap{{max-width:980px;margin:0 auto;padding:20px}}
.card{{background:#fff;border:1px solid #e2e8f0;border-radius:12px;padding:14px}}
a{{color:#0f766e;text-decoration:none}}
.meta{{font-size:12px;color:#64748b;margin-bottom:10px}}
</style></head><body><div class="wrap"><div class="card">
<h2 style="margin:0 0 8px 0;">Artifact Directory</h2>
<div class="meta">{html_lib.escape(rel)}</div>
{body}
</div></div></body></html>"""


def _apply_user_defaults(state: UiState) -> None:
    defaults = load_defaults(user=state.user_id)
    step_mode = bool(defaults.get("step_mode", state.step_mode))
    manual_auth_phase = bool(defaults.get("manual_auth_phase", state.manual_auth_phase))
    policy_worker = _load_policy_browser_worker_defaults()
    browser_worker_mode = normalize_browser_worker_mode(
        str(defaults.get("browser_worker_mode", policy_worker.get("browser_worker_mode", state.browser_worker_mode)))
    )
    human_like_interaction = bool(
        defaults.get("human_like_interaction", policy_worker.get("human_like_interaction", state.human_like_interaction))
    )
    ai_backend = normalize_backend(str(defaults.get("ai_backend", state.ai_backend)))
    compression_mode = str(defaults.get("compression_mode", state.compression_mode)).strip().lower()
    policy_default = _load_policy_min_live_non_curated_citations()
    min_live = defaults.get("min_live_non_curated_citations", policy_default)
    artifact_reuse_mode = str(defaults.get("artifact_reuse_mode", state.artifact_reuse_mode)).strip().lower()
    artifact_reuse_max_age_hours = defaults.get("artifact_reuse_max_age_hours", state.artifact_reuse_max_age_hours)
    use_domain_freshness_defaults = bool(defaults.get("use_domain_freshness_defaults", state.use_domain_freshness_defaults))
    try:
        min_live_val = max(1, min(20, int(min_live)))
    except Exception:
        min_live_val = policy_default
    if compression_mode not in {"aggressive", "normal", "strict"}:
        compression_mode = "normal"
    if artifact_reuse_mode not in {"reuse", "reuse_if_recent", "always_regenerate"}:
        artifact_reuse_mode = "reuse_if_recent"
    try:
        reuse_hours_val = max(1, min(720, int(artifact_reuse_max_age_hours)))
    except Exception:
        reuse_hours_val = 72
    with state.lock:
        state.step_mode = step_mode
        state.manual_auth_phase = manual_auth_phase
        state.browser_worker_mode = browser_worker_mode
        state.human_like_interaction = human_like_interaction
        state.ai_backend = ai_backend
        state.compression_mode = compression_mode
        state.min_live_non_curated_citations = min_live_val
        state.artifact_reuse_mode = artifact_reuse_mode
        state.artifact_reuse_max_age_hours = reuse_hours_val
        state.use_domain_freshness_defaults = use_domain_freshness_defaults
        state.recorder.set_compression_mode(compression_mode)


def _save_user_defaults_locked(state: UiState) -> None:
    save_defaults(
        {
            "step_mode": state.step_mode,
            "manual_auth_phase": state.manual_auth_phase,
            "browser_worker_mode": state.browser_worker_mode,
            "human_like_interaction": state.human_like_interaction,
            "ai_backend": state.ai_backend,
            "compression_mode": state.compression_mode,
            "min_live_non_curated_citations": state.min_live_non_curated_citations,
            "artifact_reuse_mode": state.artifact_reuse_mode,
            "artifact_reuse_max_age_hours": state.artifact_reuse_max_age_hours,
            "use_domain_freshness_defaults": state.use_domain_freshness_defaults,
        },
        user=state.user_id,
    )


def _load_policy_min_live_non_curated_citations(default_value: int = 3) -> int:
    policy_path = Path("config/policy.yaml")
    if not policy_path.exists():
        return default_value
    try:
        raw = yaml.safe_load(policy_path.read_text(encoding="utf-8")) or {}
        value = (((raw.get("policies", {}) or {}).get("competitor_analysis", {}) or {}).get("min_live_non_curated_citations", default_value))
        return max(1, min(20, int(value)))
    except Exception:
        return default_value


def _load_policy_yaml() -> Dict[str, Any]:
    policy_path = Path("config/policy.yaml")
    if not policy_path.exists():
        return {}
    try:
        raw = yaml.safe_load(policy_path.read_text(encoding="utf-8")) or {}
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def _save_policy_yaml(raw: Dict[str, Any]) -> None:
    policy_path = Path("config/policy.yaml")
    policy_path.parent.mkdir(parents=True, exist_ok=True)
    policy_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")


def _load_policy_browser_worker_defaults() -> Dict[str, Any]:
    raw = _load_policy_yaml()
    policies = raw.get("policies", {}) if isinstance(raw.get("policies"), dict) else {}
    worker_cfg = policies.get("browser_worker", {}) if isinstance(policies.get("browser_worker"), dict) else {}
    mode = normalize_browser_worker_mode(str(worker_cfg.get("mode", "local")))
    human_like = bool(worker_cfg.get("human_like_interaction", True))
    return {"browser_worker_mode": mode, "human_like_interaction": human_like}


def _looks_like_gmail_auth_instruction(text: str) -> bool:
    low = str(text or "").lower()
    return ("gmail" in low) or ("scan my inbox" in low) or ("inbox" in low and "email" in low)


def _sanitize_gmail_auth_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return "https://mail.google.com/"
    try:
        host = (urlparse(raw).netloc or "").lower()
    except Exception:
        return "https://mail.google.com/"
    if "mail.google.com" in host or "accounts.google.com" in host:
        return raw
    return "https://mail.google.com/"


def _sanitize_focus_auth_url(url: str, instruction: str = "") -> str:
    raw = str(url or "").strip()
    if not raw:
        return "https://mail.google.com/"
    try:
        host = (urlparse(raw).netloc or "").lower()
    except Exception:
        return "https://mail.google.com/"
    if _looks_like_gmail_auth_instruction(instruction):
        return _sanitize_gmail_auth_url(raw)
    # Guard against known bad Google redirect target observed in auth loops.
    if "myaccount.google.com" in host:
        return "https://mail.google.com/"
    return raw


def _auth_loop_signature(result: Dict[str, Any]) -> str:
    if not isinstance(result, dict):
        return ""
    paused = bool(result.get("paused_for_credentials", False))
    if not paused:
        return ""
    summary = result.get("summary", {}) if isinstance(result.get("summary"), dict) else {}
    source_status = result.get("source_status", {}) if isinstance(result.get("source_status"), dict) else {}
    parts = [
        str(result.get("mode", "")),
        str(result.get("error_code", "")),
        str(summary.get("error", "")),
        str(summary.get("imap_error_code", "")),
        str(result.get("pause_reason", ""))[:180],
        json.dumps(source_status, sort_keys=True)[:220],
    ]
    return "|".join(parts)


def _apply_auth_loop_tracking_locked(state: UiState, result: Dict[str, Any]) -> None:
    sig = _auth_loop_signature(result)
    if not sig:
        state.auth_loop_signature = ""
        state.auth_loop_count = 0
        state.auth_loop_blocked = False
        return
    if sig == state.auth_loop_signature:
        state.auth_loop_count = int(state.auth_loop_count or 0) + 1
    else:
        state.auth_loop_signature = sig
        state.auth_loop_count = 1
    state.auth_loop_blocked = state.auth_loop_count >= 3


def _auth_loop_block_response(state: UiState) -> Dict[str, Any]:
    count = int(state.auth_loop_count or 0)
    reason = (
        "Auth loop detected after repeated identical failures. "
        "Use Focus auth tab, complete login or inbox readiness, then click Resume once. "
        "If still blocked, click Reset Session."
    )
    return {
        "ok": False,
        "mode": "auth_loop_blocked",
        "error": "auth_loop_detected",
        "error_code": "auth_loop_detected",
        "paused_for_credentials": True,
        "pause_reason": reason,
        "auth_loop_count": count,
        "canvas": {
            "title": "Auth Loop Detected",
            "subtitle": f"Stopped after {count} repeated auth retries to avoid credit burn.",
            "cards": [],
        },
    }


def _extract_auth_error_code(result: Dict[str, Any]) -> str:
    if not isinstance(result, dict):
        return ""
    code = str(result.get("error_code", "")).strip()
    if code:
        return code
    summary = result.get("summary", {}) if isinstance(result.get("summary"), dict) else {}
    imap_code = str(summary.get("imap_error_code", "")).strip()
    if imap_code:
        return imap_code
    summary_err = str(summary.get("error", "")).strip()
    if summary_err:
        return summary_err
    return ""


def _recommend_mode_for_auth_error(error_code: str, current_mode: str) -> tuple[str, str, str]:
    code = str(error_code or "").strip().lower()
    mode = normalize_browser_worker_mode(str(current_mode or "local"))
    if code in {
        "browser_worker_unavailable",
        "docker_worker_not_ready",
        "docker_worker_start_failed",
        "docker_worker_attach_failed",
        "docker_worker_auth_required",
        "docker_worker_inbox_not_ready",
        "docker_workspace_redirect",
        "docker_worker_exception",
    }:
        return (
            "local",
            "high",
            "Docker worker cannot complete this auth flow reliably. Use local mode for interactive login.",
        )
    if code in {"auth_profile_locked", "browser_triage_exception"}:
        return (
            "docker",
            "medium",
            "Local browser profile/session appears unstable. Docker worker may provide a cleaner browser context.",
        )
    if code in {"imap_app_password_required"}:
        return (
            "local",
            "high",
            "IMAP requires an app password. Continue with Gmail UI auth in local mode or add app-password credentials.",
        )
    if code in {"auth_loop_detected", "credential_missing"}:
        return (
            mode,
            "medium",
            "Complete sign-in once in the focused auth tab, then resume once to avoid repeated retries.",
        )
    if code:
        return (mode, "low", "No strong mode preference detected from this error code.")
    return (mode, "low", "")


def _build_auth_recovery_recommendation_locked(state: UiState, current_task: Dict[str, Any]) -> Dict[str, Any]:
    result = current_task.get("result", {}) if isinstance(current_task.get("result"), dict) else {}
    error_code = _extract_auth_error_code(result)
    if state.auth_loop_blocked:
        error_code = "auth_loop_detected"
    if not error_code and state.paused_for_credentials:
        error_code = "credential_missing"
    recommended_mode, confidence, reason = _recommend_mode_for_auth_error(
        error_code=error_code,
        current_mode=state.browser_worker_mode,
    )
    show = bool(error_code or state.paused_for_credentials or state.auth_loop_blocked)
    return {
        "show": show,
        "error_code": error_code,
        "current_mode": normalize_browser_worker_mode(state.browser_worker_mode),
        "recommended_mode": recommended_mode,
        "confidence": confidence,
        "reason": reason,
        "pause_reason": str(state.pause_reason or ""),
    }


def _load_policy_freshness_defaults() -> Dict[str, Any]:
    raw = _load_policy_yaml()
    policies = raw.get("policies", {}) if isinstance(raw.get("policies"), dict) else {}
    defaults = policies.get("freshness_defaults", {}) if isinstance(policies.get("freshness_defaults"), dict) else {}
    enabled = bool(defaults.get("enabled", True))
    domains = defaults.get("domains", {}) if isinstance(defaults.get("domains"), dict) else {}
    out_domains: Dict[str, Dict[str, Any]] = {}
    for domain, cfg in domains.items():
        if not isinstance(cfg, dict):
            continue
        mode = str(cfg.get("artifact_reuse_mode", "reuse_if_recent")).strip().lower()
        if mode not in {"reuse", "reuse_if_recent", "always_regenerate"}:
            mode = "reuse_if_recent"
        try:
            hours = max(1, min(720, int(cfg.get("artifact_reuse_max_age_hours", 72))))
        except Exception:
            hours = 72
        out_domains[str(domain)] = {"artifact_reuse_mode": mode, "artifact_reuse_max_age_hours": hours}
    return {"enabled": enabled, "domains": out_domains}


def _set_policy_freshness_domain(domain: str, mode: str, max_age_hours: int) -> Dict[str, Any]:
    d = str(domain or "").strip()
    if not d:
        return {"ok": False, "error": "domain is required"}
    m = str(mode or "").strip().lower()
    if m not in {"reuse", "reuse_if_recent", "always_regenerate"}:
        return {"ok": False, "error": "invalid mode"}
    try:
        hours = max(1, min(720, int(max_age_hours)))
    except Exception:
        return {"ok": False, "error": "invalid max_age_hours"}
    raw = _load_policy_yaml()
    policies = raw.get("policies")
    if not isinstance(policies, dict):
        policies = {}
        raw["policies"] = policies
    freshness = policies.get("freshness_defaults")
    if not isinstance(freshness, dict):
        freshness = {"enabled": True, "domains": {}}
        policies["freshness_defaults"] = freshness
    freshness["enabled"] = bool(freshness.get("enabled", True))
    domains = freshness.get("domains")
    if not isinstance(domains, dict):
        domains = {}
        freshness["domains"] = domains
    domains[d] = {"artifact_reuse_mode": m, "artifact_reuse_max_age_hours": hours}
    _save_policy_yaml(raw)
    return {"ok": True, "domain": d, "mode": m, "max_age_hours": hours}


def _infer_instruction_domain(instruction: str) -> str:
    low = str(instruction or "").lower()
    if any(x in low for x in ["gmail", "inbox", "email", "draft replies"]):
        return "email_triage"
    if any(x in low for x in ["payer", "insurance", "health plan", "transparency in coverage"]):
        return "payer_pricing_review"
    if any(x in low for x in ["vscode", "vs code", "visual studio code"]):
        return "code_workbench"
    code_pipeline_hits = sum(
        1
        for x in [
            "research",
            "collect",
            "source data",
            "ingest",
            "analy",
            "build",
            "write",
            "test",
            "fix",
            "package",
        ]
        if x in low
    )
    code_deliverable_hits = sum(
        1
        for x in [
            "vscode",
            "vs code",
            "visual studio code",
            "write code",
            "analysis script",
            "workspace scaffold",
            "rag",
            "vector store",
            "retriever",
            "web app",
            "analysis app",
            "unit test",
            "smoke test",
        ]
        if x in low
    )
    if code_deliverable_hits >= 1 and code_pipeline_hits >= 3:
        return "code_workbench"
    if any(x in low for x in ["job", "linkedin", "indeed", "salary"]):
        return "job_market"
    if any(x in low for x in ["competitor", "executive summary", "epic systems"]):
        return "competitor_analysis"
    if any(x in low for x in ["study", "flashcard", "quiz", "permit exam"]):
        return "study_pack"
    if any(x in low for x in ["document", "ppt", "powerpoint", "visual", "dashboard"]):
        return "artifact_generation"
    if any(x in low for x in ["open ", "click ", "type ", "notepad", "installed app"]):
        return "desktop_sequence"
    return "web_research"


def _resolve_domain_freshness_defaults(
    instruction: str,
    requested_mode: str,
    requested_hours: int,
    use_domain_defaults: bool,
) -> tuple[str, int, str, str]:
    mode = str(requested_mode or "reuse_if_recent").strip().lower()
    if mode not in {"reuse", "reuse_if_recent", "always_regenerate"}:
        mode = "reuse_if_recent"
    try:
        hours = max(1, min(720, int(requested_hours)))
    except Exception:
        hours = 72
    domain = _infer_instruction_domain(instruction)
    if not use_domain_defaults:
        return mode, hours, domain, "manual_override"
    defaults = _load_policy_freshness_defaults()
    if not bool(defaults.get("enabled", True)):
        return mode, hours, domain, "policy_disabled"
    domains = defaults.get("domains", {}) if isinstance(defaults.get("domains"), dict) else {}
    cfg = domains.get(domain, {}) if isinstance(domains.get(domain), dict) else {}
    if not cfg:
        return mode, hours, domain, "domain_policy_missing"
    pmode = str(cfg.get("artifact_reuse_mode", mode)).strip().lower()
    if pmode not in {"reuse", "reuse_if_recent", "always_regenerate"}:
        pmode = mode
    try:
        phours = max(1, min(720, int(cfg.get("artifact_reuse_max_age_hours", hours))))
    except Exception:
        phours = hours
    return pmode, phours, domain, "domain_default"


def _preflight_status_locked(state: UiState) -> Dict[str, Any]:
    required = bool(state.preflight_required)
    if not required:
        return {"required": False, "green": True, "reason": "Preflight gate disabled."}
    reliability = dict(state.reliability_suite_result or {})
    core20 = dict(state.human_suite_result or {})
    killer5 = dict(state.killer_suite_result or {})
    if not reliability:
        return {"required": True, "green": False, "reason": "Run Reliability Suite first."}
    if not bool(reliability.get("ok", False)):
        return {"required": True, "green": False, "reason": "Latest reliability suite is not green."}
    if not core20:
        return {"required": True, "green": False, "reason": "Run Human 20-Test Suite first."}
    if not bool(core20.get("ok", False)) or str(core20.get("suite", "")) != "core20":
        return {"required": True, "green": False, "reason": "Latest Human 20-Test Suite is not green."}
    if not killer5:
        return {"required": True, "green": False, "reason": "Run Killer 5 Suite first."}
    if not bool(killer5.get("ok", False)) or str(killer5.get("suite", "")) != "killer5":
        return {"required": True, "green": False, "reason": "Latest Killer 5 Suite is not green."}
    finished_at = float(reliability.get("finished_at", 0.0) or 0.0)
    return {"required": True, "green": True, "reason": "Preflight green.", "finished_at": finished_at}


def _preflight_gate_error_locked(state: UiState) -> str:
    status = _preflight_status_locked(state)
    if status.get("green", False):
        return ""
    return str(status.get("reason", "Run Reliability Suite first."))


def _preflight_block_response(reason: str) -> Dict[str, Any]:
    return {
        "ok": False,
        "mode": "preflight_gate",
        "error": reason,
        "error_code": "preflight_required",
        "message": reason,
        "canvas": {
            "title": "Run Blocked By Preflight Gate",
            "subtitle": reason,
            "cards": [],
        },
    }


def _run_notepad_smoke_once() -> Dict[str, Any]:
    ts = time.time()
    ok, launched = open_installed_app("notepad")
    trace: List[Dict[str, Any]] = [{"step": 0, "action": "open_app", "ok": ok, "launched": launched}]
    if not ok:
        return {
            "ok": False,
            "mode": "notepad_smoke",
            "error": "notepad_not_found",
            "trace": trace,
            "canvas": {"title": "Notepad Smoke Failed", "subtitle": "Could not open Notepad.", "cards": []},
        }
    time.sleep(0.7)
    adapter = UIAAdapter(allow_input_fallback=True, dry_run=False)
    text = "hello world"
    try:
        adapter.type({}, text)
        trace.append({"step": 1, "action": "type_text", "ok": True, "text": text})
    except Exception as exc:  # pylint: disable=broad-exception-caught
        trace.append({"step": 1, "action": "type_text", "ok": False, "error": str(exc)})
        return {
            "ok": False,
            "mode": "notepad_smoke",
            "error": str(exc),
            "trace": trace,
            "canvas": {"title": "Notepad Smoke Failed", "subtitle": "Unable to type into Notepad.", "cards": []},
        }
    out_dir = Path("data/reports/smoke_tests")
    out_dir.mkdir(parents=True, exist_ok=True)
    log_path = out_dir / f"notepad_hello_world_{datetime.fromtimestamp(ts).strftime('%Y%m%d_%H%M%S')}.json"
    log_path.write_text(json.dumps({"ts": ts, "trace": trace, "text": text}, indent=2), encoding="utf-8")
    return {
        "ok": True,
        "mode": "notepad_smoke",
        "message": "Opened Notepad and typed hello world.",
        "trace": trace,
        "artifacts": {"smoke_log": str(log_path.resolve())},
        "canvas": {"title": "Notepad Smoke Passed", "subtitle": "Notepad opened and text typed.", "cards": []},
    }


def _start_instruction_task(
    state: UiState,
    instruction: str,
    confirm_risky: bool,
    ai_backend: str,
    min_live_non_curated_citations: int = 3,
    manual_auth_phase: bool = True,
    browser_worker_mode: str = "local",
    human_like_interaction: bool = True,
    use_domain_freshness_defaults: bool = True,
    artifact_reuse_mode: str = "reuse_if_recent",
    artifact_reuse_max_age_hours: int = 72,
    auth_session_id: str = "",
) -> str:
    task_id = uuid.uuid4().hex
    now = time.time()
    with state.lock:
        state.tasks[task_id] = {
            "id": task_id,
            "status": "running",
            "progress": 0,
            "message": "Queued",
            "events": [{"ts": now, "progress": 0, "message": "Queued"}],
            "result": {},
            "error": "",
            "started_ts": now,
            "finished_ts": 0.0,
        }
        state.current_task_id = task_id
        state.tasks = _trim_tasks(state.tasks)

    def _progress(pct: int, msg: str) -> None:
        with state.lock:
            task = state.tasks.get(task_id)
            if not task:
                return
            task["progress"] = max(0, min(100, int(pct)))
            task["message"] = msg
            events = task.get("events", [])
            events.append({"ts": time.time(), "progress": task["progress"], "message": msg})
            task["events"] = events[-120:]

    def _runner() -> None:
        with state.lock:
            granted = state.control_granted
            paused = state.paused_for_credentials
            step_mode = state.step_mode
            backend = normalize_backend(str(ai_backend or state.ai_backend))
            min_live = max(1, min(20, int(min_live_non_curated_citations or state.min_live_non_curated_citations)))
            worker_mode = normalize_browser_worker_mode(str(browser_worker_mode or state.browser_worker_mode))
            human_like = bool(human_like_interaction if human_like_interaction is not None else state.human_like_interaction)
            reuse_mode = str(artifact_reuse_mode or state.artifact_reuse_mode).strip().lower()
            if reuse_mode not in {"reuse", "reuse_if_recent", "always_regenerate"}:
                reuse_mode = state.artifact_reuse_mode
            reuse_hours = max(1, min(720, int(artifact_reuse_max_age_hours or state.artifact_reuse_max_age_hours)))
            use_domain_defaults = bool(use_domain_freshness_defaults if use_domain_freshness_defaults is not None else state.use_domain_freshness_defaults)
            reuse_mode, reuse_hours, _domain, _source = _resolve_domain_freshness_defaults(
                instruction=instruction,
                requested_mode=reuse_mode,
                requested_hours=reuse_hours,
                use_domain_defaults=use_domain_defaults,
            )
        if paused:
            with state.lock:
                task = state.tasks.get(task_id, {})
                task.update(
                    {
                        "status": "error",
                        "progress": 100,
                        "message": "Session paused for credential entry. Click Resume.",
                        "error": "Session paused for credential entry. Click Resume.",
                        "finished_ts": time.time(),
                    }
                )
            return
        try:
            result = execute_instruction(
                instruction=instruction,
                control_granted=granted,
                step_mode=step_mode,
                confirm_risky=confirm_risky,
                ai_backend=backend,
                min_live_non_curated_citations=min_live,
                manual_auth_phase=manual_auth_phase,
                auth_session_id=auth_session_id,
                browser_worker_mode=worker_mode,
                human_like_interaction=human_like,
                artifact_reuse_mode=reuse_mode,
                artifact_reuse_max_age_hours=reuse_hours,
                progress_cb=_progress,
            )
            with state.lock:
                task = state.tasks.get(task_id, {})
                task.update(
                    {
                        "status": "done",
                        "progress": 100,
                        "message": "Paused for auth" if result.get("paused_for_credentials") else "Completed",
                        "result": result,
                        "error": "",
                        "finished_ts": time.time(),
                    }
                )
                if result.get("ok"):
                    state.history.append(result)
                    state.history = state.history[-300:]
                    _save_history(state.history)
                state.pending_plan = result.get("pending_plan") or {}
                state.paused_for_credentials = bool(result.get("paused_for_credentials", False))
                state.pause_reason = str(result.get("pause_reason", "")) if state.paused_for_credentials else ""
                if state.paused_for_credentials:
                    state.pending_auth_instruction = instruction
                    pending_url = str(result.get("opened_url", "") or "")
                    if _looks_like_gmail_auth_instruction(instruction):
                        pending_url = _sanitize_gmail_auth_url(pending_url)
                    state.pending_auth_url = pending_url
                    state.pending_auth_session_id = str(result.get("auth_session_id", "") or "")
                else:
                    state.pending_auth_instruction = ""
                    state.pending_auth_url = ""
                    state.pending_auth_session_id = ""
                _apply_auth_loop_tracking_locked(state, result)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            with state.lock:
                task = state.tasks.get(task_id, {})
                task.update(
                    {
                        "status": "error",
                        "progress": 100,
                        "message": "Failed",
                        "error": str(exc),
                        "finished_ts": time.time(),
                    }
                )

    threading.Thread(target=_runner, daemon=True).start()
    return task_id


def _start_reliability_suite_task(
    state: UiState,
    include_pytest: bool,
    include_desktop_smoke: bool,
    pytest_args: List[str],
    pytest_timeout_seconds: int,
) -> str:
    task_id = uuid.uuid4().hex
    now = time.time()
    with state.lock:
        state.tasks[task_id] = {
            "id": task_id,
            "status": "running",
            "progress": 0,
            "message": "Running reliability suite",
            "events": [{"ts": now, "progress": 0, "message": "Starting reliability suite"}],
            "result": {},
            "error": "",
            "started_ts": now,
            "finished_ts": 0.0,
        }
        state.current_task_id = task_id
        state.reliability_suite_task_id = task_id
        state.reliability_suite_result = {}
        state.tasks = _trim_tasks(state.tasks)

    def _runner() -> None:
        try:
            with state.lock:
                task = state.tasks.get(task_id, {})
                events = task.get("events", [])
                events.append({"ts": time.time(), "progress": 25, "message": "Running scenario checks"})
                task["events"] = events[-120:]
                task["progress"] = 25
                task["message"] = "Running scenario checks"
            result = run_reliability_suite(
                include_pytest=include_pytest,
                pytest_args=pytest_args,
                pytest_timeout_seconds=pytest_timeout_seconds,
                include_desktop_smoke=include_desktop_smoke,
                desktop_smoke_runner=_run_notepad_smoke_once if include_desktop_smoke else None,
            )
            with state.lock:
                task = state.tasks.get(task_id, {})
                task.update(
                    {
                        "status": "done",
                        "progress": 100,
                        "message": "Reliability suite completed",
                        "result": result,
                        "error": "",
                        "finished_ts": time.time(),
                    }
                )
                events = task.get("events", [])
                events.append({"ts": time.time(), "progress": 100, "message": "Reliability suite completed"})
                task["events"] = events[-120:]
                state.reliability_suite_result = result
                state.history.append(result)
                state.history = state.history[-300:]
                _save_history(state.history)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            with state.lock:
                task = state.tasks.get(task_id, {})
                task.update(
                    {
                        "status": "error",
                        "progress": 100,
                        "message": "Reliability suite failed",
                        "error": str(exc),
                        "finished_ts": time.time(),
                    }
                )

    threading.Thread(target=_runner, daemon=True).start()
    return task_id


def _start_human_operator_20_suite_task(state: UiState) -> str:
    task_id = uuid.uuid4().hex
    now = time.time()
    with state.lock:
        state.tasks[task_id] = {
            "id": task_id,
            "status": "running",
            "progress": 0,
            "message": "Running human operator 20-test suite",
            "events": [{"ts": now, "progress": 0, "message": "Starting human operator 20-test suite"}],
            "result": {},
            "error": "",
            "started_ts": now,
            "finished_ts": 0.0,
        }
        state.current_task_id = task_id
        state.human_suite_task_id = task_id
        state.human_suite_result = {}
        state.tasks = _trim_tasks(state.tasks)

    def _runner() -> None:
        try:
            with state.lock:
                task = state.tasks.get(task_id, {})
                events = task.get("events", [])
                events.append({"ts": time.time(), "progress": 20, "message": "Executing scenarios S01..S20"})
                task["events"] = events[-120:]
                task["progress"] = 20
                task["message"] = "Executing scenarios S01..S20"
            result = run_human_operator_20_suite(
                scenarios_path="config/human_operator_scenarios.json",
                artifacts_root="test_artifacts/human_operator_core20_suite",
                stop_on_fail=True,
            )
            with state.lock:
                task = state.tasks.get(task_id, {})
                task.update(
                    {
                        "status": "done",
                        "progress": 100,
                        "message": "Human operator suite completed",
                        "result": result,
                        "error": "",
                        "finished_ts": time.time(),
                    }
                )
                events = task.get("events", [])
                events.append({"ts": time.time(), "progress": 100, "message": "Human operator suite completed"})
                task["events"] = events[-120:]
                state.human_suite_result = result
                state.history.append(result)
                state.history = state.history[-300:]
                _save_history(state.history)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            with state.lock:
                task = state.tasks.get(task_id, {})
                task.update(
                    {
                        "status": "error",
                        "progress": 100,
                        "message": "Human operator suite failed",
                        "error": str(exc),
                        "finished_ts": time.time(),
                    }
                )

    threading.Thread(target=_runner, daemon=True).start()
    return task_id


def _start_human_operator_killer_suite_task(state: UiState) -> str:
    task_id = uuid.uuid4().hex
    now = time.time()
    with state.lock:
        state.tasks[task_id] = {
            "id": task_id,
            "status": "running",
            "progress": 0,
            "message": "Running human operator killer 5 suite",
            "events": [{"ts": now, "progress": 0, "message": "Starting human operator killer 5 suite"}],
            "result": {},
            "error": "",
            "started_ts": now,
            "finished_ts": 0.0,
        }
        state.current_task_id = task_id
        state.killer_suite_task_id = task_id
        state.killer_suite_result = {}
        state.tasks = _trim_tasks(state.tasks)

    def _runner() -> None:
        try:
            with state.lock:
                task = state.tasks.get(task_id, {})
                events = task.get("events", [])
                events.append({"ts": time.time(), "progress": 20, "message": "Executing scenarios K1..K5"})
                task["events"] = events[-120:]
                task["progress"] = 20
                task["message"] = "Executing scenarios K1..K5"
            result = run_human_operator_killer_suite(
                scenarios_path="config/human_operator_scenarios.json",
                artifacts_root="test_artifacts/human_operator_killer_suite",
                stop_on_fail=True,
            )
            with state.lock:
                task = state.tasks.get(task_id, {})
                task.update(
                    {
                        "status": "done",
                        "progress": 100,
                        "message": "Human operator killer suite completed",
                        "result": result,
                        "error": "",
                        "finished_ts": time.time(),
                    }
                )
                events = task.get("events", [])
                events.append({"ts": time.time(), "progress": 100, "message": "Human operator killer suite completed"})
                task["events"] = events[-120:]
                state.killer_suite_result = result
                state.history.append(result)
                state.history = state.history[-300:]
                _save_history(state.history)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            with state.lock:
                task = state.tasks.get(task_id, {})
                task.update(
                    {
                        "status": "error",
                        "progress": 100,
                        "message": "Human operator killer suite failed",
                        "error": str(exc),
                        "finished_ts": time.time(),
                    }
                )

    threading.Thread(target=_runner, daemon=True).start()
    return task_id


def _start_notepad_smoke_task(state: UiState) -> str:
    task_id = uuid.uuid4().hex
    now = time.time()
    with state.lock:
        state.tasks[task_id] = {
            "id": task_id,
            "status": "running",
            "progress": 0,
            "message": "Running Notepad smoke test",
            "events": [{"ts": now, "progress": 0, "message": "Starting Notepad smoke test"}],
            "result": {},
            "error": "",
            "started_ts": now,
            "finished_ts": 0.0,
        }
        state.current_task_id = task_id
        state.tasks = _trim_tasks(state.tasks)

    def _runner() -> None:
        try:
            with state.lock:
                task = state.tasks.get(task_id, {})
                events = task.get("events", [])
                events.append({"ts": time.time(), "progress": 25, "message": "Opening Notepad and typing text"})
                task["events"] = events[-120:]
                task["progress"] = 25
            result = _run_notepad_smoke_once()
            with state.lock:
                task = state.tasks.get(task_id, {})
                task.update(
                    {
                        "status": "done",
                        "progress": 100,
                        "message": "Notepad smoke completed",
                        "result": result,
                        "error": "",
                        "finished_ts": time.time(),
                    }
                )
                events = task.get("events", [])
                events.append({"ts": time.time(), "progress": 100, "message": "Notepad smoke completed"})
                task["events"] = events[-120:]
                state.history.append(result)
                state.history = state.history[-300:]
                _save_history(state.history)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            with state.lock:
                task = state.tasks.get(task_id, {})
                task.update(
                    {
                        "status": "error",
                        "progress": 100,
                        "message": "Notepad smoke failed",
                        "error": str(exc),
                        "finished_ts": time.time(),
                    }
                )

    threading.Thread(target=_runner, daemon=True).start()
    return task_id


def _trim_tasks(tasks: Dict[str, Dict[str, Any]], keep: int = 30) -> Dict[str, Dict[str, Any]]:
    if len(tasks) <= keep:
        return tasks
    ordered = sorted(tasks.items(), key=lambda x: float(x[1].get("started_ts", 0.0)))
    trimmed = dict(ordered[-keep:])
    return trimmed
