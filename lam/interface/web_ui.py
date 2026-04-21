from __future__ import annotations

import json
import threading
import time
import uuid
import webbrowser
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlparse

from lam.adapters.uia_adapter import UIAAdapter
from lam.interface.ai_backend import AI_BACKENDS, normalize_backend
from lam.interface.app_launcher import list_installed_apps
from lam.interface.global_teach_hooks import GlobalTeachHooks
from lam.interface.scheduler import ScheduleEngine, ScheduleJob
from lam.interface.search_agent import execute_instruction, preview_instruction, resume_pending_plan
from lam.interface.selector_picker import capture_selector_at_cursor
from lam.interface.teach_recorder import TeachRecorder
from lam.interface.user_defaults import current_user, load_defaults, save_defaults
from lam.interface.password_vault import LocalPasswordVault


@dataclass(slots=True)
class UiState:
    control_granted: bool = False
    control_granted_at: float = 0.0
    paused_for_credentials: bool = False
    pause_reason: str = ""
    pending_plan: Dict[str, Any] = field(default_factory=dict)
    step_mode: bool = False
    ai_backend: str = "deterministic-local"
    compression_mode: str = "normal"
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
    lock: threading.Lock = field(default_factory=threading.Lock)

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            schedules = self.scheduler.list_jobs() if self.scheduler else []
            schedule_history = self.scheduler.list_history(limit=50) if self.scheduler else []
            return {
                "control_granted": self.control_granted,
                "control_granted_at": self.control_granted_at,
                "paused_for_credentials": self.paused_for_credentials,
                "pause_reason": self.pause_reason,
                "has_pending_plan": bool(self.pending_plan),
                "step_mode": self.step_mode,
                "ai_backend": self.ai_backend,
                "compression_mode": self.compression_mode,
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
                "task": dict(self.tasks.get(self.current_task_id, {})) if self.current_task_id else {},
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
    .brand { font-size:18px; font-weight:700; margin-bottom:10px; }
    .status { background:#111827; border:1px solid #1f2937; padding:10px; border-radius:10px; margin-bottom:12px; }
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
    .artifact-list a { color:#0f766e; text-decoration:none; }
    .json-box { white-space:pre-wrap; max-height:260px; overflow:auto; font-size:12px; color:#0f172a; background:#f8fafc; border:1px solid #e2e8f0; border-radius:10px; padding:10px; }
    .small { color:var(--muted); font-size:12px; }
    .grid2 { display:grid; grid-template-columns:1fr 1fr; gap:12px; }
    .mono { font-family:Consolas,Menlo,monospace; font-size:12px; }
    .progress-wrap { background:#e5e7eb; border-radius:999px; height:14px; overflow:hidden; }
    .progress-bar { height:100%; width:0%; background:linear-gradient(90deg,#0f766e,#6366f1); transition:width .2s ease; }
    .progress-log { max-height:140px; overflow:auto; font-size:12px; background:#f8fafc; border:1px solid #e2e8f0; border-radius:10px; padding:8px; }
    @media (max-width: 1180px) { .wrap { grid-template-columns:1fr; } .side { max-height:35vh; } .grid2{grid-template-columns:1fr;} .summary-grid{grid-template-columns:1fr;} }
  </style>
</head>
<body>
<div class="wrap">
  <aside class="side">
    <div class="brand">LAM Console</div>
    <div class="status" id="statusBox">Control: not granted</div>
    <div class="small">History</div>
    <div id="history"></div>
  </aside>
  <main class="main">
    <div class="panel">
      <div class="row">
        <button class="primary" onclick="grantControl()">Accept Control</button>
        <button class="warn" onclick="revokeControl()">Revoke Control</button>
        <button onclick="resumeAfterLogin()">Resume</button>
        <label class="small"><input type="checkbox" id="stepMode" onchange="setStepMode(this.checked)"/> Step mode</label>
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
      </div>
      <div class="row">
        <input id="instruction" class="wide" type="text" placeholder="open chatgpt app then click New chat then type &quot;hello&quot; then press enter"/>
        <button class="primary" onclick="runInstruction()">Run</button>
        <button onclick="previewInstruction()">Preview</button>
      </div>
      <div class="row">
        <input id="automationName" type="text" placeholder="Automation name"/>
        <button onclick="saveAutomation()">Save</button>
        <button onclick="runAutomation()">Run Saved</button>
        <button onclick="exportHistory()">Export History</button>
      </div>
      <div class="row">
        <button onclick="useTemplate('open chatgpt app then click New chat then type \\'Daily summary\\' then press enter')">Template: ChatGPT Daily</button>
        <button onclick="useTemplate('search Amazon for best price on Abu Garcia Voltiq baitcasting reel')">Template: Amazon Price</button>
      </div>
      <div class="row">
        <input id="appSearch" type="text" placeholder="Search installed apps"/>
        <button onclick="searchApps()">Find Apps</button>
      </div>
      <div class="row"><strong>Progress</strong> <span class="small" id="progressLabel">Idle</span></div>
      <div class="progress-wrap"><div id="progressBar" class="progress-bar"></div></div>
      <div class="progress-log mono" id="progressLog">No active task.</div>
    </div>

    <div class="grid2">
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

    <div class="panel">
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

    <div class="panel">
      <div class="row">
        <div><strong>Run Summary</strong></div>
        <label class="small"><input type="checkbox" id="showDetails" onchange="toggleDetails(this.checked)"/> Show technical details</label>
      </div>
      <div class="summary-head" id="summaryHead">Waiting for your instruction</div>
      <div class="summary-sub" id="summarySub">Use Preview or Run to start.</div>
      <div class="summary-grid" id="summaryCards"></div>
      <div class="progress-log" id="activityLog">No activity yet.</div>
      <details id="detailWrap" style="margin-top:8px;">
        <summary class="small">Raw JSON (advanced)</summary>
        <div class="json-box" id="outputRaw">No details yet.</div>
      </details>
    </div>
  </main>
</div>
<script>
const ui = { history: JSON.parse(localStorage.getItem("lam_ui_history") || "[]") };
let progressPollTimer = null;
let detailsVisible = false;
let lastRaw = {};
function persistHistory(){ localStorage.setItem("lam_ui_history", JSON.stringify(ui.history.slice(-300))); }
function toggleDetails(v){
  detailsVisible=!!v;
  localStorage.setItem("lam_details_visible", detailsVisible ? "1":"0");
  document.getElementById("detailWrap").open = detailsVisible;
  if(detailsVisible){ document.getElementById("outputRaw").innerText=JSON.stringify(lastRaw||{},null,2); }
}
function setRaw(obj){ lastRaw=obj||{}; if(detailsVisible){ document.getElementById("outputRaw").innerText=JSON.stringify(lastRaw,null,2);} }
function renderSummary(r){
  const ok = !!r?.ok;
  const mode = r?.mode || "status";
  const count = r?.results_count || (Array.isArray(r?.results)?r.results.length:0);
  const head = ok ? (r?.canvas?.title || "Task completed") : "Action needs attention";
  const sub = ok
    ? (r?.canvas?.subtitle || `${mode}${count?` • ${count} result(s)`:""}`)
    : (r?.error || "The action could not be completed.");
  document.getElementById("summaryHead").innerText = head;
  document.getElementById("summarySub").innerText = sub;

  const cards=[];
  if(r?.plan?.domain){ cards.push({t:"Planner",m:`${r.plan.domain} (${r.plan.steps?.length||0} steps)`}); }
  if(r?.query){ cards.push({t:"Query",m:r.query}); }
  if(count){ cards.push({t:"Results",m:String(count)}); }
  if(r?.opened_url){ cards.push({t:"Opened",m:r.opened_url}); }
  if(r?.artifacts){
    const lines = Object.entries(r.artifacts).slice(0,3).map(([k,v])=>`${k}: ${v}`);
    cards.push({t:"Artifacts",m:lines.join(" | ")});
  }
  (r?.canvas?.cards||[]).slice(0,4).forEach(c=>cards.push({t:c.title||"Item",m:`${c.price||""} ${c.source?`• ${c.source}`:""}`.trim()}));
  if(cards.length===0){ cards.push({t:"Status",m:ok?"Completed":"Needs input"}); }
  document.getElementById("summaryCards").innerHTML = cards.slice(0,6).map(c=>`<div class="summary-card"><div class="t">${escapeHtml(c.t||"")}</div><div class="m">${escapeHtml(c.m||"")}</div></div>`).join("");

  const activity=[];
  if(Array.isArray(r?.decision_log)){ r.decision_log.forEach(x=>activity.push(`• ${x}`)); }
  if(r?.source_status){ Object.entries(r.source_status).slice(0,10).forEach(([k,v])=>activity.push(`• ${k}: ${v}`)); }
  if(r?.pause_reason){ activity.push(`• ${r.pause_reason}`); }
  if(activity.length===0){ activity.push(ok?"• Finished successfully.":"• Check details for error context."); }
  document.getElementById("activityLog").innerText = activity.join("\\n");
}
function escapeHtml(s){ return String(s||"").replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])); }
async function refreshState(){
  const s=await fetch("/api/state").then(r=>r.json());
  let t=s.control_granted?"Control: granted":"Control: not granted";
  if(s.paused_for_credentials) t+=" | Paused";
  if(s.has_pending_plan) t+=" | Pending sequence";
  if(s.pause_reason) t+=" | "+s.pause_reason;
  document.getElementById("statusBox").innerText=t;
  document.getElementById("stepMode").checked=!!s.step_mode;
  document.getElementById("aiBackend").value=s.ai_backend||"deterministic-local";
  document.getElementById("compressionMode").value=s.compression_mode||"normal";
  const teach=s.teach||{};
  document.getElementById("teachState").innerText=`${teach.active?'Recording':'Idle'} • events: ${teach.event_count||0}${s.global_teach_active?' • global hooks active':''}`;
  const jobs=(s.schedules||[]).length, recent=(s.schedule_history||[]).length;
  document.getElementById("scheduleState").innerText=`${jobs} schedule(s) configured • ${recent} recent run(s)`;
  const vs=s.vault_status||{};
  document.getElementById("vaultState").innerText=`Vault: ${vs.entries||0} entries • ${vs.dpapi_available?'DPAPI secured':'local encryption fallback'}`;
  renderTask(s.task||{});
}
function renderHistory(){
  const el=document.getElementById("history"); el.innerHTML="";
  [...ui.history].reverse().forEach((item,idx)=>{
    const d=document.createElement("div"); d.className="history-item";
    const canRerun = !!(item && item.instruction);
    d.innerHTML=`<div style="font-weight:600">${item.instruction||item.mode||"Run"}</div><div class="small">${item.app_name||""} ${item.opened_url||""}</div>${canRerun?`<div class="row" style="margin-top:6px"><button onclick="rerunHistory(${idx}); event.stopPropagation();">Re-run</button></div>`:''}`;
    d.onclick=()=>{ renderSummary(item); setRaw(item); };
    el.appendChild(d);
  });
}
async function rerunHistory(revIndex){
  const items=[...ui.history].reverse();
  const item=items[revIndex];
  if(!item || !item.instruction){ return; }
  const ai_backend=document.getElementById("aiBackend").value;
  const r=await fetch("/api/instruct_async",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({instruction:item.instruction,ai_backend})}).then(r=>r.json());
  if(!r.ok){ showResponse(r); await refreshState(); return; }
  startTaskPolling(r.task_id, { instruction:item.instruction, ai_backend, confirm_risky:false });
}
async function grantControl(){
  const r=await fetch("/api/control",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({accept:true})}).then(r=>r.json());
  showResponse({ok:true,mode:"control",canvas:{title:"Control Granted",subtitle:"OpenLAMb can execute actions on this box",cards:[]},control_granted:r.control_granted});
  await refreshState();
}
async function revokeControl(){
  const r=await fetch("/api/control",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({accept:false})}).then(r=>r.json());
  showResponse({ok:true,mode:"control",canvas:{title:"Control Revoked",subtitle:"Execution is now blocked until re-enabled",cards:[]},control_granted:r.control_granted});
  await refreshState();
}
async function setStepMode(v){ await fetch("/api/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({step_mode:!!v})}); await refreshState(); }
async function setAiBackend(v){ await fetch("/api/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({ai_backend:v})}); await refreshState(); }
async function setCompressionMode(v){ await fetch("/api/settings",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({compression_mode:v})}); await refreshState(); }
async function resumeAfterLogin(){ const r=await fetch("/api/session/resume",{method:"POST",headers:{"Content-Type":"application/json"},body:"{}"}).then(r=>r.json()); handleResult(r); await refreshState(); }
async function runInstruction(){
  const instruction=document.getElementById("instruction").value.trim(); if(!instruction) return;
  const ai_backend=document.getElementById("aiBackend").value;
  const r=await fetch("/api/instruct_async",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({instruction,ai_backend})}).then(r=>r.json());
  if(!r.ok){ showResponse(r); await refreshState(); return; }
  startTaskPolling(r.task_id, { instruction, ai_backend, confirm_risky:false });
}
async function previewInstruction(){ const instruction=document.getElementById("instruction").value.trim(); if(!instruction)return; const r=await fetch("/api/preview",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({instruction})}).then(r=>r.json()); showResponse(r); await refreshState(); }
async function saveAutomation(){ const name=document.getElementById("automationName").value.trim(); const instruction=document.getElementById("instruction").value.trim(); const r=await fetch("/api/automation/save",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({name,instruction})}).then(r=>r.json()); showResponse(r); await refreshState(); }
async function runAutomation(){
  const name=document.getElementById("automationName").value.trim();
  const ai_backend=document.getElementById("aiBackend").value;
  const r=await fetch("/api/automation/run_async",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({name,ai_backend})}).then(r=>r.json());
  if(!r.ok){ showResponse(r); await refreshState(); return; }
  startTaskPolling(r.task_id, { instruction:r.instruction||"", ai_backend, confirm_risky:false });
}
async function exportHistory(){ const txt=await fetch("/api/history/export").then(r=>r.text()); const blob=new Blob([txt],{type:"application/json"}); const a=document.createElement("a"); a.href=URL.createObjectURL(blob); a.download="lam-history-export.json"; a.click(); }
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
  document.getElementById("vaultList").innerText = entries.slice(0,8).map(e=>`${e.service} • ${e.username_masked}${e.favorite?' • ★':''}`).join("\\n");
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
}
function stopTaskPolling(){ if(progressPollTimer){ clearInterval(progressPollTimer); progressPollTimer=null; } }
function startTaskPolling(taskId, rerunPayload){
  stopTaskPolling();
  const tick = async ()=>{
    const t = await fetch(`/api/task?id=${encodeURIComponent(taskId)}`).then(r=>r.json());
    renderTask(t.task||{});
    if((t.task||{}).status === "done"){
      stopTaskPolling();
      const result=(t.task||{}).result||{};
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
  detailsVisible = localStorage.getItem("lam_details_visible")==="1";
  document.getElementById("showDetails").checked = detailsVisible;
  toggleDetails(detailsVisible);
  renderHistory();
  await refreshState();
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
        if path == "/api/task":
            task_id = (qs.get("id", [""])[0] or "").strip()
            with self.state.lock:
                task = dict(self.state.tasks.get(task_id, {})) if task_id else {}
            self._send_json(200, {"ok": bool(task), "task": task})
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
            self._send_json(200, self.state.snapshot())
            return

        if self.path == "/api/settings":
            with self.state.lock:
                self.state.step_mode = bool(payload.get("step_mode", self.state.step_mode))
                self.state.ai_backend = normalize_backend(str(payload.get("ai_backend", self.state.ai_backend)))
                mode = str(payload.get("compression_mode", self.state.compression_mode)).strip().lower()
                if mode not in {"aggressive", "normal", "strict"}:
                    mode = self.state.compression_mode
                self.state.compression_mode = mode
                self.state.recorder.set_compression_mode(self.state.compression_mode)
                _save_user_defaults_locked(self.state)
            self._send_json(200, self.state.snapshot())
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
            if not instruction:
                with self.state.lock:
                    instruction = self.state.saved_automations.get(name, "")
            payload = {
                "instruction": instruction,
                "confirm_risky": bool(payload.get("confirm_risky", False)),
                "ai_backend": ai_backend,
            }
            self.path = "/api/instruct"

        if self.path == "/api/automation/run_async":
            name = str(payload.get("name", "")).strip()
            instruction = str(payload.get("instruction", "")).strip()
            ai_backend = normalize_backend(str(payload.get("ai_backend", "")))
            with self.state.lock:
                if not instruction:
                    instruction = self.state.saved_automations.get(name, "")
            if not instruction:
                self._send_json(404, {"ok": False, "error": f"Automation '{name}' not found."})
                return
            task_id = _start_instruction_task(
                state=self.state,
                instruction=instruction,
                confirm_risky=bool(payload.get("confirm_risky", False)),
                ai_backend=ai_backend,
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
                step_mode = self.state.step_mode
                self.state.paused_for_credentials = False
                self.state.pause_reason = ""
            if pending:
                result = resume_pending_plan(pending, step_mode=step_mode)
                with self.state.lock:
                    self.state.pending_plan = result.get("pending_plan") or {}
                    self.state.paused_for_credentials = bool(result.get("paused_for_credentials", False))
                    self.state.pause_reason = str(result.get("pause_reason", "")) if self.state.paused_for_credentials else ""
                    if result.get("ok"):
                        self.state.history.append(result)
                        self.state.history = self.state.history[-300:]
                        _save_history(self.state.history)
                self._send_json(200, result)
                return
            self._send_json(200, {"ok": True, "message": "No pending sequence."})
            return

        if self.path == "/api/instruct_async":
            instruction = str(payload.get("instruction", "")).strip()
            confirm_risky = bool(payload.get("confirm_risky", False))
            ai_backend = normalize_backend(str(payload.get("ai_backend", "")))
            task_id = _start_instruction_task(
                state=self.state,
                instruction=instruction,
                confirm_risky=confirm_risky,
                ai_backend=ai_backend,
            )
            self._send_json(200, {"ok": True, "task_id": task_id})
            return

        if self.path == "/api/instruct":
            instruction = str(payload.get("instruction", "")).strip()
            confirm_risky = bool(payload.get("confirm_risky", False))
            with self.state.lock:
                granted = self.state.control_granted
                paused = self.state.paused_for_credentials
                step_mode = self.state.step_mode
                ai_backend = normalize_backend(str(payload.get("ai_backend", self.state.ai_backend)))
            if paused:
                self._send_json(409, {"ok": False, "error": "Session paused for credential entry. Click Resume."})
                return
            result = execute_instruction(
                instruction=instruction,
                control_granted=granted,
                step_mode=step_mode,
                confirm_risky=confirm_risky,
                ai_backend=ai_backend,
            )
            if result.get("ok"):
                with self.state.lock:
                    self.state.history.append(result)
                    self.state.history = self.state.history[-300:]
                    _save_history(self.state.history)
                    self.state.pending_plan = result.get("pending_plan") or {}
                    self.state.paused_for_credentials = bool(result.get("paused_for_credentials", False))
                    self.state.pause_reason = str(result.get("pause_reason", "")) if self.state.paused_for_credentials else ""
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

    def log_message(self, fmt: str, *args: Any) -> None:
        _ = (fmt, args)


def run_ui_server(host: str = "127.0.0.1", port: int = 8795, open_browser: bool = True) -> None:
    state = UiState(saved_automations=_load_automations(), history=_load_history())
    state.global_hooks = GlobalTeachHooks(state.recorder)
    _apply_user_defaults(state)

    def scheduler_run(job: ScheduleJob) -> Dict[str, Any]:
        with state.lock:
            instruction = state.saved_automations.get(job.automation_name, "")
            granted = state.control_granted
            step_mode = state.step_mode
            ai_backend = state.ai_backend
        if not granted:
            return {"ok": False, "error": "Control not granted; scheduled run skipped."}
        if not instruction:
            return {"ok": False, "error": f"Automation '{job.automation_name}' not found."}
        result = execute_instruction(
            instruction=instruction,
            control_granted=True,
            step_mode=step_mode,
            confirm_risky=True,
            ai_backend=ai_backend,
        )
        if result.get("ok"):
            with state.lock:
                state.history.append({"mode": "scheduled_run", "job": job.to_dict(), "result": result})
                state.history = state.history[-300:]
                _save_history(state.history)
                state.pending_plan = result.get("pending_plan") or state.pending_plan
                if result.get("paused_for_credentials"):
                    state.paused_for_credentials = True
                    state.pause_reason = str(result.get("pause_reason", ""))
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


def _apply_user_defaults(state: UiState) -> None:
    defaults = load_defaults(user=state.user_id)
    step_mode = bool(defaults.get("step_mode", state.step_mode))
    ai_backend = normalize_backend(str(defaults.get("ai_backend", state.ai_backend)))
    compression_mode = str(defaults.get("compression_mode", state.compression_mode)).strip().lower()
    if compression_mode not in {"aggressive", "normal", "strict"}:
        compression_mode = "normal"
    with state.lock:
        state.step_mode = step_mode
        state.ai_backend = ai_backend
        state.compression_mode = compression_mode
        state.recorder.set_compression_mode(compression_mode)


def _save_user_defaults_locked(state: UiState) -> None:
    save_defaults(
        {
            "step_mode": state.step_mode,
            "ai_backend": state.ai_backend,
            "compression_mode": state.compression_mode,
        },
        user=state.user_id,
    )


def _start_instruction_task(state: UiState, instruction: str, confirm_risky: bool, ai_backend: str) -> str:
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
                progress_cb=_progress,
            )
            with state.lock:
                task = state.tasks.get(task_id, {})
                task.update(
                    {
                        "status": "done",
                        "progress": 100,
                        "message": "Completed",
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


def _trim_tasks(tasks: Dict[str, Dict[str, Any]], keep: int = 30) -> Dict[str, Dict[str, Any]]:
    if len(tasks) <= keep:
        return tasks
    ordered = sorted(tasks.items(), key=lambda x: float(x[1].get("started_ts", 0.0)))
    trimmed = dict(ordered[-keep:])
    return trimmed
