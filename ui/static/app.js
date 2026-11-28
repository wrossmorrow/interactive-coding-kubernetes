// session-ui — decoupled multi-session SPA talking to session-hub (/api/sessions/*)
// and live pi-sessiond instances proxied through /s/<id>/api/*.
"use strict";

const $ = (sel) => document.querySelector(sel);
const el = (tag, cls, text) => {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text != null) e.textContent = text;
  return e;
};
const store = {
  get(k, d) { try { const v = localStorage.getItem("piworkbench." + k); return v == null ? d : JSON.parse(v); } catch { return d; } },
  set(k, v) { try { localStorage.setItem("piworkbench." + k, JSON.stringify(v)); } catch {} },
};
const fmtNum = (n) => n == null ? "?" : n < 1000 ? String(n) : n < 1e6 ? (n / 1000).toFixed(n < 1e4 ? 1 : 0) + "k" : (n / 1e6).toFixed(1) + "M";
const fmtDur = (ms) => ms < 1000 ? ms + "ms" : ms < 60000 ? (ms / 1000).toFixed(1) + "s" : Math.floor(ms / 60000) + "m" + String(Math.round(ms / 1000) % 60).padStart(2, "0") + "s";
const oneLine = (s, n = 200) => { s = String(s ?? "").replace(/\s+/g, " ").trim(); return s.length > n ? s.slice(0, n - 1) + "…" : s; };
const textOf = (content) => typeof content === "string" ? content
  : Array.isArray(content) ? content.filter((c) => c && c.type === "text").map((c) => c.text).join("") : "";
const resultText = (r) => !r ? "" : typeof r === "string" ? r : Array.isArray(r) ? textOf(r) : textOf(r.content);
const lineCount = (s) => !s ? 0 : s.replace(/\n$/, "").split("\n").length;
const home = (p) => p ? p.replace(/^\/home\/[^/]+/, "~") : p;

// ---------------------------------------------------------------- hub & session state

const H = {
  sessions: new Map(), // id -> Metadata
  currentId: null,
  currentMeta: null,
  models: [],
  defaultModel: "",
  es: null,            // EventSource for active session
  hubEs: null,         // EventSource for /api/hub/events
};

const S = {
  state: null,
  stats: null,
  info: {},
  levels: ["off"],
  commands: [],
  streaming: false,
  runStart: 0,
  activity: "",
  retry: null,
  queue: { steering: [], followUp: [] },
  ext: { status: new Map(), widgets: new Map(), title: "" },
  dialogs: [],
  history: [],
  histIdx: -1,
  images: [],
  bell: store.get("bell", false),
  bashRunning: false,
  abortRequested: false,
};

const T = {
  root: $("#transcript"),
  entries: 0,
  cur: null,
  tools: new Map(),
  dirty: new Set(),
};
let bashSeq = 0;
let resyncing = null;

function sessionAPI(path) {
  return `/s/${encodeURIComponent(H.currentId)}${path}`;
}

function isLiveSession(meta) {
  return !!(meta && meta.running && (meta.status === "active" || meta.status === "needs-attention"));
}

async function cmd(type, params = {}) {
  if (!H.currentId || !isLiveSession(H.currentMeta)) {
    return { success: false, error: "session is not active" };
  }
  try {
    const r = await fetch(sessionAPI("/api/cmd"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ type, ...params }),
    });
    const j = await r.json();
    if (!j.success) notice("error", `${type}: ${j.error || r.statusText}`);
    return j;
  } catch (e) {
    notice("error", `${type}: ${e.message}`);
    return { success: false, error: e.message };
  }
}

// ---------------------------------------------------------------- markdown

if (window.marked) {
  marked.use({ gfm: true, breaks: false });
}
if (window.DOMPurify) {
  DOMPurify.addHook("afterSanitizeAttributes", (n) => {
    if (n.tagName === "A") { n.setAttribute("target", "_blank"); n.setAttribute("rel", "noopener noreferrer"); }
  });
}

function renderMarkdown(target, src) {
  if (!window.marked || !window.DOMPurify) {
    target.textContent = src || "";
    return;
  }
  let html;
  try { html = marked.parse(src || ""); } catch { html = ""; }
  target.innerHTML = DOMPurify.sanitize(html, { FORBID_TAGS: ["style", "img", "form", "input"] });
  for (const pre of target.querySelectorAll("pre")) {
    const code = pre.querySelector("code");
    const lang = (code?.className.match(/language-(\S+)/) || [])[1] || "";
    const wrap = el("div", "code");
    const head = el("div", "code-head");
    head.append(el("span", "", lang || " "), copyButton(() => code ? code.textContent : pre.textContent));
    pre.replaceWith(wrap);
    wrap.append(head, pre);
  }
}

function copyButton(get, label = "copy") {
  const b = el("button", "mini-btn", label);
  b.type = "button";
  b.addEventListener("click", async (e) => {
    e.stopPropagation();
    try { await navigator.clipboard.writeText(get()); b.textContent = "copied"; }
    catch { b.textContent = "failed"; }
    setTimeout(() => (b.textContent = label), 1200);
  });
  return b;
}

// ---------------------------------------------------------------- scrolling

const scroller = $("#scroll");
let stick = true;
scroller.addEventListener("scroll", () => {
  stick = scroller.scrollHeight - scroller.scrollTop - scroller.clientHeight < 60;
  if (stick) $("#jump").hidden = true;
});
function afterContent() {
  if (stick) scroller.scrollTop = scroller.scrollHeight;
  else $("#jump").hidden = false;
}
$("#jump").addEventListener("click", () => { stick = true; scroller.scrollTop = scroller.scrollHeight; $("#jump").hidden = true; });

let frame = 0, lastBig = 0;
function markDirty(block) { T.dirty.add(block); if (!frame) frame = requestAnimationFrame(flush); }
function flush() {
  frame = 0;
  const now = performance.now();
  for (const b of [...T.dirty]) {
    if (b.kind === "text" && b.text.length > 12000 && now - lastBig < 80) continue;
    if (b.kind === "text" && b.text.length > 12000) lastBig = now;
    T.dirty.delete(b);
    b.paint();
  }
  if (T.dirty.size && !frame) frame = requestAnimationFrame(flush);
  afterContent();
}

// ---------------------------------------------------------------- transcript

function append(node) {
  const e = T.root.querySelector(".empty");
  if (e) e.remove();
  T.root.append(node);
  T.entries++;
  afterContent();
  return node;
}

function clearTranscript() {
  T.root.textContent = "";
  T.entries = 0;
  T.cur = null;
  T.tools.clear();
  T.dirty.clear();
}

function userEntry(msg) {
  const e = el("div", "entry user");
  const text = textOf(msg.content);
  e.textContent = text;
  const imgs = Array.isArray(msg.content) ? msg.content.filter((c) => c.type === "image").length : 0;
  if (imgs) e.append(el("div", "imgs", `[${imgs} image${imgs > 1 ? "s" : ""}]`));
  if (text && !text.startsWith("/")) { S.history.push(text); S.histIdx = -1; }
  return append(e);
}

function notice(level, text, cls = "") {
  return append(el("div", `entry notice ${level} ${cls}`, text));
}

function assistantEntry() {
  const e = el("div", "entry assistant");
  const actions = el("div", "entry-actions");
  const view = { el: e, blocks: new Map(), msg: null };
  actions.append(copyButton(() => [...view.blocks.values()].filter((b) => b.kind === "text").map((b) => b.text).join("\n\n")));
  e.append(actions);
  append(e);
  return view;
}

function textBlock(parent) {
  const b = { kind: "text", text: "", el: el("div", "md") };
  b.paint = () => renderMarkdown(b.el, b.text);
  parent.append(b.el);
  return b;
}

function thinkingBlock(parent) {
  const b = { kind: "thinking", text: "", live: false, el: el("div", "thinking") };
  const head = el("div", "thinking-head");
  const body = el("div", "thinking-body");
  b.el.append(head, body);
  head.addEventListener("click", () => b.el.classList.toggle("open"));
  b.paint = () => {
    const n = b.text.length;
    head.textContent = `∴ ${b.live ? "thinking…" : "thought"}  ${n ? fmtNum(n) + " chars" : ""}`;
    body.textContent = b.text;
  };
  parent.append(b.el);
  b.paint();
  return b;
}

function argSummary(name, a) {
  a = a || {};
  const s = (v) => typeof v === "string" ? v : JSON.stringify(v);
  switch (name) {
    case "bash": return "$ " + String(a.command ?? "").split("\n")[0];
    case "read": return home(a.path ?? a.file_path ?? "");
    case "write": return home(a.path ?? a.file_path ?? "");
    case "edit": return home(a.path ?? a.file_path ?? "");
    default: return Object.entries(a).map(([k, v]) => `${k}=${oneLine(s(v), 60)}`).join("  ");
  }
}

function toolView(id, name, args, opts = {}) {
  const t = {
    kind: "tool", id, name, args: args || {}, status: "pending", output: "", details: null,
    started: 0, ended: 0, el: el("div", "tool" + (opts.bash ? " bashx" : "")),
  };
  const head = el("div", "tool-head");
  const glyph = el("span", "tool-glyph", "·");
  const tname = el("span", "tool-name", name);
  const sum = el("span", "tool-sum");
  const meta = el("span", "tool-meta");
  head.append(glyph, tname, sum, meta);
  const tail = el("div", "tool-tail");
  const body = el("div", "tool-body");
  t.el.append(head, tail, body);
  head.addEventListener("click", () => t.el.classList.toggle("open"));
  t.paint = () => {
    t.el.classList.toggle("ok", t.status === "done");
    t.el.classList.toggle("err", t.status === "error");
    t.el.classList.toggle("run", t.status === "running");
    glyph.textContent = t.status === "done" ? "✓" : t.status === "error" ? "✕" : t.status === "running" ? "●" : "·";
    sum.textContent = argSummary(t.name, t.args);
    const dur = t.started && t.ended ? fmtDur(t.ended - t.started) : "";
    const n = lineCount(t.output);
    meta.textContent = [n ? n + "L" : "", dur].filter(Boolean).join(" · ");
    const lines = t.output ? t.output.replace(/\n$/, "").split("\n") : [];
    tail.textContent = lines.length ? lines.slice(-2).join("\n") : "";
    tail.hidden = !tail.textContent;
    body.textContent = "";
    if (t.output) body.append(el("pre", "", t.output));
  };
  t.paint();
  T.tools.set(id, t);
  return t;
}

function finalizeAssistant(v, m) {
  v.msg = m;
  const keep = new Map(v.blocks);
  v.blocks.clear();
  (m.content || []).forEach((c, i) => {
    let b;
    if (c.type === "text") {
      b = keep.get(i)?.kind === "text" ? keep.get(i) : textBlock(v.el);
      v.el.append(b.el);
      if (b.text !== c.text || T.dirty.has(b)) { b.text = c.text; b.paint(); }
    } else if (c.type === "thinking") {
      b = keep.get(i)?.kind === "thinking" ? keep.get(i) : thinkingBlock(v.el);
      v.el.append(b.el);
      b.text = c.thinking || ""; b.live = false; b.paint();
      if (!b.text.trim()) b.el.remove();
    } else if (c.type === "toolCall") {
      b = T.tools.get(c.id) || toolView(c.id, c.name, c.arguments);
      b.args = c.arguments || b.args;
      v.el.append(b.el);
      b.paint();
    }
    if (b) { v.blocks.set(i, b); T.dirty.delete(b); }
  });
  if (m.stopReason === "error") v.el.append(el("div", "notice error", "error: " + (m.errorMessage || "model request failed")));
  if (v.el.childElementCount <= 1) v.el.remove();
  afterContent();
}

function finishTool(t, content, isError, details) {
  t.output = resultText(content);
  t.details = details || t.details;
  t.status = isError ? "error" : "done";
  t.ended = Date.now();
  t.paint();
  afterContent();
}

function historyMessage(m) {
  switch (m.role) {
    case "user": userEntry(m); break;
    case "assistant": finalizeAssistant(assistantEntry(), m); break;
    case "toolResult": {
      const t = T.tools.get(m.toolCallId);
      if (t) finishTool(t, m.content, m.isError, m.details);
      break;
    }
    case "bashExecution": {
      const t = toolView("bash-" + m.timestamp, "!", { command: m.command }, { bash: true });
      append(el("div", "entry")).append(t.el);
      finishTool(t, m.output + (m.truncated ? "\n… (truncated)" : ""), m.exitCode !== 0 || m.cancelled);
      break;
    }
    case "compactionSummary":
    case "branchSummary": {
      const n = notice("info summary", `${m.role === "compactionSummary" ? "compacted" : "branch"} summary — click to expand`);
      n.style.cursor = "pointer";
      n.addEventListener("click", () => { n.textContent = m.summary || "(empty)"; n.style.cursor = ""; }, { once: true });
      break;
    }
    default: {
      const text = textOf(m.content) || m.summary || "";
      if (text) notice("", `[${m.role}] ${text}`);
    }
  }
}

function msgKey(m) {
  return m.role + ":" + (m.timestamp || "") + ":" + (m.toolCallId || textOf(m.content).slice(0, 40));
}

function handleEvent(ev) {
  if (resyncing) { resyncing.buffer.push(ev); return; }
  apply(ev);
}

function apply(ev, known) {
  switch (ev.type) {
    case "agent_start": setStreaming(true); break;
    case "agent_end":
    case "agent_settled":
      setStreaming(false);
      refreshStats();
      break;
    case "message_start": {
      const m = ev.message;
      if (!m || (known && known.has(msgKey(m)))) break;
      if (m.role === "assistant") T.cur = assistantEntry();
      break;
    }
    case "message_update": {
      if (!T.cur) T.cur = assistantEntry();
      const ame = ev.assistantMessageEvent || {};
      const i = ame.contentIndex;
      if (ame.type === "text_start") T.cur.blocks.set(i, textBlock(T.cur.el));
      else if (ame.type === "text_delta") {
        let b = T.cur.blocks.get(i);
        if (!b) { b = textBlock(T.cur.el); T.cur.blocks.set(i, b); }
        b.text += ame.delta || "";
        markDirty(b);
      } else if (ame.type === "thinking_start") {
        const b = thinkingBlock(T.cur.el); b.live = true; T.cur.blocks.set(i, b);
      } else if (ame.type === "thinking_delta") {
        let b = T.cur.blocks.get(i);
        if (!b) { b = thinkingBlock(T.cur.el); b.live = true; T.cur.blocks.set(i, b); }
        b.text += ame.delta || "";
        markDirty(b);
      } else if (ame.type === "toolcall_start" && ame.block) {
        const t = toolView(ame.block.id, ame.block.name, ame.block.arguments);
        T.cur.el.append(t.el);
        T.cur.blocks.set(i, t);
      }
      break;
    }
    case "message_end": {
      const m = ev.message;
      if (!m || (known && known.has(msgKey(m)))) break;
      if (m.role === "assistant") {
        const v = T.cur || assistantEntry();
        T.cur = null;
        finalizeAssistant(v, m);
      } else if (m.role === "user") {
        userEntry(m);
      } else if (m.role === "toolResult") {
        const t = T.tools.get(m.toolCallId);
        if (t) finishTool(t, m.content, m.isError, m.details);
      }
      break;
    }
    case "tool_execution_start": {
      const t = T.tools.get(ev.toolCallId) || toolView(ev.toolCallId, ev.toolName, ev.args);
      if (!t.el.parentElement) append(el("div", "entry")).append(t.el);
      t.status = "running"; t.started = Date.now(); t.paint();
      break;
    }
    case "tool_execution_update": {
      const t = T.tools.get(ev.toolCallId);
      if (t) { t.output = resultText(ev.partialResult); t.paint(); afterContent(); }
      break;
    }
    case "tool_execution_end": {
      const t = T.tools.get(ev.toolCallId);
      if (t) finishTool(t, ev.result, ev.isError, ev.result?.details);
      break;
    }
    case "queue_update":
      S.queue = { steering: ev.steering || [], followUp: ev.followUp || [] };
      paintQueue();
      break;
    case "extension_ui_request":
      extensionUI(ev);
      break;
    case "web_dialog_closed":
      closeDialog(ev.id);
      break;
    case "session_paused":
    case "session_closed":
      disconnectSessionES();
      loadSessions().then(() => { if (H.currentId) selectSession(H.currentId, true); });
      break;
  }
}

// ---------------------------------------------------------------- session switching & archive mode

function disconnectSessionES() {
  if (H.es) { H.es.close(); H.es = null; }
  $("#conn").hidden = true;
}

async function selectSession(id, force = false) {
  if (!id) return;
  if (!force && H.currentId === id && H.es) return;
  disconnectSessionES();
  H.currentId = id;
  location.hash = "s/" + id;
  S.state = null; S.stats = null; S.info = {}; S.dialogs = []; S.ext.status.clear();
  setStreaming(false);
  clearTranscript();
  paintSidebar();

  const r = await fetch(`/api/sessions/${encodeURIComponent(id)}`);
  if (!r.ok) {
    notice("error", "Session not found");
    return;
  }
  const meta = await r.json();
  H.currentMeta = meta;
  H.sessions.set(id, meta);
  paintSidebar();
  paintTopbar(meta);

  if (meta.running && (meta.status === "active" || meta.status === "needs-attention")) {
    if (meta.status === "needs-attention") showArchiveBanner(meta);
    else $("#archive-banner").hidden = true;
    $("#editor").hidden = false;
    connectActiveSession(id);
  } else {
    $("#editor").hidden = true;
    showArchiveBanner(meta);
    await loadArchivedTranscript(id);
  }
  paintFooter();
}

function paintTopbar(meta) {
  $("#tb-title").textContent = meta.session_name || meta.session_id.slice(0, 8);
  const badge = $("#tb-badge");
  badge.hidden = false;
  badge.className = "badge " + (meta.status || "paused");
  badge.textContent = meta.status || "paused";
  $("#tb-meta").textContent = [meta.repo, meta.branch, meta.toolchain].filter(Boolean).join(" · ");

  const acts = $("#tb-actions");
  acts.textContent = "";
  const isInteractive = !meta.session_kind || meta.session_kind === "interactive";
  if (isInteractive && meta.status !== "closed") {
    const prBtn = el("button", "btn", meta.pr_number ? `PR #${meta.pr_number}` : "Link PR");
    prBtn.title = "Link a GitHub PR number for review wake-up polling";
    prBtn.onclick = async () => {
      const val = window.prompt("GitHub PR number (0 to unlink):", meta.pr_number ? String(meta.pr_number) : "");
      if (val === null) return;
      const trimmed = val.trim();
      if (!/^\d+$/.test(trimmed)) {
        notice("error", "PR number must be a non-negative integer (0 to unlink)");
        return;
      }
      await updateSessionSettings(meta.session_id, { pr_number: parseInt(trimmed, 10) });
    };
    const polSel = el("select", "btn");
    polSel.title = "PR review wake-up policy";
    for (const opt of ["evaluate-and-stage", "auto-push", "off"]) {
      const o = el("option", "", opt);
      o.value = opt;
      if ((meta.wake_policy || "evaluate-and-stage") === opt) o.selected = true;
      polSel.append(o);
    }
    polSel.onchange = () => updateSessionSettings(meta.session_id, { wake_policy: polSel.value });
    acts.append(prBtn, polSel);
  }
  if (meta.running && (meta.status === "active" || meta.status === "needs-attention")) {
    const pauseBtn = el("button", "btn", "Pause");
    pauseBtn.onclick = () => pauseCurrentSession();
    const closeBtn = el("button", "btn danger", "Close");
    closeBtn.onclick = () => closeCurrentSession();
    acts.append(pauseBtn, closeBtn);
  } else if (meta.status === "paused" || meta.status === "needs-attention") {
    const rejoinBtn = el("button", "btn primary", "Rejoin");
    rejoinBtn.onclick = () => rejoinSession(meta.session_id);
    const closeBtn = el("button", "btn danger", "Close");
    closeBtn.onclick = () => closeSessionById(meta.session_id);
    acts.append(rejoinBtn, closeBtn);
  } else if (meta.status === "closed") {
    const forkBtn = el("button", "btn primary", "Fork");
    forkBtn.onclick = () => forkSession(meta.session_id);
    acts.append(forkBtn);
  }
}

async function updateSessionSettings(id, patch) {
  const r = await fetch(`/api/sessions/${encodeURIComponent(id)}/settings`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(patch),
  });
  if (!r.ok) { notice("error", "failed to update session settings"); return; }
  const updated = await r.json();
  H.sessions.set(id, updated);
  if (H.currentId === id) { H.currentMeta = updated; paintTopbar(updated); }
  paintSidebar();
}

function showArchiveBanner(meta) {
  const banner = $("#archive-banner");
  banner.hidden = false;
  if (meta.status === "closed") {
    $("#archive-msg").textContent = "Read-only archived session (0 running daemons). Fork to continue in a new session.";
  } else if (meta.status === "needs-attention") {
    $("#archive-msg").textContent = meta.wake_summary || "PR review wake-up completed — review staged changes before pushing.";
  } else {
    $("#archive-msg").textContent = `Session is ${meta.status} (0 running daemons). Rejoin to restore workspace & continue.`;
  }
  const acts = $("#archive-actions");
  acts.textContent = "";
  if (meta.last_review_session_id && H.sessions.has(meta.last_review_session_id)) {
    const revBtn = el("button", "btn", "View CI Review");
    revBtn.onclick = () => selectSession(meta.last_review_session_id, true);
    acts.append(revBtn);
  }
  if (meta.status === "closed") {
    const b = el("button", "btn primary", "Fork Session");
    b.onclick = () => forkSession(meta.session_id);
    acts.append(b);
  } else if (!meta.running && (!meta.session_kind || meta.session_kind === "interactive" || meta.session_kind === "analyst")) {
    const b = el("button", "btn primary", "Rejoin Session");
    b.onclick = () => rejoinSession(meta.session_id);
    acts.append(b);
  }
}

async function loadArchivedTranscript(id) {
  const r = await fetch(`/api/sessions/${encodeURIComponent(id)}/transcript`);
  if (!r.ok) { notice("error", "Failed to load transcript"); return; }
  const data = await r.json();
  clearTranscript();
  const msgs = data.messages || [];
  for (const m of msgs) historyMessage(m);
  if (!msgs.length) {
    T.root.append(el("div", "empty", "No messages recorded in this session yet."));
  }
  stick = true;
  afterContent();
}

function connectActiveSession(id) {
  const es = new EventSource(sessionAPI("/api/events"));
  H.es = es;
  es.onopen = () => { $("#conn").hidden = true; resyncActive(); };
  es.onmessage = (m) => { try { handleEvent(JSON.parse(m.data)); } catch (e) { console.error(e); } };
  es.onerror = () => {
    if (H.currentId === id && isLiveSession(H.currentMeta)) $("#conn").hidden = false;
  };
}

async function resyncActive() {
  resyncing = { buffer: [] };
  const [st, msgs, info] = await Promise.all([
    cmd("get_state"),
    cmd("get_messages"),
    fetch(sessionAPI("/api/info")).then((r) => r.json()).catch(() => ({})),
  ]);
  S.info = info || {};
  if (st.success) S.state = st.data;
  clearTranscript();
  S.history = [];
  const messages = msgs.success ? msgs.data.messages || [] : [];
  for (const m of messages) historyMessage(m);
  if (!messages.length) {
    const wrap = el("div", "empty");
    wrap.append(el("div", "", `Session ready · ${S.state?.model?.name || "model"}`), el("div", "hints", "/ commands · ! bash · /pause · /close"));
    T.root.append(wrap);
  }
  const known = new Set(messages.map(msgKey));
  const buffered = resyncing.buffer;
  resyncing = null;
  for (const ev of buffered) apply(ev, known);
  setStreaming(!!S.state?.isStreaming);
  refreshStats();
  loadMeta();
  stick = true;
  afterContent();
  $("#input").focus();
}

async function loadMeta() {
  const [lv, cs] = await Promise.all([cmd("get_available_thinking_levels"), cmd("get_commands")]);
  if (lv.success) S.levels = lv.data?.levels || ["off"];
  if (cs.success) S.commands = cs.data?.commands || [];
  paintFooter();
}

let statsTimer = 0;
function refreshStats() {
  clearTimeout(statsTimer);
  statsTimer = setTimeout(async () => {
    if (!H.currentMeta?.running) return;
    const [st, ss] = await Promise.all([cmd("get_state"), cmd("get_session_stats")]);
    if (st.success) S.state = st.data;
    if (ss.success) S.stats = ss.data;
    paintFooter();
  }, 150);
}

// ---------------------------------------------------------------- lifecycle actions

let busyTimer = 0;
function startBusy(message) {
  stopBusy();
  const banner = $("#busy-banner");
  const msgEl = $("#busy-msg");
  const elapsedEl = $("#busy-elapsed");
  const started = Date.now();
  msgEl.textContent = message;
  elapsedEl.textContent = "0s";
  banner.hidden = false;
  $("#btn-new").disabled = true;
  busyTimer = setInterval(() => {
    elapsedEl.textContent = Math.floor((Date.now() - started) / 1000) + "s";
  }, 500);
}

function stopBusy() {
  clearInterval(busyTimer);
  busyTimer = 0;
  $("#busy-banner").hidden = true;
  $("#btn-new").disabled = false;
}

async function pauseCurrentSession() {
  if (!H.currentId) return;
  const id = H.currentId;
  startBusy("Pausing session and saving workspace…");
  try {
    const r = await fetch(`/api/sessions/${encodeURIComponent(id)}/pause`, {
      method: "POST", headers: { "Content-Type": "application/json" }, body: "{}",
    });
    if (!r.ok) { notice("error", "pause failed"); return; }
    await loadSessions();
    await selectSession(id, true);
  } finally {
    stopBusy();
  }
}

async function closeCurrentSession() {
  if (!H.currentId) return;
  await closeSessionById(H.currentId);
}

async function closeSessionById(id) {
  startBusy("Closing and compacting session archive…");
  try {
    const r = await fetch(`/api/sessions/${encodeURIComponent(id)}/close`, {
      method: "POST", headers: { "Content-Type": "application/json" }, body: "{}",
    });
    if (!r.ok) { notice("error", "close failed"); return; }
    await loadSessions();
    await selectSession(id, true);
  } finally {
    stopBusy();
  }
}

async function rejoinSession(id) {
  startBusy("Provisioning sandbox pod & restoring workspace…");
  try {
    const r = await fetch(`/api/sessions/${encodeURIComponent(id)}/rejoin`, {
      method: "POST", headers: { "Content-Type": "application/json" }, body: "{}",
    });
    if (!r.ok) {
      const j = await r.json().catch(() => ({}));
      notice("error", "rejoin failed: " + (j.error || r.statusText));
      return;
    }
    await loadSessions();
    await selectSession(id, true);
  } finally {
    stopBusy();
  }
}

async function forkSession(id) {
  startBusy("Forking session & provisioning sandbox pod…");
  try {
    const r = await fetch(`/api/sessions/${encodeURIComponent(id)}/fork`, {
      method: "POST", headers: { "Content-Type": "application/json" }, body: "{}",
    });
    if (!r.ok) { notice("error", "fork failed"); return; }
    const created = await r.json();
    await loadSessions();
    await selectSession(created.session_id, true);
  } finally {
    stopBusy();
  }
}

// ---------------------------------------------------------------- inbox sidebar & hub SSE

async function loadSessions() {
  const r = await fetch("/api/sessions");
  if (!r.ok) return;
  const data = await r.json();
  H.sessions.clear();
  for (const s of data.sessions || []) H.sessions.set(s.session_id, s);
  paintSidebar();
}

// client-side sidebar filtering (search + kind + status); pure over H.sessions.
const SF = { q: "", kind: "all", status: "all" };

function sidebarFiltersActive() {
  return SF.q !== "" || SF.kind !== "all" || SF.status !== "all";
}

function sessionMatches(s) {
  if (SF.kind !== "all" && (s.session_kind || "interactive") !== SF.kind) return false;
  if (SF.status === "live") {
    if (s.status !== "active" && s.status !== "needs-attention") return false;
  } else if (SF.status !== "all" && s.status !== SF.status) return false;
  if (SF.q) {
    const hay = [
      s.session_name, s.session_id, s.repo, s.branch,
      s.model_config?.modelId,
      s.pr_number != null ? `#${s.pr_number}` : "",
      s.wake_summary,
    ].filter((v) => v != null && v !== "").join(" ").toLowerCase();
    if (!hay.includes(SF.q)) return false;
  }
  return true;
}

// paintSidebar() only rebuilds #session-list, so the search input keeps focus
// when live SSE session_updated events re-render the list.
$("#sb-search").addEventListener("input", () => {
  SF.q = $("#sb-search").value.trim().toLowerCase();
  paintSidebar();
});
$("#sb-kind").addEventListener("change", () => { SF.kind = $("#sb-kind").value; paintSidebar(); });
$("#sb-status").addEventListener("change", () => { SF.status = $("#sb-status").value; paintSidebar(); });
$("#sb-clear").addEventListener("click", () => {
  SF.q = ""; SF.kind = "all"; SF.status = "all";
  $("#sb-search").value = "";
  $("#sb-kind").value = "all";
  $("#sb-status").value = "all";
  paintSidebar();
});

function paintSidebar() {
  const list = $("#session-list");
  list.textContent = "";
  const all = [...H.sessions.values()].sort((a, b) => (b.updated_at || b.started_at || "").localeCompare(a.updated_at || a.started_at || ""));
  const active = sidebarFiltersActive();
  const items = active ? all.filter(sessionMatches) : all;
  const metaRow = $("#sb-filter-meta");
  metaRow.hidden = !active;
  if (active) $("#sb-count").textContent = `${items.length} / ${all.length}`;
  else $("#sb-count").textContent = "";
  if (!items.length) {
    list.append(el("div", "dim", active ? "No sessions match the current filters." : "No sessions yet. Click + new."));
    return;
  }
  for (const s of items) {
    const btn = el("button", "sb-item" + (s.session_id === H.currentId ? " selected" : ""));
    btn.type = "button";
    const row = el("div", "sb-row");
    row.append(el("span", "sb-name", s.session_name || s.session_id.slice(0, 8)));
    row.append(el("span", "badge " + (s.status || "paused"), s.status || "paused"));
    const kindTag = s.session_kind && s.session_kind !== "interactive" ? `[${s.session_kind}]` : "";
    const sub = el("div", "sb-sub", [kindTag, s.repo || (s.session_kind === "analyst" ? "archive" : "local"), s.branch, s.verdict, s.model_config?.modelId].filter(Boolean).join(" · "));
    btn.append(row, sub);
    btn.onclick = () => selectSession(s.session_id);
    list.append(btn);
  }
}

function connectHubEvents() {
  const es = new EventSource("/api/hub/events");
  H.hubEs = es;
  es.onmessage = (m) => {
    try {
      const ev = JSON.parse(m.data);
      if (ev.type === "session_updated" && ev.session) {
        H.sessions.set(ev.session.session_id, ev.session);
        paintSidebar();
        if (ev.session.session_id === H.currentId) {
          const wasLive = isLiveSession(H.currentMeta);
          const nowLive = isLiveSession(ev.session);
          H.currentMeta = ev.session;
          paintTopbar(ev.session);
          if (wasLive && !nowLive) {
            selectSession(H.currentId, true);
          } else if (nowLive) {
            if (ev.session.status === "needs-attention") showArchiveBanner(ev.session);
            else $("#archive-banner").hidden = true;
          }
        }
      }
    } catch {}
  };
}

// ---------------------------------------------------------------- new session modal

async function loadModels() {
  const r = await fetch("/api/models");
  if (!r.ok) return;
  const data = await r.json();
  H.models = data.models || [];
  H.defaultModel = data.defaultModel || "";
  const sel = $("#nm-model");
  sel.textContent = "";
  for (const m of H.models) {
    const opt = el("option", "", `${m.name || m.id} (${m.provider}/${m.id})`);
    opt.value = `${m.provider}|${m.id}`;
    if (m.id === H.defaultModel) opt.selected = true;
    sel.append(opt);
  }
}

$("#btn-new").onclick = () => { $("#modal-backdrop").hidden = false; $("#nm-name").focus(); };
$("#nm-cancel").onclick = () => { $("#modal-backdrop").hidden = true; };
$("#nm-kind").addEventListener("change", () => {
  const isAnalyst = $("#nm-kind").value === "analyst";
  $("#nm-repo-wrap").hidden = isAnalyst;
  if (isAnalyst) $("#nm-toolchain").value = "python";
});
$("#new-modal").addEventListener("submit", async (e) => {
  e.preventDefault();
  const [provider, model] = ($("#nm-model").value || "|").split("|");
  const kind = $("#nm-kind").value || "interactive";
  const payload = {
    kind,
    name: $("#nm-name").value.trim(),
    repo: kind === "analyst" ? "" : $("#nm-repo").value.trim(),
    toolchain: $("#nm-toolchain").value,
    size: $("#nm-size").value,
    provider,
    model,
  };
  $("#modal-backdrop").hidden = true;
  const label = payload.name ? ` (${payload.name})` : "";
  startBusy(`Provisioning sandbox pod${label}…`);
  try {
    const r = await fetch("/api/sessions", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!r.ok) {
      const j = await r.json().catch(() => ({}));
      notice("error", "create session failed: " + (j.error || r.statusText));
      return;
    }
    const created = await r.json();
    $("#nm-name").value = "";
    await loadSessions();
    await selectSession(created.session_id, true);
  } finally {
    stopBusy();
  }
});

// ---------------------------------------------------------------- editor, dialogs & footer

function setStreaming(v) {
  if (v && !S.streaming) S.runStart = Date.now();
  S.streaming = v;
  $("#spinner").hidden = !v;
  $("#spinner").textContent = v ? "● working…" : "";
  $("#editor").classList.toggle("busy", v);
  paintFooter();
}

function paintQueue() {
  const q = $("#queue");
  q.textContent = "";
  for (const s of S.queue.steering) q.append(el("div", "q", "↳ steer  " + oneLine(s)));
  for (const s of S.queue.followUp) q.append(el("div", "q", "↳ next   " + oneLine(s)));
}

function extensionUI(ev) {
  switch (ev.method) {
    case "select": case "confirm": case "input": case "editor":
      if (!S.dialogs.some((d) => d.id === ev.id)) S.dialogs.push({ ...ev, opened: Date.now() });
      if (S.dialogs.length === 1) showDialog();
      break;
    case "notify":
      notice(ev.notifyType === "error" ? "error" : ev.notifyType === "warning" ? "warn" : "info", ev.message || "");
      break;
    case "setStatus":
      if (ev.statusText == null) S.ext.status.delete(ev.statusKey); else S.ext.status.set(ev.statusKey, ev.statusText);
      paintFooter();
      break;
  }
}

function showDialog() {
  const d = S.dialogs[0];
  const box = $("#dialog");
  if (!d) { box.hidden = true; box.textContent = ""; $("#editor").hidden = !(H.currentMeta?.status === "active" && H.currentMeta?.running); return; }
  $("#editor").hidden = true;
  box.hidden = false;
  box.textContent = "";
  box.append(el("div", "d-title", d.title || "The agent needs input"));
  if (d.message) box.append(el("div", "d-msg", d.message));
  const row = el("div", "d-row");
  const answer = (r) => answerDialog(d.id, r);
  if (d.method === "confirm") {
    const yes = el("button", "btn primary", "Yes"); yes.type = "button"; yes.onclick = () => answer({ confirmed: true });
    const no = el("button", "btn", "No"); no.type = "button"; no.onclick = () => answer({ confirmed: false });
    row.append(yes, no);
  } else if (d.method === "select") {
    for (const opt of d.options || []) {
      const b = el("button", "btn", opt); b.type = "button"; b.onclick = () => answer({ value: opt });
      row.append(b);
    }
  } else {
    const f = el("input"); f.type = "text"; f.placeholder = d.placeholder || "";
    const ok = el("button", "btn primary", "Submit"); ok.type = "button"; ok.onclick = () => answer({ value: f.value });
    box.append(f);
    row.append(ok);
  }
  const cancel = el("button", "btn", "Cancel"); cancel.type = "button"; cancel.onclick = () => answer({ cancelled: true });
  row.append(cancel);
  box.append(row);
}

async function answerDialog(id, response) {
  closeDialog(id);
  await fetch(sessionAPI("/api/extension-ui/response"), {
    method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ id, ...response }),
  }).catch(() => {});
}

function closeDialog(id) {
  const i = S.dialogs.findIndex((d) => d.id === id);
  if (i >= 0) { S.dialogs.splice(i, 1); if (i === 0) showDialog(); }
}

const input = $("#input");
function autoGrow() {
  input.style.height = "auto";
  input.style.height = Math.min(input.scrollHeight + 2, window.innerHeight * 0.4) + "px";
}

const BUILTINS = [
  { name: "pause", description: "pause session to zero (preserves workspace + offset)", run: () => pauseCurrentSession() },
  { name: "close", description: "close & compact session archive", run: () => closeCurrentSession() },
  { name: "compact", description: "compact context", run: (arg) => cmd("compact", arg ? { customInstructions: arg } : {}) },
  { name: "model", description: "choose a model", run: () => openModelMenu($("#f-model")) },
  { name: "name", description: "rename session", run: async (arg) => { if (arg) { await cmd("set_session_name", { name: arg }); refreshStats(); } } },
  { name: "reload", description: "re-render transcript", run: () => resyncActive() },
];

async function submit(mode) {
  const raw = input.value;
  const text = raw.trim();
  if (!text) return;
  const sm = /^\/(\S+)\s*([\s\S]*)$/.exec(text);
  if (sm) {
    const b = BUILTINS.find((x) => x.name === sm[1]);
    if (b) { input.value = ""; autoGrow(); await b.run(sm[2].trim()); return; }
  }
  if (text.startsWith("!") && !S.streaming) {
    const command = text.replace(/^!+/, "").trim();
    if (!command) return;
    input.value = ""; autoGrow();
    const t = toolView("bash-" + ++bashSeq, "!", { command }, { bash: true });
    append(el("div", "entry")).append(t.el);
    t.status = "running"; t.started = Date.now(); t.paint();
    const r = await cmd("bash", { command });
    const d = r.data || {};
    finishTool(t, d.output ?? r.error ?? "", !r.success || d.exitCode !== 0);
    return;
  }
  const params = { message: text };
  let type = "prompt";
  if (S.streaming) type = mode === "followUp" ? "follow_up" : "steer";
  input.value = ""; autoGrow();
  stick = true;
  const r = await cmd(type, params);
  if (!r.success) { input.value = raw; autoGrow(); }
}

input.addEventListener("input", autoGrow);
input.addEventListener("keydown", (e) => {
  if (e.key === "Enter" && !e.shiftKey && !e.isComposing) {
    e.preventDefault();
    submit(e.altKey ? "followUp" : "steer");
  }
});

function openMenu(anchor, items, onPick) {
  const m = $("#menu");
  m.textContent = "";
  for (const it of items) {
    const o = el("div", "opt" + (it.current ? " cur" : ""));
    o.append(el("span", "k", it.label), el("span", "s", it.hint || ""));
    o.onclick = () => { m.hidden = true; onPick(it); };
    m.append(o);
  }
  m.hidden = false;
  const r = anchor.getBoundingClientRect();
  m.style.left = Math.max(8, Math.min(r.left, window.innerWidth - m.offsetWidth - 8)) + "px";
  m.style.top = Math.max(8, r.top - m.offsetHeight - 6) + "px";
}
document.addEventListener("mousedown", (e) => { if (!$("#menu").hidden && !e.target.closest("#menu")) $("#menu").hidden = true; });

async function openModelMenu(anchor) {
  const r = await cmd("get_available_models");
  const models = r.data?.models || [];
  const cur = S.state?.model;
  openMenu(anchor, models.map((m) => ({
    label: m.name || m.id,
    hint: `${m.provider}/${m.id}`,
    current: cur && m.id === cur.id && m.provider === cur.provider,
    m,
  })), async (it) => { await cmd("set_model", { provider: it.m.provider, modelId: it.m.id }); refreshStats(); loadMeta(); });
}

function paintFooter() {
  const st = S.state || {}, ss = S.stats || {};
  $("#f-cwd").textContent = [home(S.info.cwd || ""), S.info.branch ? `(${S.info.branch})` : ""].filter(Boolean).join(" ");
  const tk = ss.tokens || {};
  $("#f-tokens").textContent = tk.input != null ? `↑${fmtNum(tk.input)} ↓${fmtNum(tk.output)}` : "";
  const m = st.model;
  $("#f-model").textContent = m ? (m.name || m.id) : "";
  const lvl = st.thinkingLevel || "off";
  $("#f-thinking").textContent = m?.reasoning === false ? "" : (m ? "◇ " + lvl : "");
  $("#f-status").textContent = [...S.ext.status.values()].join("  ");
  $("#f-view-thinking").textContent = document.body.classList.contains("show-thinking") ? "thinking: shown" : "thinking: hidden";
  $("#f-view-tools").textContent = document.body.classList.contains("expand-tools") ? "tools: expanded" : "tools: compact";
  $("#f-bell").textContent = S.bell ? "🔔" : "🔕";
}

function toggleView(cls) {
  document.body.classList.toggle(cls);
  store.set(cls, document.body.classList.contains(cls));
  paintFooter();
}
for (const cls of ["show-thinking", "expand-tools"]) if (store.get(cls, false)) document.body.classList.add(cls);

$("#f-model").onclick = (e) => openModelMenu(e.currentTarget);
$("#f-view-thinking").onclick = () => toggleView("show-thinking");
$("#f-view-tools").onclick = () => toggleView("expand-tools");
$("#f-bell").onclick = () => { S.bell = !S.bell; store.set("bell", S.bell); paintFooter(); };
$("#f-flush").onclick = async () => {
  if (!H.currentMeta?.running) return;
  const r = await fetch(sessionAPI("/api/flush"), { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });
  const j = await r.json().catch(() => ({}));
  if (r.ok && j.success) notice("info", `flushed: ${j.partCount} parts, offset ${j.finalOffset}`);
  else notice("error", "flush failed");
};

document.addEventListener("keydown", (e) => {
  if (e.ctrlKey && !e.metaKey && !e.altKey) {
    if (e.key === "t") { e.preventDefault(); toggleView("show-thinking"); }
    if (e.key === "o") { e.preventDefault(); toggleView("expand-tools"); }
  }
});

// ---------------------------------------------------------------- boot

(async function boot() {
  paintFooter();
  await Promise.all([loadModels(), loadSessions()]);
  connectHubEvents();
  const hashId = location.hash.startsWith("#s/") ? location.hash.slice(3) : "";
  if (hashId && H.sessions.has(hashId)) {
    await selectSession(hashId, true);
  } else if (H.sessions.size > 0) {
    const first = [...H.sessions.values()][0];
    await selectSession(first.session_id, true);
  } else {
    T.root.append(el("div", "empty", "Create a new session from the sidebar to begin."));
  }
})();
