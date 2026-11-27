#!/usr/bin/env node
// Model-agnostic stand-in for `pi --mode rpc` used for local development and
// automated E2E tests. It speaks pi's JSONL RPC protocol, writes real JSONL
// message records to `--session <file>`, and reloads existing messages on
// startup so leave -> pause -> browse -> rejoin works end-to-end.

import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

const speed = Number(process.env.FAKE_PI_SPEED ?? "0.25");
const sleep = (ms) => (speed <= 0 ? Promise.resolve() : new Promise((r) => setTimeout(r, ms * speed)));
const out = (o) => process.stdout.write(JSON.stringify(o) + "\n");
const now = () => Date.now();

function argValue(flag, fallback = "") {
  const idx = process.argv.indexOf(flag);
  return idx >= 0 && idx + 1 < process.argv.length ? process.argv[idx + 1] : fallback;
}

const models = [
  {
    id: "model-alpha",
    name: "Alpha 27B",
    api: "openai-completions",
    provider: "gateway",
    baseUrl: "http://agent-router.ai-gateway.svc.cluster.local:8080/v1",
    reasoning: true,
    input: ["text"],
    contextWindow: 131072,
    maxTokens: 32768,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  },
  {
    id: "model-beta",
    name: "Beta Mini",
    api: "openai-completions",
    provider: "gateway",
    baseUrl: "http://agent-router.ai-gateway.svc.cluster.local:8080/v1",
    reasoning: false,
    input: ["text", "image"],
    contextWindow: 131072,
    maxTokens: 16384,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  },
];

const requestedModel = argValue("--model", "model-alpha");
const initialModel = models.find((m) => m.id === requestedModel) || models[0];
const sessionIdArg = process.env.SESSION_ID || ("sess_" + Math.random().toString(36).slice(2, 10));
const sessionFile = argValue("--session", path.join(process.env.TMPDIR || "/tmp", `${sessionIdArg}.jsonl`));

const S = {
  model: initialModel,
  thinkingLevel: initialModel.reasoning ? "medium" : "off",
  isStreaming: false,
  isCompacting: false,
  sessionName: argValue("--name", process.env.SESSION_NAME || ""),
  sessionId: sessionIdArg,
  sessionFile,
  messages: [],
  steering: [],
  followUp: [],
  aborted: false,
  tokens: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
  dialogs: new Map(),
  toolSeq: 0,
};

function ensureSessionDir() {
  fs.mkdirSync(path.dirname(S.sessionFile), { recursive: true });
}

function loadExistingSession() {
  if (!fs.existsSync(S.sessionFile)) return;
  const raw = fs.readFileSync(S.sessionFile, "utf8");
  for (const line of raw.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      const rec = JSON.parse(trimmed);
      if (rec.type === "message" && rec.message) {
        S.messages.push(rec.message);
      } else if (rec.type === "compaction") {
        S.messages.push({ role: "compactionSummary", summary: rec.summary, tokensBefore: rec.tokensBefore, timestamp: rec.timestamp });
      } else if (rec.role) {
        S.messages.push(rec);
      }
    } catch {
      // ignore malformed lines
    }
  }
}

function persistRecord(rec) {
  ensureSessionDir();
  fs.appendFileSync(S.sessionFile, JSON.stringify(rec) + "\n", "utf8");
}

function pushAndPersistMessage(msg) {
  S.messages.push(msg);
  persistRecord({ type: "message", timestamp: msg.timestamp || now(), message: msg });
}

loadExistingSession();

function respond(id, command, data, error) {
  const r = { type: "response", command, success: !error };
  if (id) r.id = id;
  if (error) r.error = error;
  else if (data !== undefined) r.data = data;
  out(r);
}

function state() {
  const s = {
    model: S.model,
    thinkingLevel: S.thinkingLevel,
    isStreaming: S.isStreaming,
    isCompacting: S.isCompacting,
    steeringMode: "one-at-a-time",
    followUpMode: "one-at-a-time",
    sessionFile: S.sessionFile,
    sessionId: S.sessionId,
    autoCompactionEnabled: true,
    messageCount: S.messages.length,
    pendingMessageCount: S.steering.length + S.followUp.length,
  };
  if (S.sessionName) s.sessionName = S.sessionName;
  return s;
}

function stats() {
  const count = (role) => S.messages.filter((m) => m.role === role).length;
  const toolCalls = S.messages
    .filter((m) => m.role === "assistant")
    .flatMap((m) => m.content || [])
    .filter((b) => b.type === "toolCall").length;
  const ctx = Math.min(S.model.contextWindow, S.tokens.input + S.tokens.output);
  return {
    sessionFile: S.sessionFile,
    sessionId: S.sessionId,
    userMessages: count("user"),
    assistantMessages: count("assistant"),
    toolCalls,
    toolResults: count("toolResult"),
    totalMessages: S.messages.length,
    tokens: S.tokens,
    cost: 0,
    contextUsage: { tokens: ctx, contextWindow: S.model.contextWindow, percent: (100 * ctx) / S.model.contextWindow },
  };
}

function dialog(method, fields) {
  const id = "dlg_" + Math.random().toString(36).slice(2, 8);
  out({ type: "extension_ui_request", id, method, ...fields });
  return new Promise((resolve) => {
    S.dialogs.set(id, resolve);
    if (fields.timeout) {
      setTimeout(() => {
        if (S.dialogs.delete(id)) resolve({ cancelled: true });
      }, fields.timeout);
    }
  });
}

async function streamAssistant(blocks, stopReason) {
  const msg = {
    role: "assistant",
    content: [],
    api: S.model.api,
    provider: S.model.provider,
    model: S.model.id,
    usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: { total: 0 } },
    stopReason: "stop",
    timestamp: now(),
  };
  out({ type: "message_start", message: structuredClone(msg) });
  const upd = (e) => out({ type: "message_update", message: structuredClone(msg), assistantMessageEvent: { ...e, partial: structuredClone(msg) } });
  upd({ type: "start" });
  for (const b of blocks) {
    if (S.aborted) break;
    const i = msg.content.length;
    if (b.type === "thinking" || b.type === "text") {
      const key = b.type === "thinking" ? "thinking" : "text";
      msg.content.push({ type: b.type, [key]: "" });
      upd({ type: `${b.type}_start`, contentIndex: i });
      for (const chunk of b[key].match(/[\s\S]{1,12}/g) || []) {
        if (S.aborted) break;
        msg.content[i][key] += chunk;
        upd({ type: `${b.type}_delta`, contentIndex: i, delta: chunk });
        await sleep(b.fast ? 2 : 10);
      }
      upd({ type: `${b.type}_end`, contentIndex: i, content: msg.content[i][key] });
    } else if (b.type === "toolCall") {
      const tc = { type: "toolCall", id: "call_" + ++S.toolSeq, name: b.name, arguments: {} };
      msg.content.push(tc);
      upd({ type: "toolcall_start", contentIndex: i });
      const json = JSON.stringify(b.arguments);
      for (const chunk of json.match(/[\s\S]{1,20}/g) || []) {
        upd({ type: "toolcall_delta", contentIndex: i, delta: chunk });
        await sleep(5);
      }
      tc.arguments = b.arguments;
      upd({ type: "toolcall_end", contentIndex: i, toolCall: structuredClone(tc) });
    }
  }
  msg.stopReason = S.aborted ? "aborted" : stopReason;
  const outTok = (JSON.stringify(msg.content).length / 4) | 0;
  msg.usage = { input: 600 + S.tokens.total, output: outTok, cacheRead: S.tokens.total, cacheWrite: 300, cost: { total: 0 } };
  S.tokens.input += 600;
  S.tokens.output += outTok;
  S.tokens.cacheRead += S.tokens.total;
  S.tokens.cacheWrite += 300;
  S.tokens.total = S.tokens.input + S.tokens.output;
  if (S.aborted) upd({ type: "error", reason: "aborted" });
  else upd({ type: "done", reason: stopReason });
  pushAndPersistMessage(msg);
  out({ type: "message_end", message: msg });
  return msg;
}

async function runTool(call, result, { lines = [], isError = false, details } = {}) {
  out({ type: "tool_execution_start", toolCallId: call.id, toolName: call.name, args: call.arguments });
  let acc = "";
  for (const l of lines) {
    if (S.aborted) break;
    acc += l + "\n";
    out({ type: "tool_execution_update", toolCallId: call.id, toolName: call.name, args: call.arguments, partialResult: { content: [{ type: "text", text: acc }], details: {} } });
    await sleep(40);
  }
  const text = S.aborted ? acc + "\n[aborted]" : result ?? acc;
  const res = { content: [{ type: "text", text }], details: details ?? {} };
  out({ type: "tool_execution_end", toolCallId: call.id, toolName: call.name, result: res, isError: isError || S.aborted });
  const tr = { role: "toolResult", toolCallId: call.id, toolName: call.name, content: res.content, details: res.details, isError: isError || S.aborted, timestamp: now() };
  out({ type: "message_start", message: tr });
  pushAndPersistMessage(tr);
  out({ type: "message_end", message: tr });
}

async function userMessage(text) {
  const m = { role: "user", content: [{ type: "text", text }], timestamp: now() };
  out({ type: "message_start", message: m });
  pushAndPersistMessage(m);
  out({ type: "message_end", message: m });
}

async function deliverSteering() {
  if (!S.steering.length) return false;
  const text = S.steering.shift();
  out({ type: "queue_update", steering: [...S.steering], followUp: [...S.followUp] });
  await userMessage(text);
  return text;
}

async function run(text) {
  S.isStreaming = true;
  S.aborted = false;
  out({ type: "agent_start" });
  out({ type: "turn_start" });
  await userMessage(text);
  const lower = text.toLowerCase();

  try {
    if (lower.includes("confirm")) {
      const r = await dialog("confirm", { title: "Proceed with change?", message: "Approve simulated edit.", timeout: 30000 });
      await streamAssistant([{ type: "text", text: r.confirmed ? "Approved — applied change." : "Cancelled by user." }], "stop");
    } else if (lower.includes("hello") || lower.length < 16) {
      await streamAssistant(
        [
          { type: "thinking", thinking: "Short prompt; replying directly." },
          { type: "text", text: `Ready on \`${S.model.provider}/${S.model.id}\`. You said: ${text}` },
        ],
        "stop",
      );
    } else {
      const m = await streamAssistant(
        [
          { type: "thinking", thinking: "Inspect the workspace and run a quick check." },
          { type: "text", text: "Checking the workspace first." },
          { type: "toolCall", name: "bash", arguments: { command: "git status -sb" } },
        ],
        "toolUse",
      );
      const calls = m.content.filter((b) => b.type === "toolCall");
      if (!S.aborted && calls[0]) {
        await runTool(calls[0], undefined, { lines: ["## main", " M README.md"] });
      }
      out({ type: "turn_end", message: m, toolResults: [] });
      await deliverSteering();

      if (!S.aborted) {
        out({ type: "turn_start" });
        const finalMsg = await streamAssistant(
          [
            { type: "thinking", thinking: "Summarize findings concisely." },
            { type: "text", text: `Completed turn for: **${text}**\n\n- Model: \`${S.model.provider}/${S.model.id}\`\n- Messages in session: ${S.messages.length + 1}` },
          ],
          "stop",
        );
        out({ type: "turn_end", message: finalMsg, toolResults: [] });
      }
    }
  } finally {
    out({ type: "agent_end", messages: [], willRetry: false });
    S.isStreaming = false;
    if (S.followUp.length && !S.aborted) {
      const next = S.followUp.shift();
      out({ type: "queue_update", steering: [...S.steering], followUp: [...S.followUp] });
      run(next);
    } else {
      out({ type: "agent_settled" });
    }
  }
}

const commands = [
  { name: "skill:review", source: "skill", description: "Review the current diff against main", location: "user" },
  { name: "fix-tests", source: "prompt", description: "Find and fix failing tests", location: "project" },
];

async function handle(cmd) {
  const { id, type } = cmd;
  switch (type) {
    case "get_state":
      return respond(id, type, state());
    case "get_messages":
      return respond(id, type, { messages: S.messages });
    case "get_session_stats":
      return respond(id, type, stats());
    case "get_commands":
      return respond(id, type, { commands });
    case "get_available_models":
      return respond(id, type, { models });
    case "get_available_thinking_levels":
      return respond(id, type, { levels: S.model.reasoning ? ["off", "minimal", "low", "medium", "high"] : ["off"] });
    case "set_thinking_level":
      S.thinkingLevel = cmd.level;
      return respond(id, type);
    case "cycle_thinking_level": {
      if (!S.model.reasoning) return respond(id, type, null);
      const lv = ["off", "minimal", "low", "medium", "high"];
      S.thinkingLevel = lv[(lv.indexOf(S.thinkingLevel) + 1) % lv.length];
      return respond(id, type, { level: S.thinkingLevel });
    }
    case "set_model": {
      const m = models.find((x) => x.provider === cmd.provider && x.id === cmd.modelId);
      if (!m) return respond(id, type, undefined, "model not found");
      S.model = m;
      if (!m.reasoning) S.thinkingLevel = "off";
      return respond(id, type, m);
    }
    case "set_session_name":
      S.sessionName = cmd.name;
      respond(id, type);
      return out({ type: "session_info_changed", name: cmd.name });
    case "abort":
      S.aborted = true;
      return respond(id, type);
    case "get_last_assistant_text": {
      const a = [...S.messages].reverse().find((m) => m.role === "assistant");
      return respond(id, type, { text: a ? a.content.filter((b) => b.type === "text").map((b) => b.text).join("") : null });
    }
    case "compact": {
      out({ type: "compaction_start", reason: "manual" });
      S.isCompacting = true;
      await sleep(300);
      const before = S.tokens.total;
      S.isCompacting = false;
      const summary = "Compacted conversation summary.";
      const comp = { role: "compactionSummary", summary, tokensBefore: before, timestamp: now() };
      S.messages = [comp];
      persistRecord({ type: "compaction", summary, tokensBefore: before, timestamp: comp.timestamp });
      S.tokens = { input: 400, output: 0, cacheRead: 0, cacheWrite: 0, total: 400 };
      out({ type: "compaction_end", reason: "manual", result: { summary, tokensBefore: before }, aborted: false, willRetry: false });
      return respond(id, type, { summary, firstKeptEntryId: "e1", tokensBefore: before, estimatedTokensAfter: 400 });
    }
    case "bash": {
      await sleep(150);
      const output = `fake-pi bash: ${cmd.command}\n`;
      const m = { role: "bashExecution", command: cmd.command, output, exitCode: 0, cancelled: false, truncated: false, timestamp: now() };
      pushAndPersistMessage(m);
      return respond(id, type, { output, exitCode: 0, cancelled: false, truncated: false });
    }
    case "prompt":
    case "steer":
    case "follow_up": {
      const text = cmd.message ?? "";
      const mode = type === "steer" ? "steer" : type === "follow_up" ? "followUp" : cmd.streamingBehavior;
      if (S.isStreaming) {
        if (!mode) return respond(id, type, undefined, "Agent is streaming; specify streamingBehavior");
        (mode === "steer" ? S.steering : S.followUp).push(text);
        respond(id, type, { disposition: "queued" });
        return out({ type: "queue_update", steering: [...S.steering], followUp: [...S.followUp] });
      }
      respond(id, type, { disposition: "started" });
      return run(text);
    }
    case "extension_ui_response": {
      const r = S.dialogs.get(cmd.id);
      if (r) {
        S.dialogs.delete(cmd.id);
        r(cmd);
      }
      return;
    }
    default:
      return respond(id, type, undefined, `Unknown command: ${type}`);
  }
}

out({ type: "extension_ui_request", id: "st1", method: "setStatus", statusKey: "gateway", statusText: `gateway: ${S.model.id}` });
const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
rl.on("line", (line) => {
  if (!line.trim()) return;
  let cmd;
  try {
    cmd = JSON.parse(line);
  } catch {
    return out({ type: "response", command: "parse", success: false, error: "bad json" });
  }
  handle(cmd).catch((e) => process.stderr.write(String(e?.stack || e) + "\n"));
});
rl.on("close", () => process.exit(0));
