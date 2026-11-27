/**
 * session-attribution: thin, model-agnostic pi extension that stamps session
 * and GitHub attribution headers onto every provider request routed through
 * Agent Router (Envoy AI Gateway).
 *
 * Headers stamped:
 *   - x-session-id   : session UUID (from SESSION_ID env or session filename)
 *   - x-session-name : human-readable session name (tracks /name renames live)
 *   - x-session-kind : actor kind ("interactive" | "analyst" | ...)
 *   - x-github-repo  : optional owner/repo
 *   - x-github-pr    : optional PR number
 */

import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

export default function (pi: ExtensionAPI) {
  let baseSessionId: string | null = process.env.SESSION_ID || null;

  function resolveSessionId(): string | null {
    if (!baseSessionId) return null;
    const parts = baseSessionId.split("_");
    return parts.length > 1 && parts[1] ? parts[1] : baseSessionId;
  }

  pi.on("session_start", async (_event, ctx) => {
    if (process.env.SESSION_ID) {
      baseSessionId = process.env.SESSION_ID;
      return;
    }
    const sessionFile = ctx.sessionManager.getSessionFile();
    if (sessionFile) {
      const match = sessionFile.match(/([^/\\]+)\.jsonl$/);
      if (match) {
        baseSessionId = match[1];
        return;
      }
    }
    baseSessionId = `ephemeral-${Date.now()}-${Math.random().toString(36).slice(2, 12)}`;
  });

  pi.on("before_provider_headers", (event, ctx) => {
    const sessionId = resolveSessionId() ?? ctx.sessionManager.getSessionId();
    if (sessionId) {
      event.headers["x-session-id"] = sessionId;
    }
    const name = pi.getSessionName() || process.env.SESSION_NAME;
    if (name) {
      event.headers["x-session-name"] = name;
    }
    const kind = process.env.SESSION_KIND || "interactive";
    event.headers["x-session-kind"] = kind;

    if (process.env.GITHUB_REPO) {
      event.headers["x-github-repo"] = process.env.GITHUB_REPO;
    }
    if (process.env.GITHUB_PR) {
      event.headers["x-github-pr"] = process.env.GITHUB_PR;
    }
  });
}
