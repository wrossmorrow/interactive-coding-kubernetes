# k8s-agents

Model-agnostic Kubernetes interactive coding-session workbench for [`pi`](https://pi.dev/) (`pi --mode rpc`).

## Architecture (Milestone 1)

- **`cmd/pi-sessiond`**: Headless per-sandbox daemon supervising `pi --mode rpc`, generating OpenAI-compatible Agent Router `models.json`, scrubbing hub/gateway/cloud secrets from `pi`'s child env, shipping JSONL parts + git `workspace.bundle` archives over HTTP to `session-hub`, and cleanly separating `paused` (resumable, `$0` on Autopilot) from `closed` (`_closed` + `compacted.jsonl.gz`).
- **`cmd/session-hub`**: Control-plane server managing the session index, `DirStore` / `GCSStore` (with server-side GCS `Compose`), scoped-token internal ingest API (`/internal/sessions/{id}/*`), read-only archive transcript browsing (0 running daemons), live `/s/{id}/*` SSE/RPC reverse proxy, and sandbox spawner (`LocalSpawner` in Milestone 1; K8s Pod spawner in Milestone 2).
- **`ui/`**: Decoupled multi-session SPA with session inbox/switcher, read-only archive mode (`Rejoin` / `Fork`), and live streaming transcript.
- **`ext/session-attribution/`**: Thin `pi` extension stamping `x-session-id`, `x-session-name`, `x-session-kind`, `x-github-repo`, and `x-github-pr`.
- **`images/sandbox/Dockerfile`**: Thin sandbox image layering `FROM ghcr.io/wrossmorrow/agentrunner-{agent,python,golang,rust,java17,java21}`.

## Local Development & Verification

```bash
# Run formatting, vet, build, and full unit + E2E test suite (-race -shuffle=on)
just check

# Start local session-hub + LocalSpawner backed by testdata/fake-pi.mjs and .dev/store
just dev
# Open http://127.0.0.1:8787
```
