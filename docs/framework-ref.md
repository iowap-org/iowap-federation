# iowap-node Framework API Reference (for Federation development)

> This reference file lives in the federation repo so that the code developer
> can use the framework APIs without having to look into the
> `~/projects/iowap-node` directory (the permission sandbox blocks external
> directories). The framework is available as an editable install in the
> venv — imports work.

## Availability

```bash
.venv/bin/pip install -e ../iowap-node   # editable install into the venv
.venv/bin/python -c "from nodes.common.relay_client import RelayClient"
```

## nodes.common.relay_client.RelayClient

The shared HTTP client to an AI relay. The federation daemon uses it to
heartbeat to the relay as a normal node, claim stages, complete tasks, and
submit tasks.

Constructor:
```python
client = RelayClient(meta: dict, cfg: dict)
# meta:  {"node_id": str, "node_name": str, ...} (from iowap-agent.json)
# cfg:   relay_config.json merged with env overrides (_effective_config())
# Properties: client.base_url (str), client.token (str), client.meta
```

### Key methods

| Method | Purpose |
|---------|-------|
| `heartbeat(caps: list[dict], in_flight: dict[str,int]) -> dict` | Heartbeat to `/relay/v2/discovery/heartbeat`. `caps` = active capabilities, `in_flight` = {cap_name: count}. |
| `claim(capability: str) -> dict \| None` | Claims a stage. `None` on 204 (nothing claimable). Otherwise a `stage` dict with `stage_id`, `task_id`, `payload`. |
| `complete(task_id: str, stage_id: str, result: dict) -> dict` | Reports the result of a stage to `/relay/v2/scheduler/stages/{stage_id}/complete`. |
| `submit_simple_task(capability: str, payload: dict, *, name="", priority=0, owner_node_id=None) -> dict` | Submits a new task. The remote FedNode uses this to submit an incoming forwarded task into its local relay. |
| `get_task(task_id: str) -> dict` | Task details incl. stages/artifacts/notes. |

### Token handling (central, no manual refresh needed)

- `maybe_refresh_token()` — proactive refresh before expiry (call in the heartbeat loop).
- `_current_backoff() -> float` — auth-error backoff (0 = ok). Write into the status.
- Auth/refresh/recovery is encapsulated in the client; `_post_with_retry`/`_get_with_retry` handle 401/403 automatically.

## nodes.common.node_daemon (reference pattern)

The SSE-driven daemon pattern shows the thread structure that the federation
daemon should mirror (T-096). Key points:

- **Thread 1 — Heartbeat:** `while not stop: client.maybe_refresh_token(); caps = load_active_profile(); with lock: inflight=dict(self._in_flight); hb = client.heartbeat(caps, inflight); sleep(interval + client._current_backoff())`
- **Claim/Execute/Complete:** `stage = client.claim(cap); ... context = {...RELAY_* envs...}; result = run_handler(handler, stage, context=ctx, timeout=...); client.complete(task_id, stage_id, result)`.
- `_in_flight: dict[cap, count]` with `threading.Lock()` — max_parallel enforcement.
- `_write_status(status_dict)` — status file via `write_json_atomic(STATUS_PATH, status)`.
- Signal handlers SIGTERM/SIGINT → `_stop_event.set()`.
- `load_active_profile()` → active caps list from the node.yaml.

For the federation daemon the **core difference** is: instead of running
`run_handler` locally, it writes a forward file into `outbox/` (thread 2) and
processes inbox files (thread 4). Heartbeat stays as above.

## nodes.common.node_utils

- `BASE_DIR` — `~/.relay` (path root).
- `write_json_atomic(path, data)` — atomic JSON write (tmp + rename).
- `load_config()` / `load_token()` / `save_token(token, expires_at=None)` — config/token access.
- `load_meta()` — iowap-agent.json.

## nodes.common.node_config

- `load_active_profile()` — active capabilities from node.yaml.
- `current_profile_name()` / `invalidate_active_cache()`.

## Important for tests

The RelayClient makes real HTTP calls to the `base_url`. For unit tests, mock
the client (e.g. a fake with the same method signatures) or use
`httpx.MockTransport`. The `_effective_config()` reads the `RELAY_BASE_URL`
env var — tests can set it, but the simplest is a fake client.

## Federation-specific deviation

The federation daemon heartbeats **only** `federation` + the admin-approved
remote capabilities (not all local ones). The local capabilities run in the
own relay via the normal node daemon. Federation is an **additional**
capability (`federation`) offered by the FedNode.