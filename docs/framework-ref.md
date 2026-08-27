# iowap-node Framework API Reference (für Federation-Entwicklung)

> Dieses Referenz-File liegt im Federation-Repo, damit der Code-Entwickler die
> Framework-APIs nutzen kann, ohne in das `~/projects/iowap-node`-Verzeichnis
> schauen zu müssen (Permission-Sandbox blockt externe Verzeichnisse). Das
> Framework ist als Editable-Install im venv verfügbar — Importe funktionieren.

## Verfügbarkeit

```bash
.venv/bin/pip install -e ../iowap-node   # Editable-Install im venv
.venv/bin/python -c "from nodes.common.relay_client import RelayClient"
```

## nodes.common.relay_client.RelayClient

Der gemeinsame HTTP-Client zu einem AI-Relay. Der Federation Daemon nutzt ihn,
um als normaler Node am Relay zu heartbeaten, Stages zu claimen, Tasks zu
completen und Tasks einzureichen.

Konstruktor:
```python
client = RelayClient(meta: dict, cfg: dict)
# meta:  {"node_id": str, "node_name": str, ...} (aus iowap-agent.json)
# cfg:   relay_config.json gemerged mit env-overrides (_effective_config())
# Eigenschaften: client.base_url (str), client.token (str), client.meta
```

### Wichtigste Methoden

| Methode | Zweck |
|---------|-------|
| `heartbeat(caps: list[dict], in_flight: dict[str,int]) -> dict` | Heartbeat an `/relay/v2/discovery/heartbeat`. `caps` = aktive Capabilities, `in_flight` = {cap_name: count}. |
| `claim(capability: str) -> dict \| None` | Claimt einen Stage. `None` bei 204 (nichts claimbar). Sonst `stage` dict mit `stage_id`, `task_id`, `payload`. |
| `complete(task_id: str, stage_id: str, result: dict) -> dict` | Meldet Ergebnis eines Stages an `/relay/v2/scheduler/stages/{stage_id}/complete`. |
| `submit_simple_task(capability: str, payload: dict, *, name="", priority=0, owner_node_id=None) -> dict` | Reicht einen neuen Task ein. Der Remote-FedNode nutzt das, um einen ankommenden Forward-Task in seinen lokalen Relay einzureichen. |
| `get_task(task_id: str) -> dict` | Task-Details inkl. Stages/Artifacts/Notes. |

### Token-Handling (zentral, kein manueller Refresh nötig)

- `maybe_refresh_token()` — proaktiv refresh vor Ablauf (in heartbeat-loop aufrufen).
- `_current_backoff() -> float` — Auth-Fehler-Backoff (0 = ok). In Status schreiben.
- Auth/Refresh/Recovery ist im Client gekapselt; `_post_with_retry`/`_get_with_retry` handlen 401/403 automatisch.

## nodes.common.node_daemon (Referenz-Muster)

Das SSE-getriebene Daemon-Muster zeigt die Thread-Struktur, die der Federation
Daemon nachbilden soll (T-096). Kernpunkte:

- **Thread 1 — Heartbeat:** `while not stop: client.maybe_refresh_token(); caps = load_active_profile(); with lock: inflight=dict(self._in_flight); hb = client.heartbeat(caps, inflight); sleep(interval + client._current_backoff())`
- **Claim/Execute/Complete:** `stage = client.claim(cap); ... context = {...RELAY_* envs...}; result = run_handler(handler, stage, context=ctx, timeout=...); client.complete(task_id, stage_id, result)`.
- `_in_flight: dict[cap, count]` mit `threading.Lock()` — max_parallel-Enforcement.
- `_write_status(status_dict)` — Statusdatei via `write_json_atomic(STATUS_PATH, status)`.
- Signal-Handler SIGTERM/SIGINT → `_stop_event.set()`.
- `load_active_profile()` → aktive Capabilities-Liste aus der node.yaml.

Für den Federation Daemon ist der **Kernunterschied**: statt `run_handler` lokal
auszuführen, schreibt er einen Forward-File in `outbox/` (Thread 2) und
verarbeitet Inbox-Files (Thread 4). Heartbeat bleibt wie oben.

## nodes.common.node_utils

- `BASE_DIR` — `~/.relay` (Pfad-Root).
- `write_json_atomic(path, data)` — atomarer JSON-Write (tmp + rename).
- `load_config()` / `load_token()` / `save_token(token, expires_at=None)` — Config/Token-Zugriff.
- `load_meta()` — iowap-agent.json.

## nodes.common.node_config

- `load_active_profile()` — aktive Capabilities aus node.yaml.
- `current_profile_name()` / `invalidate_active_cache()`.

## Wichtig für Tests

Der RelayClient macht reale HTTP-Calls zur `base_url`. Für Unit-Tests den
Client mocken (z.B. ein Fake mit gleicher Methodensignatur) oder `httpx.MockTransport`
verwenden. Die `_effective_config()` liest `RELAY_BASE_URL` env — Tests können
das setzen, aber am einfachsten ist ein Fake-Client.

## Federation-spezifische Abweichung

Der Federation Daemon heartbeats **nur** `federation` + die admin-freigegebenen
Remote-Capabilities (nicht alle lokalen). Die lokalen Capabilities laufen im
eigenen Relay via normalem Node-Daemon. Federation ist eine **zusätzliche**
Capability (`federation`), die der FedNode anbietet.
