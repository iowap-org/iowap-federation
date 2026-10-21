# Federation — Implementation Notes

> **Concept source of truth:** [iowap-docs → federation/concept.md](https://github.com/iowap-org/iowap-docs/blob/main/federation/concept.md)
> (readable live at any relay: `/relay/v2/docs/federation-concept`).
>
> **Implementation status (canonical: "V1 forward path implemented; result
> return leg open"):** the V1 forward path is implemented in this
> repo (`federation/` package, forward-path tasks done, test suite in
> `tests/`); the section-by-section status table lives further below.
> Framework-internal APIs for developing against the node framework:
> [framework-ref.md](framework-ref.md).

## Implementation status

| Area | State |
|---|---|
| Transport wrapper (Inbox/Outbox, transport-agnostic) | implemented |
| Task forward/export path | implemented |
| Return path (results flowing back) | open design work |
| End-to-end encryption on the fed channel | implemented per message (`crypto_box`, T-101) |
| Enforcement of E2EE across all peers | open — a listener without crypto config still accepts plaintext envelopes (both modes coexist, see `federation/listener.py`) |

The status table above mirrors the repo board (`TASKS.md`); update both
together.

## Heartbeat-reported configuration (inventory)

A configuration change needs reporting to the server only when it changes
something the relay must know. This section is the current state of that
boundary — the node tail is what the relay sees, the worker part stays
local. It is the boundary a federation node extends.

**Reported today (node tail)** — built in
`iowap-node/nodes/common/relay_client.py:773-849` (`_build_heartbeat_payload`),
accepted by `POST /relay/v2/discovery/worker-heartbeat`
(`iowap-server/api/v2/discovery.py:59-87`) in **replace mode**
(`replace_capabilities=True`):

| Field | Source | Server-side effect |
|---|---|---|
| `status` | active profile (`node_config.py:754` `load_active_status`) | `busy`/`idle`/`online`; drives the auto-busy logic |
| `available`, `load`, `load_source`, `load_cap` | measured / config | `load_source` names the rung of the load chain (cgroup2/cgroup/loadavg) so the dashboard can tell measurements apart |
| `queue_depth` | in-flight map | sum over advertised capabilities |
| `capabilities[]` | active profile | `name`, `version`, `available` (derived from `max_parallel`, `relay_client.py:782`), plus `type`, `description`, `input_schema`, `upload_modes`, `result_path_hints` |
| `node_name`, `description` | meta file | surfaced via `node list` / `node info` |
| `forward_slots` | node.yaml (`node_config.py:707` `load_forward_slots`) | node-declared slots; server replaces its declaration rows (`iowap-server/core/forward.py:205` `replace_forward_slots`) |

The per-capability metadata is forwarded so the server can populate
`node_capabilities` (`iowap-server/core/db.py:516`) and resolve
`capability_details` on claim/task-view without an extra discovery
round-trip.

**Declared locally and deliberately not reported (worker part)** — the
profile schema allows these keys (`node_config.py:122`), the heartbeat
builder does not send them:

| Field | Why it stays local |
|---|---|
| `handler` | the executable path is meaningless to the server — it never runs the handler |
| `timeout` | execution detail of the worker part |
| `max_parallel` | the server learns the *result* via `available`; the local ceiling stays local |
| `auto_publish` | a local publishing decision, applied before the heartbeat (`relay_client.py:775` skips the capability entirely) |
| `claimable` | a local routing hint |
| `config` | arbitrary worker-part configuration; may contain local paths and secrets |

**What a federation node adds on top** (planned, not implemented — the
grant model is described in the concept page):

| Field | Source | Purpose |
|---|---|---|
| `peers[]` / routes | `federation.recipients` in node.yaml | which recipients the node can address, over which transport |
| exposed capabilities | `offer[].fed: true` | which of the node's own capabilities may leave via federation |
| open subscription requests | received over the fed channel | which remote capabilities the node wants to import |
| `fed_grants[]` (response) | server | which capabilities are released, for whom, with which limits |

**Prerequisite:** namespacing of imported capabilities
(`federation:<relay-name>:<capability>`) is specified in the concept page but
**not implemented** — `grep -rn "federation:" federation/` returns no hits in
the Python package. Until it exists, an imported `image.mflux` would collide
with a local capability of the same name.
