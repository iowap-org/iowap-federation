# Federation Node — Concept

> **⚠️ Concept — not yet implemented.** This document describes the planned
> Federation Node based on discussions (2026-08-01) and subsequent design
> refinements (2026-08-03). No code exists yet.
> See [IDEAS.md](../../.hermes/projects/iowap-server/IDEAS.md) for the
> original idea and [TASKS.md](../../.hermes/projects/iowap-server/TASKS.md)
> for the current board state.

## What is a Federation Node?

A **Federation Node** is a node that bridges capabilities between two or more
relays. It is not a special node type — it is a normal node that heartbeats the
`federation` capability. When connected to a remote relay, it can import remote
capabilities and make them available locally, or export local capabilities to
the remote relay.

```
┌── Local Relay ──────────────────┐       ┌── Remote Relay ─────────────────┐
│                                  │       │                                  │
│  ┌──────────────────────────┐   │       │   ┌──────────────────────────┐  │
│  │  Federation Node         │──┼───────┼──→│  Federation Node           │  │
│  │  heartbeatet `federation` │   │transport│   │  heartbeatet `federation` │  │
│  │  + subscribed caps       │   │  (HTTP  │   │  + exported caps         │  │
│  └──────────────────────────┘   │   /email │   └──────────────────────────┘  │
│         │                       │   /P2P)  │         │                       │
│    ┌────┴─────┐                 │       │    ┌────┴─────┐                 │
│    │ Tasks    │                 │       │    │ Tasks    │                 │
│    │ forwarded│                 │       │    │ executed │                 │
│    └──────────┘                 │       │    └──────────┘                 │
└──────────────────────────────────┘       └──────────────────────────────────┘
```

### Two-sided model: the node is a bridge

The Federation Node has **two strictly separated sides**:

```
┌──────────── Relay A ────────────┐          ┌──────────── Relay B ────────────┐
│                                 │          │                                 │
│  ┌─────────────────────────┐    │          │  ┌─────────────────────────┐    │
│  │  Fed Node A              │    │          │  │  Fed Node B              │    │
│  │                         │    │          │  │                         │    │
│  │  [Relay-Side]           │    │          │  │  [Relay-Side]           │    │
│  │  heartbeats like any    │    │          │  │  heartbeats like any    │    │
│  │  normal node            │    │          │  │  normal node            │    │
│  │  provides remote caps   │    │          │  │  provides remote caps   │    │
│  │                         │    │          │  │                         │    │
│  │  [Fed-Side]             │───►│ Fed-Kanal│◄──│  [Fed-Side]             │    │
│  │  forwards/receives      │    │          │  │  forwards/receives      │    │
│  └─────────────────────────┘    │          │  └─────────────────────────┘    │
└─────────────────────────────────┘          └─────────────────────────────────┘
```

- **Relay-Side** (inward): behaves exactly like a normal node — heartbeats,
  claims, completes. It exposes **no internals** of its own relay, only the
  provided remote capabilities.
- **Fed-Side** (outward): the actual federation — an encrypted channel to other
  Federation Nodes; forwards tasks to them or receives tasks from them.
- The local relay sees **only the Relay-Side** (a normal node). The federation
  sees **only the Fed-Side** (an encrypted channel).

### E2EE scope

The message-level E2EE protects **only the federation connection A↔B** (the
Fed-Side). Once Federation Node B decrypts a payload and submits it as a normal
task into its own local relay, that relay's **own security model applies**
(tokens, RBAC, etc.). E2EE secures the bridge, not the target relay — the
regular workers of B receive the plaintext under B's internal security
governance. This is intentional: each relay has its own security concept, and
the Federation Node is only the bridge between them.

### The relays never meet — only the nodes talk

The two relays **never know each other exists**. Only the Federation Nodes
communicate with each other over the encrypted channel. There is no
relay-to-relay connection and no relay ever learns the other relay's identity.

```
┌── Relay A ────────────┐     ═══ Federation channel ═══     ┌── Relay B ────────────┐
│                       │         (nodes only,               │                       │
│  Worker Nodes         │          relays never meet)        │  Worker Nodes         │
│  ┌─────────────────┐  │                                    │  ┌─────────────────┐  │
│  │ Relay A ↔ Fed A │  │        Fed A ◄══════► Fed B        │  │ Relay B ↔ Fed B │  │
│  └─────────────────┘  │       (only nodes talk)            │  └─────────────────┘  │
│                       │                                    │                       │
└───────────────────────┘                                    └───────────────────────┘
```

Consequences that this establishes:

1. **No relay-to-relay knowledge.** Relay A does not know Relay B exists. To A,
   Fed A is just a normal node offering the remote capabilities. Relay B does
   not know the tasks come from Relay A — to B, Fed B simply submits tasks.
   The Fed Node gives away no internals because it knows nothing about its
   relay beyond the tasks it forwards and receives.
2. **Federation is purely node-to-node.** The only connection is
   Fed A ↔ Fed B over the encrypted channel. The task flow inside B happens
   internally — B never learns that A was the originator.
3. **Multi-hop is structurally impossible.** Because the relays never meet,
   there can be no relay chain A→B→C. Federation is strictly pairwise between
   nodes — this is the OpenAI "no multi-hop in V1" recommendation, now
   guaranteed structurally rather than being only a design choice.
4. **The `connect` task is a pure node-to-node handshake.** The `{url, token}`
   authenticates the nodes against each other, not the relays. The trust anchor
   lives entirely between the two Federation Nodes; the token is a federation
   channel credential, not a relay token.

### Capability addressing: `capability@transport`

Every forwarded capability gets a **unique address** in the form
`capability@transport` — e.g. `chat.ai@mac-relay`, `image.gen@http://relay-b`.

This solves three problems:

1. **Name collisions** — relay A and relay B can both have `chat.ai`; the
   addressed form `chat.ai@b` disambiguates them.
2. **Loop detection** — a task addressed to `chat.ai@b` is processed only by B
   (it is the target relay); no node forwards an address that belongs to a
   different peer.
3. **Targeted routing** — the admin dashboard shows `chat.ai@b`, and tasks in A
   target that address.

## Core architecture: Inbox/Outbox + transport-agnostic node

The Federation Node is built on the **Inbox/Outbox message pattern** (the same
pattern proven in the Hermes Gateway Bridge Adapter). The node itself is
**fully transport-agnostic** — it only reads and writes files. The concrete
transport (HTTP, E-Mail, P2P) is handled by a swappable **Transport wrapper**,
the only component that touches the network.

```
┌─────────────────────────────────────────────────────────┐
│  Federation Node (transport-agnostic)                    │
│  • heartbeats `federation` + approved caps locally        │
│  • claims stage → writes forward file to outbox/          │
│  • polls inbox/ → completes stage locally                 │
│  • NO network access, NO remote_task_id knowledge         │
└───────────────────────┬─────────────────────────────────┘
                        │ file only
┌───────────────────────┴─────────────────────────────────┐
│  Transport wrapper (the ONLY network component)          │
│  • knows HTTP / E-Mail / P2P                             │
│  • translates file ↔ transport                           │
│  • carries remote_task_id ↔ local stage mapping          │
└──────────────────────────────────────────────────────────┘
        ┌───────────────┬───────────────┬───────────────┐
   transport_type:   http           email          p2p
```

### Key properties

- **The wrapper is the only component that knows the transport.** The node,
  handler and dashboard never see a socket — they only read/write JSON files
  in `inbox/`/`outbox/`.
- **New transport = a new wrapper class.** HTTP, E-Mail and P2P are all
  interchangeable backends behind the same `create_transport()` factory,
  selected by `transport_type` in the node config. No system change.
- **Crash-safe by design.** A task is a file. If the connection drops, the
  file stays in `outbox/` and the wrapper simply re-syncs when the connection
  returns. Nothing is lost.
- **Async by nature.** The two relays do not need to be online at the same
  time — the transport (especially E-Mail) buffers until both sides are ready.

### Directory layout (per remote connection)

```
~/.relay/federation/<remote-relay-name>/
├── inbox/      # results from remote relay → node processes → completes stage
└── outbox/     # task forwards → wrapper syncs out
```

## Transport abstraction

The `Transport` interface mirrors the pluggable database abstraction in the
server (`core/db.py`): a `Database` protocol → `create_database()` factory on
`db_type`. Here it is a `Transport` protocol → `create_transport()` factory on
`transport_type`.

```python
# nodes/common/federation/transport.py
class Transport:
    """Pluggable federation transport backend.

    The only component that touches the network. Translates files in
    inbox/outbox into transport calls (HTTP, E-Mail, P2P). Carries the
    remote_task_id ↔ local stage mapping. A new transport = new class +
    factory entry + config line — no node changes.
    """

    def connect(self, url: str, token: str) -> None:
        """Establish a session to the remote relay. Raise on failure."""
        raise NotImplementedError

    def sync_outbox(self, outbox_dir: Path) -> list[str]:
        """Send pending outbox task files to the remote. Return ids sent."""
        raise NotImplementedError

    def sync_inbox(self, inbox_dir: Path) -> list[str]:
        """Fetch remote results, write them as inbox files. Return new ids."""
        raise NotImplementedError

    def discover_capabilities(self) -> list[dict[str, Any]]:
        """Return the remote relay's advertised capabilities (for approval)."""
        raise NotImplementedError

    def close(self) -> None:
        """Release resources. No-op by default."""
        pass


def create_transport(cfg: dict[str, Any]) -> Transport:
    transport_type = cfg.get("federation.transport_type", "http")
    if transport_type == "http":
        from nodes.common.federation.transports.http_transport import HttpTransport
        return HttpTransport(cfg)
    # if transport_type == "email":   # T-100
    #     from ...email_transport import EmailTransport
    #     return EmailTransport(cfg)
    # if transport_type == "p2p":     # later, optional
    #     from ...p2p_transport import P2pTransport
    #     return P2pTransport(cfg)
    raise ValueError(f"Unknown transport_type: {transport_type!r}")
```

### Transport backends

The transport is **open-ended** — any backend that can move a JSON file from
one place to another behind the `Transport` interface works. The table lists
illustrative examples:

| `transport_type` | Example backend | Use case | NAT |
|------------------|-----------------|----------|-----|
| `http` | **HTTP/HTTPS (V1, default)** — `POST /relay/v2/scheduler/task-simple`, poll `/relay/v2/scheduler/tasks/{id}` | Fast LAN / Tailscale | Tailscale or reachable host |
| `email` | **E-Mail** — task JSON in mail body, `fed_<id>` subject | Async fallback (illustrative) | None (works everywhere) |
| `p2p` | **P2P (QUIC/WebRTC)** — later, optional | Fully decentralized | NAT-traversal, no port forwarding |

A backend that maps naturally onto the Inbox/Outbox pattern (e.g. E-Mail,
which is itself an asynchronous message queue) needs no node changes — only a
new `Transport` class behind the factory. The choice of transport is purely a
config line; it has no effect on the node, handler, or dashboard.

## Security: message-level end-to-end encryption

Because the transport is open-ended, security must **not** depend on any one
transport. It lives at the **message layer**: the JSON payload is encrypted by
the Federation Node *before* it is written to `outbox/`, and decrypted only
when read from `inbox/`. The transport wrapper moves ciphertext and nothing
else, so confidentiality, authenticity and integrity hold across HTTP, E-Mail
and P2P alike. (Security model validated by external review, 2026-08-03.)

### Layered model

| Layer | Mechanism | Role |
|-------|-----------|------|
| **Message layer** | `crypto_box` (X25519 + XSalsa20-Poly1305) | End-to-end E2EE — confidentiality, authenticity, integrity in one primitive |
| **Transport layer** | HTTPS / STARTTLS / DTLS | Defense-in-depth only |

> **Naming correction (OpenAI review, 2026-08-03):** libsodium's classic `crypto_box`
> uses **XSalsa20-Poly1305**, not XChaCha20-Poly1305. XChaCha20-Poly1305 belongs
> to `secretstream_xchacha20poly1305` / other AEAD constructions. If XChaCha20 is
> preferred, use `crypto_kx` + `secretstream_xchacha20poly1305` instead of
> `crypto_box`.

Transport-level TLS alone is **not** sufficient in an Inbox/Outbox system:
messages land as files on intermediate file systems, so TLS-only leaves them
plaintext at rest on disk and vulnerable when a transport lacks native TLS
(e.g. plain SMTP or P2P). Message-layer E2EE makes relays "blind couriers" —
compromised nodes, leaked backups, or malicious filesystem access cannot read
the payload.

### Why TLS-only is not enough (review comparison)

| Security property | Transport TLS only | Message E2EE |
|-------------------|--------------------|--------------|
| In-transit | ✅ between hops | ✅ end-to-end |
| At-rest (`inbox/` on disk) | ❌ plaintext | ✅ stays ciphertext |
| Intermediary trust | ❌ must trust relay storage | ✅ zero-trust, blind courier |
| Transport agnosticism | ❌ vulnerable on plain SMTP/P2P | ✅ inherently secure over any transport |

### Crypto scheme

- Each relay has an X25519 keypair; public keys are shared **out-of-band**
  (e.g. during the `connect` task with the token, or manually).
- Every message gets a **fresh 24-byte random nonce** via `os.urandom(24)` /
  libsodium `randombytes_buf`. **Never reuse a nonce with the same keypair** —
  a collision destroys confidentiality. XChaCha20's 192-bit nonce space makes
  collisions negligible with random generation.
- The plaintext envelope carries: a unique message id (`jti`), a strict
  timestamp, and a **key fingerprint** of the sender's public key so the
  receiver knows which Curve25519 key to use for decryption.

```python
import nacl.bindings
# encrypt: local_sk + remote_pk + nonce → ciphertext
# decrypt: remote_sk + local_pk + nonce → plaintext
```

### Hard requirements (built in from day one)

1. **Nonce discipline** — a fresh random nonce per message; never a static
   counter or reused nonce.
2. **Replay protection** — because messages are static files in `inbox/`, an
   attacker could capture a valid ciphertext file and re-inject it. The
   receiver keeps a stateful set of processed message ids and rejects
   duplicates.
3. **DoS protection** — strict file-size limits and cheap structural checks in
   the transport wrapper *before* the decryption pipeline, so a flood of
   garbage files cannot exhaust CPU.

### Key management

Out-of-band key distribution is fine for a small trusted federation (2–10
relays). For rotation/revocation, prefer **TOFU with local config pinning**
(public keys pinned in `node.yaml`) plus a lightweight signed key directory
when the federation grows. A leaked private key requires rotating it across
all federated partners.

Config:

```yaml
federation:
  transport_type: http
  crypto:
    local_private_key: "..."     # stays on this relay
    remote_public_key: "..."     # public key of the remote relay
    key_fingerprint: "..."       # optional: pin the remote key id
```

### Scalability (future, not V1)

- **Envelope encryption (hybrid)** — for large payloads, encrypt the body with
  a random one-time ChaCha20 key and wrap only that small key with
  `crypto_box_seal`. Faster than boxing the whole payload.
- **DIDComm / Matrix-style protocols** — if the federation grows beyond a
  point-to-point bridge, these standardize transport-agnostic message E2EE
  (Curve25519/Ed25519) for agent/relay architectures.
- **P2P as a discovery layer** — the federation's discovery source grows from
  "out-of-band configured peers" to a P2P mesh as a capability search space.
  Each Federation Node publishes its offer into the mesh, and any node can
  search ("who can do `chat.ai`?"). The **subscription model stays unchanged**:
  P2P only shows the offer — approval stays manual and reviewed (default: no
  import), and pinning against drift still applies. P2P is a **discovery
  layer**, not merely another transport backend. The trust anchor remains with
  the admin (a mesh is not trustless, just a larger search space). This is
  **not V1** — it only matters once the federation grows beyond a handful of
  relays.

## Capability

### `federation`

The Federation Node heartbeats `federation` as its only capability. It does not
heartbeat the remote capabilities directly — those are only heartbeated after
the local admin approves them via the dashboard.

## How it works

### 1. Peer registration (out-of-band, not a connect task)

The federation channel credentials are exchanged **out-of-band** by the two
operators/admins. Each Federation Node has a set of configured peers in
`node.yaml`. `connect` is not a task — it is the **activation** of an already
configured peer.

```yaml
# node.yaml (Fed A)
federation:
  peers:
    - id: "fed-b"
      endpoint: "https://fed-b:8791"
      token: "fed_t_..."          # given to A by B's operator, out-of-band
      peer_public_key: "..."      # Fed B's X25519 public key (out-of-band)
      my_public_key: "..."        # Fed A's X25519 public key (shared with B)
```

Peers are managed with the CLI (no config edit needed for temporary bridges):

```bash
node-cli federation peer add fed-b https://fed-b:8791 fed_t_... <pubkey>
node-cli federation peer remove fed-b
node-cli federation peer list
```

**Why out-of-band (no dynamic handshake endpoint in V1):** the trust anchor is
the manually shared configuration. There is no initial handshake that would
itself be an attack surface, and no new endpoint on the Fed-Side. This is the
same out-of-band pattern already used for the E2EE public-key distribution.

### 2. Mutual authentication (every sync cycle)

Each sync authenticates both directions without any relay involvement:

- **Channel token** (`fed_t_...`) proves "I am the configured peer Fed A".
- **`crypto_box`** signed with B's public key proves "and I hold the private
  key matching A's public key, which you received out-of-band".

Together these give mutual authentication between the two Federation Nodes,
independent of both relays. Once the peer is activated, the node builds the
transport via `create_transport(cfg)` and starts syncing.

### 3. Discover

Once connected, the transport fetches the remote **peer node's** capability
list via `discover_capabilities()`:
```
[mflux.generate, esrgan.upscale, chat.ai]
```

### 4. Dashboard page = the subscription catalog

The node deploys a dashboard page (via the SSN's `ssn.pages` capability or its
own Dynamic Route). It acts as a **subscription catalog** — it shows the
remote peer's offer plus your current subscriptions. Nothing is imported
automatically.

| Column | Meaning |
|--------|---------|
| **Capability** | Remote name (`chat.ai`, `image.gen.mflux`) + version |
| **Local name** | Editable translation (`chat.ai@b` → `chat.ai`) |
| **Status** | `available` / `subscribed` / `paused` / `rejected` |
| **Fair-use** | `max_parallel`, `max_daily` (set per subscription) |
| **Pinned version** | `schema_hash` of the approved interface |
| **Activity** | Tasks completed, errors, latency |

Remote capabilities are **only visible here** — they are not in the local
relay and not heartbeated until explicitly subscribed.

### 5. Subscribe (manual approval, default: no import)

The admin reviews each remote capability against a checklist (need, trust
level, sensitivity, fair-use, version/pinning) and explicitly subscribes to
the ones worth importing:

```
[x] chat.ai       →  chat.ai        max_parallel: 2   max_daily: 100
[x] ocr           →  ocr@b          max_parallel: 1   max_daily: 20
[ ] image.gen     →  (not needed, we have image.gen locally)
```

A subscription is **explicit and reviewed** — no capability is adopted
automatically just because the peer offers it. This is the "subscription"
model: you see the offer (auto-discover), choose what to subscribe to (manual
approval), set per-subscription limits, and the subscription is pinned to a
specific version (`schema_hash`).

**Pinning against drift:** if the remote later changes a capability (version
or schema), your existing subscription does **not** silently change. It is
flagged as "update available" and requires re-review by the admin before it
takes effect.

### 6. Node heartbeats approved capabilities

Only after admin approval, the node starts heartbeating the approved
capabilities locally. The relay now sees `image.gen.mflux` as a local
capability.

### 7. Task forwarding

When a task arrives for `image.gen.mflux`:

1. Federation Node A claims the stage
2. Node A writes a forward file to `outbox/<remote>/`
3. Transport A `sync_outbox()` sends it **only to Federation Node B**
   (over HTTP — it never talks to Relay B's scheduler directly)
4. Federation Node B receives it and submits it **as a normal task into its
   own local Relay B** (using B's own node token, `POST /relay/v2/scheduler/task-simple`)
5. Relay B matches the capability and routes it to its regular worker nodes,
   which process it normally
6. Transport A `sync_inbox()` fetches the result from Federation Node B,
   writes it as an inbox file
7. Node A reads the inbox file, completes the stage locally

**Key point:** the HTTP transport never reaches the remote relay's scheduler
directly. It only reaches the remote Federation Node, which then re-submits
the task into its local relay as an ordinary task. The remote relay treats the
incoming work exactly like any locally submitted task — its own worker nodes
claim and execute it, with no knowledge that it came from a federation.

## Config

```yaml
# ~/.relay/node.yaml (Federation Node)
node_name: federation
description: "Bridge to remote relays"
federation:
  transport_type: http        # http | email | p2p
  peers:
    - id: "fed-b"
      endpoint: "https://fed-b:8791"
      token: "fed_t_..."          # out-of-band, from the remote operator
      peer_public_key: "..."      # Fed B's X25519 public key (out-of-band)
      my_public_key: "..."        # Fed A's X25519 public key (shared with B)
capabilities:
  - name: federation
    type: native
    claimable: true
    handler: /opt/relay/handlers/federation-handler.sh
    max_parallel: 10
    timeout: 600
    description: "Bridges capabilities from remote relays"
```

## Dashboard page

The dashboard page is hosted by the **SSN** (via `ssn.pages`) or by the
Federation Node itself via **Dynamic Routes**. It is an HTMX page that shows:

```
┌─────────────────────────────────────────────┐
│  🌐 Federation — Mac Relay                   │
│  Status: ● Connected  (latency: 12ms)       │
│                                               │
│  Available capabilities:                      │
│                                               │
│  ☑ image.gen.mflux  →  mflux.generate        │
│     max_parallel: 2  │  max_daily: 50        │
│     Tasks today: 12  │  Errors: 0            │
│                                               │
│  ☐ image.upscale  →  esrgan.upscale          │
│     max_parallel: 1  │  max_daily: 20        │
│                                               │
│  [Save]  [Disconnect]                         │
└─────────────────────────────────────────────┘
```

## Multiple remote relays

A single Federation Node can connect to multiple remote relays. Each connection
has its own transport instance (via `create_transport()`) and its own
inbox/outbox directory. Each connection has its own dashboard card:

```
┌── Federation ──────────────────────────────┐
│                                              │
│  ┌── Mac Relay ──────────────────────────┐  │
│  │  ● Connected  │  ☑ image.gen.mflux    │  │
│  └────────────────────────────────────────┘  │
│                                              │
│  ┌── Community Relay ─────────────────────┐  │
│  │  ● Connected  │  ☐ llm.chat           │  │
│  │               │  ☐ web.ai             │  │
│  └────────────────────────────────────────┘  │
│                                              │
│  ┌── Friend's 4090 ───────────────────────┐  │
│  │  ○ Disconnected                        │  │
│  └────────────────────────────────────────┘  │
└──────────────────────────────────────────────┘
```

## Reliability & lifecycle (from Gemini + OpenAI reviews, 2026-08-03)

### Message lifecycle + ACK protocol

Give every message an explicit state machine, not implicit states:

```
CREATED → ENCRYPTED → QUEUED → SENT → ACKED → COMPLETED → ARCHIVED
                    ↘ FAILED / EXPIRED / REJECTED
```

- **ACK is mandatory.** Transport success ≠ delivery. The receiver ACKs only
  after writing the message to `inbox/`; the sender removes the outbox file
  only after receiving the ACK. Without ACKs, data is eventually lost.
- **Delivery semantics:** design for **at-least-once** delivery plus
  **idempotent processing** (deduplicate on `message_id`), not exactly-once —
  exactly-once is rarely achievable and much more complex.

### Async transport mismatch

Different transports have very different timing. E-Mail is asynchronous while
HTTP is request/response. A task waiting on an async transport must move to a
`pending_remote_response` state rather than blocking a worker or connection.
Do not assume a synchronous request/result cycle.

### SQLite metadata index + directory partitioning

- **SQLite holds message metadata** (message_id, state, retry_count, ownership,
  timestamps); files hold only payloads. Avoids scanning directories with
  thousands of files.
- **Hash-partition** outbox/inbox (`00/ 01/ ... ff/` via
  `SHA256(message_id)[:2]`) so flat directories don't degrade.
- **Aggressive cleanup/archival** of files after terminal state — prevents
  inode exhaustion.
- **Replay cache** must be TTL-pruned (drop processed ids older than the
  message expiry window), or it grows unbounded.
- **File I/O:** write to a temp file (`.tmp_<id>`) then atomic `os.rename()`
  into place, so readers never see a partially-written file.

### Retry policy

Exponential backoff with jitter (e.g. `30s 1m 2m 4m 8m 16m 30m 60m`), not
"every 60s forever". Two relays rebooting simultaneously can otherwise
synchronize their retries. Use a `sync()` cycle (upload → download → ACKs →
cleanup) rather than separate send/receive.

### Capability pinning + versioning

- **Pin the approved capability** to a hash/interface fingerprint + version.
  The remote must not silently grow `OCR → OCR+Shell.Execute`. A capability
  change (schema/version) requires re-review by the admin.
- Include `schema_hash` / `interface_hash` in the message envelope so interface
  changes are detected instantly.
- Treat heartbeats as health signals, **not** registration — capability
  registration should survive missed heartbeats.

### Back-pressure

- The remote publishes `queue_depth` / `load`; routing adapts. A 500-worker
  relay must not flood a 2-worker relay.
- Expand fair-use limits beyond `max_parallel`/`max_daily` to also cover
  `requests/day`, `bytes/day`, `runtime/day`, `burst`, `priority`.
- No multi-hop federation in V1 — keep it pairwise (A↔B). Transitive
  federation (A→B→C) adds routing, trust, policy, latency and failure
  complexity.

### Observability

Emit structured events from day one:
`MESSAGE_CREATED MESSAGE_ENCRYPTED MESSAGE_SENT MESSAGE_ACKED MESSAGE_RETRIED
MESSAGE_DECRYPTED MESSAGE_COMPLETED MESSAGE_FAILED`, with message_id, remote,
latency, retry count, size. This makes diagnosing issues far easier than
reconstructing state from files alone.

## Channel model (vision) — a gateway, not just a bridge

The Federation Node is not only a relay-to-relay bridge. It is a **universal
gateway**: tasks can be distributed and collected over **any channel** a node
can reach. The core system (Inbox/Outbox, E2EE, subscription, two-sided model)
is unchanged — each channel is just an adapter behind the same wrapper
abstraction.

```
channel_type: http | email | p2p | x | activitypub | web | messaging | ...
```

Tasks can flow in and out over:

| Channel | How tasks move |
|---------|----------------|
| **HTTP/HTTPS** | relay-to-relay (V1) |
| **E-Mail** | async, works over any NAT |
| **P2P / Mesh** | discovery + distribution (Phase 23) |
| **X / Twitter** | post = task, reply = result |
| **ActivityPub** | Mastodon / Fediverse, decentralized |
| **Websites** | Federation Nodes listed in web directories |
| **Messaging** | Matrix, Signal, iMessage, WhatsApp… |

**The three layers stay cleanly separated:**

| Layer | Question | Channels |
|-------|----------|----------|
| **Discovery** | where do I find capabilities? | peer config, P2P mesh, web directory, ActivityPub list |
| **Transport** | how do tasks get through? | HTTP, E-Mail, P2P, X, messaging |
| **Subscription** | what do I want to use? | catalog + manual approval (unchanged) |

A single channel can serve multiple layers (X can do discovery *and*
transport). All core principles hold on every channel: subscription model,
manual approval (default no import), E2EE, Inbox/Outbox.

> **Scale note (2026-08-03):** the Federation Node is expected to grow into a
> system **as large as the relay server itself**. This is intentional — it is
> the "Capability Mesh" vision: not just a task router, but an open gateway
> that distributes and collects tasks over every conceivable medium. Concrete
> channel integrations (X, ActivityPub, web, messaging) are separate projects
> with their own pitfalls (API limits, protocol signing, federation models),
> planned as Phase 24+ when there is need — **none are V1**.

## Key properties

- **No relay code changes** — Federation is pure node code
- **Federation is a capability** — the node heartbeats `federation`, nothing else
- **Transport-agnostic node** — the node only reads/writes inbox/outbox files
- **Swappable transport/channel** — HTTP / E-Mail / P2P / X / ActivityPub / web / messaging via factory
- **Crash-safe** — tasks are files; a dropped connection loses nothing
- **Admin controls what gets heartbeated** — no capability is advertised without dashboard approval
- **Capability translation** — local name ≠ remote name (admin sets the local name)
- **Multiple remote relays / channels** — one Federation Node, many connections, one transport each
- **One hop** — the node forwards directly to the target, no chain
- **Fair use** — `max_parallel`, `max_daily` as policy mechanism, no money
- **Temporary bridges** — connect, use, disconnect. No permanent setup needed

## What the Federation Node needs (node code, no relay code)

- `nodes/common/federation/transport.py` — `Transport` protocol + `create_transport()` factory
- `nodes/common/federation/transports/http_transport.py` — HTTP backend (V1)
- `nodes/common/federation/federation_node.py` — Daemon (claim, forward, complete; inbox/outbox processing)
- `federation` handler — peer management, capability discovery
- Dashboard HTML (capability page or Dynamic Route) — subscription catalog, peer status
- Fair-use policy — `max_parallel`, `max_daily` enforcement in the daemon
- Later: one channel adapter per medium (X, ActivityPub, web, messaging) — Phase 24+

## See also

- **[concept.md](concept.md)** — what a node is
- **[capability-concept.md](capability-concept.md)** — what a capability is
- **[capabilities.md](capabilities.md)** — capability reference, naming, handler contract
- **[ssn.md](ssn.md)** — SSN implementation (hosts the federation dashboard page)
- **[node-config.md](node-config.md)** — `node.yaml` format
- **Inbox/Outbox pattern** — inspired by the Hermes Gateway Bridge Adapter
- **Pluggable backend pattern** — mirrors `core/db.py` (`Database` → `create_database()`)
