# iowap-federation

**IOWAP Federation Node — bridge capabilities between relays**

A specialized IOWAP node that lets one relay use another relay's capabilities
without the relays ever knowing each other exist. Built on the Inbox/Outbox
message pattern with message-layer end-to-end encryption.

> **Status:** V1 forward path implemented; result return leg open (canonical
> string — keep verbatim across repos; detail: "Implementation status" below
> and in the umbrella board). Not yet wired end-to-end: the **result return
> leg** (B → A reply delivery + results endpoint) and dashboard-driven
> subscription management (T-098, moved to the server board). See
> [docs/federation.md](docs/federation.md) → "Implementation status" and
> "Open gaps".

## What it does

A Federation Node heartbeats `federation` as its only capability. Tasks for
forwarded capabilities are claimed by the node, written as encrypted
envelopes to a per-peer `outbox/`, delivered over HTTPS to the remote
Federation Node's listener (`POST /fed/v1/inbox`), and re-submitted there as
ordinary tasks into the remote relay. Results come back the same way — a
`reply` envelope in the local `inbox/` completes the originally claimed stage.

```
┌── Relay A ────────────┐     ═══ Federation channel ═══     ┌── Relay B ────────────┐
│                       │         (nodes only,               │                       │
│  Worker Nodes         │          relays never meet)        │  Worker Nodes         │
│  ┌─────────────────┐  │                                    │  ┌─────────────────┐  │
│  │ Relay A ↔ Fed A │  │        Fed A ◄══════► Fed B        │  │ Relay B ↔ Fed B │  │
│  └─────────────────┘  │       (only nodes talk)            │  └─────────────────┘  │
└───────────────────────┘                                    └───────────────────────┘
```

## Key properties

- **No relay code changes** — Federation is pure node code
- **Federation is a capability** — the node heartbeats `federation`, nothing else
- **Transport-agnostic node** — the node only reads/writes inbox/outbox files
- **Swappable transport** — HTTP (V1), E-Mail, P2P via factory
- **E2EE on the message layer** — `crypto_box` (X25519 + XSalsa20-Poly1305),
  with replay cache, size guards, and ACK-based lifecycle
- **At-least-once delivery** — idempotent processing (dedupe on `message_id`),
  exponential backoff with full jitter
- **Fair use** — per-capability `max_parallel` / `max_daily` enforcement
- **SQLite metadata index** — `state.db` + hash-partitioned payload files
- **One hop** — the node forwards directly to the target, no chain

## Install & run

```bash
python -m venv .venv && .venv/bin/pip install -e .
# the daemon talks to a local relay; the framework is imported from iowap-node
.venv/bin/pip install -e ../iowap-node
```

Configure peers in `~/.relay/node.yaml` (or manage them with the CLI):

```bash
fed-cli federation peer add fed-b https://fed-b:8791 fed_t_... <peer-pubkey>
fed-cli federation peer list
```

```yaml
# ~/.relay/node.yaml (Fed A) — minimal federation section
federation:
  peers:
    - id: "fed-b"
      endpoint: "https://fed-b:8791"
      token: "fed_t_..."          # B's listener token, given to A out-of-band
      peer_public_key: "..."      # Fed B's X25519 public key (out-of-band)
      my_public_key: "..."        # Fed A's X25519 public key (shared with B)
  listener_token: "fed_t_..."     # what peers must present to our listener
  offer:                          # capabilities we expose to peers
    - name: image.gen
      claimable: true
```

Run the two processes:

```bash
federation-daemon                # claim/forward/complete loop + sync cycle
uvicorn federation.listener:create_app_from_config_file --factory --port 8791
```

Both ends are symmetric: each site runs a relay server, a federation daemon,
and the same P2P listener.

## Package structure

```
federation/
├── federation_node.py    # daemon: heartbeat, claim → forward, inbox → complete (T-096)
├── transport.py          # Transport protocol + create_transport() factory (T-094)
├── transports/http_transport.py  # HTTP backend (T-095)
├── listener.py           # FastAPI P2P listener: /fed/v1/{inbox,discover,ack,results} (T-095)
├── envelope.py           # plaintext envelope ⇄ sealed ciphertext envelope (T-101)
├── crypto.py             # Crypto (crypto_box wrapper), SizeGuard, fingerprints (T-101)
├── lifecycle.py          # MessageState machine, ReplayGuard, RetryPolicy (T-102)
├── ack.py                # SenderAck / ReceiverAck ledgers (T-102)
├── index.py              # MetadataIndex (state.db) + archive/prune (T-103)
├── fairuse.py            # FairUse: max_parallel / max_daily (T-099)
├── peers.py              # PeerManager (node.yaml peers section) (T-097)
├── cli.py                # fed-cli (peer management) (T-097)
└── paths.py              # ~/.relay/federation/<remote>/{inbox,outbox,...} (T-094)
```

The node is transport-agnostic — it only reads/writes JSON files in
`inbox/`/`outbox/`. The `Transport` wrapper is the only network component.

## Design

The full design document lives in **[docs/federation.md](docs/federation.md)**.

## Status

| Phase | Scope | Tasks | Status |
|-------|-------|-------|--------|
| 22 | Federation V1 (HTTP, pairwise) | T-094–T-105 | 🚧 in progress — T-094–T-103 done, T-104–T-105 open |
| 23 | P2P discovery | T-106 | later |
| 24 | Channel model / gateway | T-107 | later |

## License

MIT — see [LICENSE](LICENSE).
