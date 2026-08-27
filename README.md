# iowap-federation

**IOWAP Federation Node — bridge capabilities between relays**

A specialized IOWAP node that lets one relay use another relay's capabilities
without the relays ever knowing each other exist. Built on the Inbox/Outbox
message pattern with message-layer end-to-end encryption.

> **⚠️ Concept — not yet implemented.** This repo currently holds the design
> document only. The Federation Node is planned (Phase 22, T-094–T-107).

## What it does

A Federation Node heartbeats `federation` as its only capability. Once the
local admin approves a remote capability via the dashboard, the node starts
heartbeating it locally — so the local relay sees `image.gen.mflux` as a local
capability, even though it's executed on a remote relay.

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
- **E2EE on the message layer** — `crypto_box` (X25519 + XSalsa20-Poly1305)
- **Admin controls what gets heartbeated** — no capability is advertised without dashboard approval
- **One hop** — the node forwards directly to the target, no chain

## Design

The full design document lives in **[docs/federation.md](docs/federation.md)**.

## Package structure

```
federation/
├── transport.py          # Transport protocol + create_transport() factory
├── paths.py              # inbox/outbox directory layout per remote
└── transports/
    ├── http_transport.py # HTTP backend (T-095)
    └── ...               # email (T-100), p2p (later)
```

The node is transport-agnostic — it only reads/writes JSON files in
`inbox/`/`outbox/`. The `Transport` wrapper is the only network component.

## Status

| Phase | Scope | Tasks | Status |
|-------|-------|-------|--------|
| 22 | Federation V1 (HTTP, pairwise) | T-094–T-105 | 📝 planned |
| 23 | P2P discovery | T-106 | later |
| 24 | Channel model / gateway | T-107 | later |
