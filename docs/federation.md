# Federation — Implementation Notes

> **Concept source of truth:** [iowap-docs → federation/concept.md](https://github.com/iowap-org/iowap-docs/blob/main/federation/concept.md)
> (readable live at any relay: `/relay/v2/docs/federation-concept`).
>
> **Implementation status:** the V1 forward path is implemented in this
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
| End-to-end encryption on the fed channel | designed, not implemented |

The status table above mirrors the repo board (`TASKS.md`); update both
together.
