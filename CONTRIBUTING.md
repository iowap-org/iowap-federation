# Contributing

Thanks for your interest in iowap-federation!

## How to contribute

1. **Open an issue** first for non-trivial changes — discuss the approach before writing code.
2. **Fork** the repo and create a feature branch.
3. **Write tests** for your changes.
4. **Open a pull request** with a clear description of what and why.

## Conventions

- Python, type hints throughout.
- Follow the existing code style in the repo.
- Keep the Federation Node **transport-agnostic** — the node only reads/writes
  inbox/outbox files; network code lives in transport wrappers.
- **No relay code changes** — Federation is pure node code.
