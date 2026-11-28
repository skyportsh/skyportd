# skyportd

Rust node daemon for Skyport. Manages Docker containers, console/stats WebSockets, SFTP, backups, and workflows on game server nodes.

## Tech Stack

- Rust 1.85 (`edition = "2024"`), tokio, axum, rusqlite (WAL), russh, rustls

## Commands

- `cargo test` - Run unit and integration tests (no Docker required)
- `cargo test -- --ignored` - Run Docker integration tests
- `cargo build --release` - Build release binary
- `cargo clippy` - Run lints

## Rules

The detailed rules live in `.agents/rules/`. Read the relevant file before acting:

- **Protocol Versioning** - [.agents/rules/protocol-versioning.md](.agents/rules/protocol-versioning.md) - Integer `PROTOCOL_VERSION` contract, decoupled `Cargo.toml` releases, and wire compatibility checks
- **Repo Boundaries** - [.agents/rules/repo-boundaries.md](.agents/rules/repo-boundaries.md) - Strict separation between `skyportd` and `skyportsh/installer`

## Universal Rules

- **CRITICAL**: NEVER add installation, update, migration, or systemd management shell scripts to this repository. All install/update/ops scripts belong exclusively in `skyportsh/installer` (`install-daemon.sh`, `update-daemon.sh`).
- **ALWAYS** keep `README.md` installation/update sections pointing to `https://github.com/skyportsh/installer` instead of duplicating scripts or instructions here.
