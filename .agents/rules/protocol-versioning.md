# Protocol Versioning

Decouples `skyportd` release versions (`Cargo.toml`) from Skyport Panel wire-protocol compatibility checks.

## When this applies

- Modifying `src/configuration.rs`, `src/service.rs`, `src/server_lifecycle.rs`, `src/api/mod.rs`, `src/api/backups.rs`, or `src/api/websocket.rs`.
- Bumping `Cargo.toml` version or changing any Panel <-> `skyportd` JSON/query/WebSocket contract.

## Rules

- **CRITICAL**: NEVER compare `panel_version` against `env!("CARGO_PKG_VERSION")` (`CURRENT_VERSION`); ALWAYS validate `protocol_version` against `crate::configuration::PROTOCOL_VERSION` (fail-closed) in `ensure_compatible_panel_protocol` and `ensure_compatible_request`.
- **CRITICAL**: ALWAYS send `"protocol_version": PROTOCOL_VERSION` alongside `"version": env!("CARGO_PKG_VERSION")` in enroll, heartbeat, server sync, and `/runtime` payloads.
- **CRITICAL**: ALWAYS require `protocol_version: u32` (and keep `panel_version: String` informational) on every panel-facing request, query struct, and `WsTokenPayload` in `src/api/`.
- **CRITICAL**: NEVER make a breaking change to panel-facing routes, payloads, or WebSocket auth without incrementing `PROTOCOL_VERSION` in both `skyportd` and `panel`.
- **ALWAYS** keep `env!("CARGO_PKG_VERSION")` solely for CLI `--help`, startup banner, logs, and informational `version` reporting to the panel.
