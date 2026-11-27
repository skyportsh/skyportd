# Repo Boundaries

Strict separation between the `skyportd` daemon codebase and the `skyportsh/installer` repository.

## When this applies

- Adding or editing files under `scripts/` or root shell scripts.
- Editing installation, update, or deployment sections in `README.md`.
- Adding systemd units, firewall helpers, or daemon lifecycle wrappers.

## Rules

- **CRITICAL**: NEVER add installation, update, backup, uninstall, or server management shell scripts (e.g., `scripts/skyport.sh`) to `skyportd`.
- **CRITICAL**: All production installation, update, and lifecycle scripts live exclusively in `https://github.com/skyportsh/installer` (`install-daemon.sh`, `update-daemon.sh`).
- **NEVER** hardcode alternative install paths (e.g., `/var/lib/skyportd`) that conflict with the official `installer` layout (`/etc/skyportd`).
- **ALWAYS** route installation or update improvements to `skyportsh/installer` instead of creating parallel scripts in `skyportd`.
- **ALWAYS** link to `https://github.com/skyportsh/installer` in `README.md` for Linux installation and updates.
