# skyportd

`skyportd` is the node daemon of [Skyport](https://github.com/skyportsh). It runs on every
machine that hosts game or application servers, and it is the only component that talks to
Docker. The Skyport panel decides *what* should run; the daemon makes it happen and reports back.

It is a single static-friendly Rust binary built on [tokio](https://tokio.rs) and
[axum](https://github.com/tokio-rs/axum).

## What it does

- **Enrolls** a node with the panel and keeps a heartbeat going.
- **Installs servers**: runs the catalog install script inside an installer container, with the
  server volume mounted at `/mnt/server`.
- **Runs servers**: one Docker container per server, with memory and CPU limits, a port
  allocation, a volume mounted at `/home/container`, and the Pterodactyl-style environment
  (`STARTUP`, `SERVER_MEMORY`, `SERVER_PORT`, ...). It is designed to run the images from the
  Skyport [depot](https://github.com/skyportsh/depot).
- **Controls power**: start, stop (graceful, using the cargo's stop command), restart, kill and
  reinstall.
- **Streams the console** and live resource stats to the browser over a WebSocket, and accepts
  console commands.
- **Manages files** through an HTTP API (list, read, write, upload, copy, move, archive,
  extract) and through an embedded **SFTP** server. Paths are confined to the server volume.
- **Backups**: creates, restores and serves `tar.gz` backups, with disk quota checks.
- **Workflows**: runs per-server automation defined in the panel.
- **Interconnects**: attaches servers to private Docker networks.

## Architecture

```
                    ┌────────────────────────────── skyportd ──────────────────────────────┐
  Panel  ──HTTPS──► │  api (axum)            ┐                                              │
  Browser ──WSS───► │   REST + WebSocket     │      ┌───────────────────────┐               │
  SFTP client ────► │  sftp (russh)          ├────► │   ServerRegistry      │               │
                    │  heartbeat             │      │  SQLite: server state │               │
                    │  workflow engine       │      │  memory: console,     │               │
                    │  server lifecycle  ◄───┘      │          status feed  │               │
                    │       │                       └───────────────────────┘               │
                    └───────┼───────────────────────────────────────────────────────────────┘
                            ▼
                      docker CLI  ──►  one container per server (+ install containers)
```

The daemon is a set of cooperating tokio services started from `app.rs`, all stopped together
through a cancellation token:

| Service | Responsibility |
| --- | --- |
| `api` | HTTP/HTTPS API for the panel and the browser WebSocket. TLS uses rustls. |
| `sftp` | SFTP access to server volumes. |
| `heartbeat` | Talks to the panel and reconciles the local server list with it. |
| `server_lifecycle` | The reconciliation loop that drives containers toward the desired state. |
| `workflow` | Executes server workflows. |

### The registry

`ServerRegistry` (`server_registry.rs`) is shared by every service:

- **SQLite (WAL)** stores the server records and their runtime state (status, container id,
  last error). Every connection waits for competing writers (`busy_timeout`) and uses
  `synchronous=NORMAL`.
- **In memory** it keeps each server's console (see below) and a status feed. Nothing that is
  written per log line touches the database.

### The lifecycle loop

Every 3 seconds `server_lifecycle` compares the panel's desired state with Docker:

1. It lists the server statuses of the node and runs **one** `docker ps` for the whole node,
   instead of one `docker inspect` per server. If Docker does not answer, the tick is skipped:
   an unreachable daemon is never interpreted as "the servers are down".
2. A healthy running server that is already being followed needs nothing.
3. Anything else is reconciled in its own task: install (`pending`), boot, resume startup
   monitoring, or recover a crashed container.
4. Preparing a server (permissions, quota scan, image pull, container creation) is limited to a
   few servers at a time, so a node reboot cannot saturate the disk and the network.

**Restarting the daemon does not restart your servers.** On start, the daemon *adopts*
containers that are already running: it re-attaches a log follower and leaves the container
alone. Console input keeps working because commands are written with `docker exec` into the
container's own stdin, which does not depend on the daemon process.

### The console and the WebSocket

- Console output lives in a **bounded in-memory ring buffer per server**: at most 1000 lines and
  2 MiB, with lines capped at 8 KiB. Docker already keeps the full history; the daemon only
  needs the recent part, and it reloads the last 200 lines when it adopts a container.
- Ingestion is **rate limited** (5000 lines/s sustained, 10000 burst). A server that floods its
  console cannot burn the daemon's CPU, and readers get a single notice saying how many lines
  were dropped.
- Live lines are **pushed** to WebSocket clients through a broadcast channel. A client that
  falls too far behind catches up from the buffer and is told how many lines it skipped, so a
  hole in the console is never silent.
- Status changes are pushed the same way (a `watch` channel per server), so there is no polling.
- On connect a client starts at the live edge and asks for history explicitly (`send logs`).

### Stats

Each server being watched gets **one persistent `docker stats` process**, shared by every
connected client. It stops by itself when nobody has asked for numbers for 20 seconds. Disk
usage is computed once, then served from a cache and refreshed in the background every
3 minutes, so a large volume is never scanned on a request path.

### Docker access

The daemon drives Docker through the `docker` command line client. All short commands have a
timeout and are killed when it elapses, and a Docker problem that looks transient (timeout,
unreachable daemon) never changes a server's state.

### Files and uploads

Uploads are streamed to a hidden temporary file and renamed when complete, so memory stays flat
whatever the size and a failed transfer never leaves a truncated file under its real name.
Downloads (backups, install logs) are streamed too.

## What changed in the scalability pass

This version reworked the parts of the daemon that did not scale:

- Console storage moved from one SQLite row per line (unbounded growth, a synchronous write and
  a new connection per line) to a bounded in-memory buffer.
- WebSocket polling (100 ms console, 500 ms status, a stats process per client) became
  push-based, which also fixed a bug where each new connection replayed the whole history.
- A daemon restart no longer recreates every running container.
- One `docker ps` per tick replaced one `docker inspect` per server.
- Shared stats and disk caches replaced per-client sampling and per-tick directory walks.
- Docker commands have timeouts, and startups are rate limited.
- Uploads and log downloads stream instead of being loaded into memory (an upload used to be
  held twice in RAM).
- A shared HTTP client replaced one client per request.

## Known limits

Be honest about what is and is not proven:

- It has been tested on macOS with Docker Desktop and the depot images (Alpine, Debian, Node,
  Python, Go, Java, and a real Paper server installed and booted end to end). It has **not**
  been load tested on a large Linux node, and Linux file-ownership behaviour has not been
  exercised.
- The daemon still starts one `docker logs -f` and, while watched, one `docker stats` process
  per server. Talking to the Docker API directly (for example with `bollard`) would remove
  those processes and is the next big step for very dense nodes.
- A node is one machine. Running many nodes also depends on the panel.

## Installation & updates

Use the official [skyportsh/installer](https://github.com/skyportsh/installer) repository to install
or update `skyportd` on Linux (Ubuntu 24.04 / Debian 13):

**Install:**
```bash
bash <(curl -fsSL https://raw.githubusercontent.com/skyportsh/installer/main/install-daemon.sh)
```

**Update:**
```bash
bash <(curl -fsSL https://raw.githubusercontent.com/skyportsh/installer/main/update-daemon.sh)
```

## Building and running

Requirements: Rust (see `rust-toolchain.toml`) and Docker.

```bash
cargo build --release
./target/release/skyportd --help
```

```text
Usage: skyportd [OPTIONS]

Options:
      --configure  Re-run the configuration prompt
  -d, --debug      Enable verbose debug logging
  -h, --help       Show this help message
```

On first start the daemon asks for the panel URL and a configuration token, enrolls the node,
and stores the result in `config/local.toml`. Defaults live in `config/default.toml`:

```toml
[daemon]
name = "skyportd"
tick_interval = "5s"
shutdown_timeout = "30s"
allow_private_webhooks = false

[panel]
url = "http://127.0.0.1:8000"

[logging]
level = "info"      # or "debug"; format is "pretty" or "json"

[runtime]
worker_threads = 0  # 0 = number of CPUs
```

The node section (API port, SFTP port, TLS certificate and key) is provided by the panel during
enrollment. The SFTP server defaults to port 2022.

Server volumes are stored under `volumes/<server id>` and the registry in `skyportd.db`, both
relative to the daemon's root directory.

## Tests

```bash
cargo test                       # unit and integration tests, no Docker needed
cargo test -- --ignored          # tests that use a real Docker daemon
```

The ignored tests cover adoption after a daemon restart, boot and crash recovery, the shared
stats collector, and the depot images (`depot_*`). The depot tests need the images built
locally first:

```bash
cd ../depot
tests/run.sh --keep os-alpine os-debian nodejs-22 python-3.12 go-1.24 java-21 java-21-jre \
    java-25-jre installer-debian
cd ../skyportd
cargo test depot_ -- --ignored
```

The Paper test also needs internet access.

## License

See the repository license.
