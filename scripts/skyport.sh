#!/usr/bin/env bash
#
# skyport - install, configure and manage the Skyport node daemon (skyportd).
#
# First run:    sudo bash skyport.sh install
# Afterwards:   sudo skyport <status|start|stop|restart|update|...>

set -Eeuo pipefail

REPO="skyportsh/skyportd"
SERVICE="skyportd"
DEFAULT_INSTALL_DIR="/var/lib/skyportd"
SETTINGS_FILE="/etc/skyportd.conf"
UNIT_FILE="/etc/systemd/system/skyportd.service"
CLI_PATH="/usr/local/bin/skyport"
LETSENCRYPT_DIR="/etc/letsencrypt/live"
ENROLL_TIMEOUT=60

INSTALL_DIR="$DEFAULT_INSTALL_DIR"
TMP_DIR=""

if [[ -t 1 ]]; then
    ORANGE=$'\033[38;2;240;90;36m'
    GREEN=$'\033[32m'
    RED=$'\033[31m'
    GRAY=$'\033[38;2;140;140;140m'
    BOLD=$'\033[1m'
    RESET=$'\033[0m'
else
    ORANGE="" GREEN="" RED="" GRAY="" BOLD="" RESET=""
fi

# ---------------------------------------------------------------------------
# Output and prompts
# ---------------------------------------------------------------------------

step() { printf '\n%s==>%s %s%s%s\n' "$ORANGE" "$RESET" "$BOLD" "$*" "$RESET"; }
info() { printf '%s•%s %s\n' "$GRAY" "$RESET" "$*"; }
ok() { printf '%s✓%s %s\n' "$GREEN" "$RESET" "$*"; }
warn() { printf '%s!%s %s\n' "$ORANGE" "$RESET" "$*" >&2; }
die() {
    printf '%s✗%s %s\n' "$RED" "$RESET" "$*" >&2
    exit 1
}

cleanup() {
    if [[ -n "$TMP_DIR" && -d "$TMP_DIR" ]]; then
        rm -rf -- "$TMP_DIR"
    fi
}
trap cleanup EXIT

read_input() {
    # $1: variable name, remaining arguments: extra `read` options.
    local __target="$1"
    shift
    IFS= read -r "$@" "$__target" || die "Input closed. Run this command from an interactive terminal."
}

# ask VARIABLE "Question" ["default"]
ask() {
    local __var="$1" question="$2" default="${3:-}" __reply=""

    if [[ -n "$default" ]]; then
        printf '%s›%s %s %s[%s]%s: ' "$ORANGE" "$RESET" "$question" "$GRAY" "$default" "$RESET"
    else
        printf '%s›%s %s: ' "$ORANGE" "$RESET" "$question"
    fi

    read_input __reply
    printf -v "$__var" '%s' "${__reply:-$default}"
}

ask_secret() {
    local __var="$1" question="$2" __reply=""

    printf '%s›%s %s %s(hidden)%s: ' "$ORANGE" "$RESET" "$question" "$GRAY" "$RESET"
    read_input __reply -s
    printf '\n'
    printf -v "$__var" '%s' "$__reply"
}

# confirm "Question" [y|n]
confirm() {
    local question="$1" default="${2:-y}" reply="" hint="[Y/n]"
    [[ "$default" == "n" ]] && hint="[y/N]"

    while true; do
        printf '%s›%s %s %s%s%s: ' "$ORANGE" "$RESET" "$question" "$GRAY" "$hint" "$RESET"
        read_input reply
        case "${reply:-$default}" in
            [yY] | [yY][eE][sS]) return 0 ;;
            [nN] | [nN][oO]) return 1 ;;
            *) warn "Please answer y or n." ;;
        esac
    done
}

# choose VARIABLE "Question" default_number "label" "label" ...
choose() {
    local __var="$1" question="$2" default="$3" reply="" index=1
    shift 3

    printf '\n%s\n' "$question"
    local label
    for label in "$@"; do
        printf '  %s%d)%s %s\n' "$ORANGE" "$index" "$RESET" "$label"
        index=$((index + 1))
    done

    while true; do
        ask reply "Choice" "$default"
        if [[ "$reply" =~ ^[0-9]+$ ]] && ((reply >= 1 && reply <= $#)); then
            printf -v "$__var" '%s' "$reply"
            return 0
        fi
        warn "Enter a number between 1 and $#."
    done
}

# ---------------------------------------------------------------------------
# Environment helpers
# ---------------------------------------------------------------------------

has() { command -v "$1" >/dev/null 2>&1; }

require_root() {
    [[ "$(id -u)" -eq 0 ]] || die "This command needs root. Try: sudo skyport ${1:-}"
}

require_linux() {
    [[ "$(uname -s)" == "Linux" ]] || die "skyportd can only be installed on Linux with this script."
    has systemctl && [[ -d /run/systemd/system ]] || die "systemd is required."
}

load_settings() {
    if [[ -f "$SETTINGS_FILE" ]]; then
        local value
        value="$(sed -n 's/^INSTALL_DIR=//p' "$SETTINGS_FILE" | head -n 1)"
        if [[ -n "$value" ]]; then
            INSTALL_DIR="$value"
        fi
    fi
}

require_installed() {
    load_settings
    [[ -x "$INSTALL_DIR/skyportd" && -f "$UNIT_FILE" ]] \
        || die "skyportd is not installed. Run: sudo bash skyport.sh install"
}

valid_install_dir() {
    local dir="$1"
    [[ "$dir" =~ ^/[A-Za-z0-9._/-]+$ ]] || return 1
    [[ "$dir" != *..* ]] || return 1
    case "${dir%/}" in
        "" | /bin | /boot | /dev | /etc | /home | /lib | /proc | /root | /sbin | /sys | /tmp | /usr | /var | /var/lib | /opt | /srv)
            return 1
            ;;
    esac
    return 0
}

detect_arch() {
    case "$(uname -m)" in
        x86_64 | amd64) echo "x86_64" ;;
        aarch64 | arm64) echo "aarch64" ;;
        riscv64) echo "riscv64" ;;
        *) die "Unsupported CPU architecture: $(uname -m)" ;;
    esac
}

install_packages() {
    if has apt-get; then
        apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y "$@"
    elif has dnf; then
        dnf install -y "$@"
    elif has yum; then
        yum install -y "$@"
    else
        die "No supported package manager found. Install manually: $*"
    fi
}

download() {
    # download URL DESTINATION
    curl --fail --silent --show-error --location --proto '=https' --retry 3 --output "$2" "$1"
}

make_tmp_dir() {
    if [[ -z "$TMP_DIR" ]]; then
        TMP_DIR="$(mktemp -d)"
    fi
}

ensure_dependencies() {
    step "Checking dependencies"

    if ! has curl; then
        confirm "curl is missing. Install it now?" || die "curl is required."
        install_packages curl ca-certificates
    fi
    ok "curl"

    if ! has docker; then
        warn "Docker is not installed."
        confirm "Install Docker with the official script from https://get.docker.com?" \
            || die "Docker is required. Install it, then run this script again."
        make_tmp_dir
        download "https://get.docker.com" "$TMP_DIR/get-docker.sh"
        sh "$TMP_DIR/get-docker.sh"
    fi

    if ! docker info >/dev/null 2>&1; then
        systemctl enable --now docker || true
        docker info >/dev/null 2>&1 || die "Docker is installed but not responding. Check: systemctl status docker"
    fi
    ok "Docker $(docker version --format '{{.Server.Version}}' 2>/dev/null || echo '')"
}

# ---------------------------------------------------------------------------
# TOML editing (only what config/local.toml needs: scalar keys in a section)
# ---------------------------------------------------------------------------

toml_quote() {
    local value="$1"
    value="${value//\\/\\\\}"
    value="${value//\"/\\\"}"
    printf '"%s"' "$value"
}

# toml_get FILE SECTION KEY
toml_get() {
    [[ -f "$1" ]] || return 0
    awk -v section="[$2]" -v key="$3" '
        /^[[:space:]]*\[/ { current = $0; gsub(/[[:space:]]/, "", current); next }
        current == section {
            line = $0
            name = line
            sub(/[[:space:]]*=.*/, "", name)
            gsub(/[[:space:]]/, "", name)
            if (name == key) {
                sub(/^[^=]*=[[:space:]]*/, "", line)
                if (line ~ /^".*"$/) { line = substr(line, 2, length(line) - 2) }
                if (line ~ /^\047.*\047$/) { line = substr(line, 2, length(line) - 2) }
                print line
                exit
            }
        }
    ' "$1"
}

# toml_edit FILE SECTION KEY [RAW_VALUE]; without a value the key is removed.
toml_edit() {
    local file="$1" section="$2" key="$3" action="remove" value="" tmp
    if [[ $# -ge 4 ]]; then
        action="set"
        value="$4"
    fi

    [[ -f "$file" ]] || (umask 077 && : >"$file")
    tmp="$(mktemp "${file}.XXXXXX")"

    # The value goes through the environment so awk does not interpret backslashes.
    TOML_VALUE="$value" awk -v section="[$section]" -v key="$key" -v action="$action" '
        function emit() {
            if (action == "set" && !done) { print key " = " ENVIRON["TOML_VALUE"]; done = 1 }
        }
        /^[[:space:]]*\[/ {
            if (current == section) { emit() }
            current = $0; gsub(/[[:space:]]/, "", current)
            print; next
        }
        current == section {
            name = $0
            sub(/[[:space:]]*=.*/, "", name)
            gsub(/[[:space:]]/, "", name)
            if (name == key) { emit(); next }
        }
        { print }
        END {
            if (action == "set" && !done) {
                if (current != section) { print ""; print section }
                emit()
            }
        }
    ' "$file" >"$tmp"

    cat "$tmp" >"$file"
    rm -f -- "$tmp"
}

local_config() { echo "$INSTALL_DIR/config/local.toml"; }

# ---------------------------------------------------------------------------
# Releases
# ---------------------------------------------------------------------------

latest_version() {
    local url
    url="$(curl --fail --silent --show-error --location --proto '=https' --output /dev/null \
        --write-out '%{url_effective}' "https://github.com/$REPO/releases/latest")" || return 1
    [[ "$url" == */releases/tag/* ]] || return 1
    echo "${url##*/}"
}

normalize_version() {
    local version="$1"
    [[ "$version" == v* ]] || version="v$version"
    [[ "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([-+][A-Za-z0-9.-]+)?$ ]] || return 1
    echo "$version"
}

installed_version() {
    [[ -x "$INSTALL_DIR/skyportd" ]] || return 0
    local line
    line="$(cd "$INSTALL_DIR" && ./skyportd --help 2>/dev/null | head -n 1)" || return 0
    [[ "$line" == "skyportd "* ]] && echo "v${line#skyportd }"
    return 0
}

# release_digest VERSION ASSET -> sha256 published by GitHub, empty if unknown
release_digest() {
    local json
    json="$(curl --fail --silent --location --proto '=https' \
        "https://api.github.com/repos/$REPO/releases/tags/$1" 2>/dev/null)" || return 0
    printf '%s' "$json" | tr ',{}' '\n\n\n' | awk -v asset="$2" '
        /"name"[[:space:]]*:/ { found = index($0, "\"" asset "\"") > 0 }
        found && /"digest"[[:space:]]*:[[:space:]]*"sha256:/ {
            sub(/.*sha256:/, ""); sub(/".*/, ""); print; exit
        }
    '
}

build_from_source() {
    # build_from_source VERSION DESTINATION
    local version="$1" destination="$2"

    step "Building skyportd $version from source (this takes several minutes)"

    if ! has git || ! has cc; then
        confirm "A compiler and git are required. Install them now?" || die "Cannot build without them."
        if has apt-get; then
            install_packages git build-essential pkg-config
        else
            install_packages git gcc gcc-c++ make pkgconf-pkg-config
        fi
    fi

    # sudo may keep the caller's HOME; the toolchain belongs to root.
    local HOME=/root
    export HOME
    if ! has cargo && [[ -x "$HOME/.cargo/bin/cargo" ]]; then
        PATH="$HOME/.cargo/bin:$PATH"
    fi
    if ! has rustup; then
        confirm "Install the Rust toolchain with the official script from https://sh.rustup.rs?" \
            || die "Rust (rustup) is required to build skyportd."
        make_tmp_dir
        download "https://sh.rustup.rs" "$TMP_DIR/rustup.sh"
        sh "$TMP_DIR/rustup.sh" -y --profile minimal --default-toolchain none
        PATH="$HOME/.cargo/bin:$PATH"
    fi

    make_tmp_dir
    rm -rf -- "$TMP_DIR/source"
    git clone --quiet --depth 1 --branch "$version" "https://github.com/$REPO.git" "$TMP_DIR/source"
    # rust-toolchain.toml in the repository selects the compiler version.
    (cd "$TMP_DIR/source" && cargo build --release)
    install -m 0755 "$TMP_DIR/source/target/release/skyportd" "$destination"
}

# fetch_release VERSION: leaves a tested binary and default.toml in $TMP_DIR
fetch_release() {
    local version="$1" asset expected actual output
    asset="skyportd-linux-$(detect_arch)"

    make_tmp_dir
    step "Downloading skyportd $version"

    download "https://raw.githubusercontent.com/$REPO/$version/config/default.toml" "$TMP_DIR/default.toml" \
        || die "Could not download the configuration for $version. Does this version exist?"
    download "https://github.com/$REPO/releases/download/$version/$asset" "$TMP_DIR/skyportd" \
        || die "Could not download $asset for $version."

    expected="$(release_digest "$version" "$asset")"
    if [[ -n "$expected" ]]; then
        actual="$(sha256sum "$TMP_DIR/skyportd" | awk '{ print $1 }')"
        [[ "$actual" == "$expected" ]] || die "Checksum mismatch for $asset. Download aborted."
        ok "Checksum verified"
    else
        warn "Could not get the published checksum from GitHub."
        confirm "Continue without verifying the download?" n || die "Aborted."
    fi

    chmod 0755 "$TMP_DIR/skyportd"

    if output="$("$TMP_DIR/skyportd" --help 2>&1)"; then
        ok "The binary runs on this machine"
    else
        warn "The prebuilt binary cannot run on this system:"
        printf '%s\n' "$output" >&2
        info "This usually means the system libraries (glibc) are too old."
        confirm "Build skyportd from source instead?" || die "Aborted."
        build_from_source "$version" "$TMP_DIR/skyportd"
        "$TMP_DIR/skyportd" --help >/dev/null || die "The built binary does not run."
    fi
}

# ---------------------------------------------------------------------------
# Configuration questions
# ---------------------------------------------------------------------------

ask_panel() {
    local current_url url token
    current_url="$(toml_get "$(local_config)" panel url)"

    step "Panel connection"
    info "Create the node in your panel first, then copy its one-time configuration token."

    while true; do
        ask url "Panel URL (https://panel.example.com)" "$current_url"
        while [[ "$url" == */ ]]; do url="${url%/}"; done
        [[ "$url" =~ ^https?://[^[:space:]/]+ ]] && break
        warn "Enter a full http:// or https:// URL."
    done

    while true; do
        ask_secret token "Configuration token"
        token="$(printf '%s' "$token" | tr -d '[:space:]')"
        [[ -n "$token" ]] && break
        warn "A configuration token is required."
    done

    local config
    config="$(local_config)"
    toml_edit "$config" panel url "$(toml_quote "$url")"
    toml_edit "$config" panel configuration_token "$(toml_quote "$token")"
    # Removing the previous credentials makes the daemon enroll again.
    toml_edit "$config" panel daemon_secret
    toml_edit "$config" panel daemon_callback_token
    toml_edit "$config" panel node_id
}

# link_certificate FQDN CERT KEY: expose custom files where skyportd looks for them
link_certificate() {
    local fqdn="$1" cert="$2" key="$3" live="$LETSENCRYPT_DIR/$1" config
    config="$(local_config)"

    # An already enrolled node reads the paths from its own configuration.
    if [[ -n "$(toml_get "$config" node fqdn)" ]]; then
        toml_edit "$config" node tls_cert_path "$(toml_quote "$cert")"
        toml_edit "$config" node tls_key_path "$(toml_quote "$key")"
        ok "Certificate paths saved"
    fi

    # During enrollment the daemon only looks in the Let's Encrypt directory.
    if [[ "$cert" == "$live/fullchain.pem" && "$key" == "$live/privkey.pem" ]]; then
        return 0
    fi

    if [[ -e "$live/fullchain.pem" || -e "$live/privkey.pem" ]]; then
        if [[ "$(readlink "$live/fullchain.pem")" != "$cert" || "$(readlink "$live/privkey.pem")" != "$key" ]]; then
            warn "$live already contains a certificate; it will not be overwritten."
            info "A new enrollment would use that one instead of your files."
            confirm "Continue?" || die "Aborted."
        fi
        return 0
    fi

    mkdir -p "$live"
    ln -s "$cert" "$live/fullchain.pem"
    ln -s "$key" "$live/privkey.pem"
    ok "Linked your certificate into $live (where skyportd expects it)"
    info "If you later use certbot for $fqdn, delete $live first."
}

ask_tls() {
    local mode fqdn cert key email

    step "HTTPS for the daemon API"
    info "This must match the SSL setting of the node in your panel."

    choose mode "How is SSL set up for this node?" 1 \
        "No SSL (the node uses plain http in the panel)" \
        "Get a new Let's Encrypt certificate with certbot" \
        "I already have a Let's Encrypt certificate" \
        "I have my own certificate and key files"

    [[ "$mode" == 1 ]] && return 0

    while true; do
        ask fqdn "Node domain name, exactly as set in the panel (node.example.com)" \
            "$(toml_get "$(local_config)" node fqdn)"
        [[ "$fqdn" =~ ^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$ && "$fqdn" != *..* ]] && break
        warn "Enter a valid domain name, without http:// and without a port."
    done

    case "$mode" in
        2)
            if ! has certbot; then
                confirm "certbot is missing. Install it now?" || die "certbot is required."
                install_packages certbot
            fi
            ask email "Email address for Let's Encrypt expiry notices"
            confirm "Accept the Let's Encrypt terms of service and request the certificate? (port 80 must be free and reachable)" n \
                || die "Aborted."
            certbot certonly --standalone --non-interactive --agree-tos -m "$email" -d "$fqdn" \
                || die "certbot failed. Check the DNS record of $fqdn and that port 80 is open."
            link_certificate "$fqdn" "$LETSENCRYPT_DIR/$fqdn/fullchain.pem" "$LETSENCRYPT_DIR/$fqdn/privkey.pem"
            info "Certificates are renewed by certbot; restart skyportd after a renewal."
            if [[ -d /etc/letsencrypt/renewal-hooks/deploy ]] \
                && confirm "Restart skyportd automatically after each renewal?"; then
                printf '#!/bin/sh\nsystemctl try-restart %s\n' "$SERVICE" \
                    >/etc/letsencrypt/renewal-hooks/deploy/skyportd.sh
                chmod 0755 /etc/letsencrypt/renewal-hooks/deploy/skyportd.sh
            fi
            ;;
        3)
            cert="$LETSENCRYPT_DIR/$fqdn/fullchain.pem"
            key="$LETSENCRYPT_DIR/$fqdn/privkey.pem"
            [[ -f "$cert" && -f "$key" ]] || die "No certificate found in $LETSENCRYPT_DIR/$fqdn."
            link_certificate "$fqdn" "$cert" "$key"
            ;;
        4)
            while true; do
                ask cert "Certificate file (full chain), absolute path"
                [[ "$cert" == /* && -f "$cert" ]] && break
                warn "That file does not exist."
            done
            while true; do
                ask key "Private key file, absolute path"
                [[ "$key" == /* && -f "$key" ]] && break
                warn "That file does not exist."
            done
            link_certificate "$fqdn" "$cert" "$key"
            ;;
    esac
}

ask_options() {
    local config level format workers webhooks default current choice
    config="$(local_config)"

    step "Daemon options"

    current="$(toml_get "$config" logging level)"
    default=1
    [[ "$current" == "debug" ]] && default=2
    [[ "$current" == "warn" ]] && default=3
    choose choice "Log level" "$default" "info (recommended)" "debug (verbose)" "warn (quiet)"
    case "$choice" in
        1) level="info" ;;
        2) level="debug" ;;
        3) level="warn" ;;
    esac

    default=1
    [[ "$(toml_get "$config" logging format)" == "json" ]] && default=2
    choose choice "Log format" "$default" "pretty (human readable)" "json (for log collectors)"
    format="pretty"
    [[ "$choice" == 2 ]] && format="json"

    current="$(toml_get "$config" runtime worker_threads)"
    while true; do
        ask workers "Worker threads (0 = one per CPU)" "${current:-0}"
        [[ "$workers" =~ ^[0-9]{1,4}$ ]] && break
        warn "Enter a number."
    done

    default=n
    [[ "$(toml_get "$config" daemon allow_private_webhooks)" == "true" ]] && default=y
    webhooks=false
    if confirm "Allow workflow webhooks to reach private network addresses? (less secure)" "$default"; then
        webhooks=true
    fi

    toml_edit "$config" logging level "$(toml_quote "$level")"
    toml_edit "$config" logging format "$(toml_quote "$format")"
    toml_edit "$config" runtime worker_threads "$((10#$workers))"
    toml_edit "$config" daemon allow_private_webhooks "$webhooks"
}

open_firewall_ports() {
    local config api_port sftp_port
    config="$(local_config)"
    api_port="$(toml_get "$config" node daemon_port)"
    sftp_port="$(toml_get "$config" node sftp_port)"
    [[ -n "$api_port" && -n "$sftp_port" ]] || return 0

    step "Network"
    info "API port: $api_port, SFTP port: $sftp_port (both are set in the panel)."

    if has ufw && ufw status 2>/dev/null | grep -q '^Status: active'; then
        if confirm "Open these two ports in ufw?"; then
            ufw allow "$api_port/tcp"
            ufw allow "$sftp_port/tcp"
        fi
    elif has firewall-cmd && firewall-cmd --state >/dev/null 2>&1; then
        if confirm "Open these two ports in firewalld?"; then
            firewall-cmd --permanent --add-port="$api_port/tcp"
            firewall-cmd --permanent --add-port="$sftp_port/tcp"
            firewall-cmd --reload
        fi
    else
        info "No active ufw or firewalld found. Make sure your provider's firewall allows them."
    fi
    info "Remember to also open the ports allocated to your game servers."
}

# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------

write_unit() {
    cat >"$UNIT_FILE" <<UNIT
[Unit]
Description=Skyport node daemon (skyportd)
Documentation=https://github.com/$REPO
After=network-online.target docker.service
Wants=network-online.target
Requires=docker.service

[Service]
Type=simple
User=root
WorkingDirectory=$INSTALL_DIR
ExecStart=$INSTALL_DIR/skyportd
Restart=on-failure
RestartSec=5
TimeoutStopSec=60
LimitNOFILE=65536
UMask=0022

[Install]
WantedBy=multi-user.target
UNIT
    chmod 0644 "$UNIT_FILE"
    systemctl daemon-reload
}

show_recent_logs() {
    journalctl -u "$SERVICE" -n "${1:-25}" --no-pager 2>/dev/null || true
}

# The service must stay up for a few seconds without being restarted.
service_is_stable() {
    local before after
    before="$(systemctl show -p NRestarts --value "$SERVICE" 2>/dev/null || echo 0)"
    sleep 5
    systemctl is-active --quiet "$SERVICE" || return 1
    after="$(systemctl show -p NRestarts --value "$SERVICE" 2>/dev/null || echo 0)"
    [[ "$before" == "$after" ]]
}

wait_for_enrollment() {
    local config elapsed=0
    config="$(local_config)"

    info "Waiting for the panel to accept this node..."
    while ((elapsed < ENROLL_TIMEOUT)); do
        if [[ -n "$(toml_get "$config" panel daemon_secret)" ]]; then
            return 0
        fi
        case "$(systemctl is-active "$SERVICE" 2>/dev/null || true)" in
            active) ;;
            *) return 1 ;;
        esac
        sleep 2
        elapsed=$((elapsed + 2))
    done
    return 1
}

enroll() {
    # Starts the service and asks for new credentials until the panel accepts them.
    while true; do
        step "Starting skyportd"
        systemctl reset-failed "$SERVICE" 2>/dev/null || true
        systemctl restart "$SERVICE" || true

        if wait_for_enrollment; then
            ok "Node enrolled with the panel"
            break
        fi

        systemctl stop "$SERVICE" || true
        warn "Enrollment failed. Last log lines:"
        show_recent_logs 15
        info "The panel and daemon versions must be identical."
        confirm "Enter the panel URL and a configuration token again?" \
            || die "Not enrolled. Run 'sudo skyport configure' when ready."
        ask_panel
    done

    # One restart is allowed: it reloads what the enrollment has just saved.
    if ! service_is_stable && ! { systemctl restart "$SERVICE" && service_is_stable; }; then
        warn "skyportd stopped after enrollment. Last log lines:"
        show_recent_logs 25
        die "Fix the problem above, then run: sudo skyport restart"
    fi

    # The node settings are only saved once the certificate (if any) is found.
    if [[ -z "$(toml_get "$(local_config)" node fqdn)" ]]; then
        warn "The node settings were not saved. Last log lines:"
        show_recent_logs 25
        die "If the node uses SSL, run 'sudo skyport configure' and choose the SSL certificate option."
    fi
    ok "skyportd is running"
}

install_cli() {
    local source="${BASH_SOURCE[0]}"
    if [[ ! -f "$source" ]]; then
        warn "Could not install the 'skyport' command. Download the script to a file and run it from there."
        return 0
    fi
    if [[ ! "$source" -ef "$CLI_PATH" ]]; then
        install -m 0755 "$source" "$CLI_PATH"
    fi
    ok "Command installed: $CLI_PATH"
}

# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

cmd_install() {
    require_root install
    require_linux
    load_settings

    printf '%s\n' "${BOLD}${ORANGE}Skyport daemon installer${RESET}"

    if [[ -x "$INSTALL_DIR/skyportd" && -f "$UNIT_FILE" ]]; then
        warn "skyportd is already installed in $INSTALL_DIR."
        confirm "Reinstall it? Your configuration and server files are kept." n || exit 0
    fi

    ensure_dependencies

    step "Installation"
    while true; do
        ask INSTALL_DIR "Install directory (server files and backups live here too)" "$INSTALL_DIR"
        INSTALL_DIR="${INSTALL_DIR%/}"
        valid_install_dir "$INSTALL_DIR" && break
        warn "Use a dedicated absolute path without spaces, such as $DEFAULT_INSTALL_DIR."
        INSTALL_DIR="$DEFAULT_INSTALL_DIR"
    done

    local latest version
    latest="$(latest_version)" || die "Could not find the latest release on GitHub."
    info "The daemon version must be the same as your panel version."
    while true; do
        ask version "Version to install" "$latest"
        version="$(normalize_version "$version")" && break
        warn "Enter a version such as $latest."
    done

    fetch_release "$version"

    if systemctl is-active --quiet "$SERVICE"; then
        systemctl stop "$SERVICE"
    fi

    mkdir -p "$INSTALL_DIR/config" "$INSTALL_DIR/volumes" "$INSTALL_DIR/backups"
    install -m 0755 "$TMP_DIR/skyportd" "$INSTALL_DIR/skyportd"
    install -m 0644 "$TMP_DIR/default.toml" "$INSTALL_DIR/config/default.toml"
    [[ -f "$(local_config)" ]] || (umask 077 && : >"$(local_config)")
    chmod 0600 "$(local_config)"
    printf 'INSTALL_DIR=%s\n' "$INSTALL_DIR" >"$SETTINGS_FILE"
    chmod 0644 "$SETTINGS_FILE"
    ok "Files installed in $INSTALL_DIR"

    write_unit
    install_cli

    local needs_enrollment=true
    if [[ -n "$(toml_get "$(local_config)" panel daemon_secret)" ]]; then
        if confirm "This node is already enrolled. Keep the current panel connection?"; then
            needs_enrollment=false
        fi
    fi

    if [[ "$needs_enrollment" == true ]]; then
        ask_panel
        ask_tls
    fi
    ask_options

    if confirm "Start skyportd automatically when the machine boots?"; then
        systemctl enable "$SERVICE"
    else
        systemctl disable "$SERVICE" 2>/dev/null || true
    fi

    if [[ "$needs_enrollment" == true ]]; then
        enroll
    else
        systemctl restart "$SERVICE"
        service_is_stable || {
            show_recent_logs
            die "skyportd failed to start."
        }
        ok "skyportd is running"
    fi

    open_firewall_ports

    step "Done"
    info "sudo skyport status     show the daemon state"
    info "sudo skyport logs       follow the logs"
    info "sudo skyport help       list every command"
}

cmd_configure() {
    require_root configure
    require_linux
    require_installed

    local choice backup
    choose choice "What do you want to configure?" 1 \
        "Daemon options (logs, threads, webhooks)" \
        "Panel connection (enroll again with a new token)" \
        "SSL certificate" \
        "Start at boot" \
        "Cancel"

    backup="$(local_config).bak"
    [[ -f "$(local_config)" ]] && cp -p "$(local_config)" "$backup"

    case "$choice" in
        1)
            ask_options
            ;;
        2)
            warn "The current enrollment is replaced. A copy is saved in $backup."
            confirm "Continue?" n || return 0
            ask_panel
            ask_tls
            enroll
            open_firewall_ports
            return 0
            ;;
        3)
            ask_tls
            if [[ "$(toml_get "$(local_config)" node use_ssl)" != "true" ]]; then
                info "Enable or disable SSL for the node in the panel; the daemon follows that setting."
            fi
            ;;
        4)
            if confirm "Start skyportd automatically when the machine boots?"; then
                systemctl enable "$SERVICE"
            else
                systemctl disable "$SERVICE"
            fi
            return 0
            ;;
        *)
            return 0
            ;;
    esac

    if confirm "Restart skyportd now to apply the changes? (game servers keep running)"; then
        cmd_restart
    fi
}

cmd_start() {
    require_root start
    require_linux
    require_installed

    systemctl reset-failed "$SERVICE" 2>/dev/null || true
    systemctl start "$SERVICE" || true
    if service_is_stable; then
        ok "skyportd started"
    else
        show_recent_logs
        die "skyportd failed to start."
    fi
}

cmd_stop() {
    require_root stop
    require_linux
    require_installed

    systemctl stop "$SERVICE"
    ok "skyportd stopped (game server containers keep running)"
}

cmd_restart() {
    require_root restart
    require_linux
    require_installed

    systemctl reset-failed "$SERVICE" 2>/dev/null || true
    systemctl restart "$SERVICE" || true
    if service_is_stable; then
        ok "skyportd restarted"
    else
        show_recent_logs
        die "skyportd failed to restart."
    fi
}

cmd_status() {
    require_linux
    require_installed

    local state boot version config url api_port sftp_port ssl containers
    config="$(local_config)"
    state="$(systemctl is-active "$SERVICE" 2>/dev/null || true)"
    boot="$(systemctl is-enabled "$SERVICE" 2>/dev/null || true)"
    version="$(installed_version)"

    printf '\n%sskyportd%s\n' "$BOLD" "$RESET"
    if [[ "$state" == "active" ]]; then
        printf '  %-16s %s%s%s\n' "State" "$GREEN" "running" "$RESET"
        printf '  %-16s %s\n' "Since" "$(systemctl show -p ActiveEnterTimestamp --value "$SERVICE")"
    else
        printf '  %-16s %s%s%s\n' "State" "$RED" "${state:-unknown}" "$RESET"
    fi
    printf '  %-16s %s\n' "Start at boot" "${boot:-unknown}"
    printf '  %-16s %s\n' "Version" "${version:-unknown}"
    printf '  %-16s %s\n' "Directory" "$INSTALL_DIR"

    if [[ -r "$config" ]]; then
        url="$(toml_get "$config" panel url)"
        api_port="$(toml_get "$config" node daemon_port)"
        sftp_port="$(toml_get "$config" node sftp_port)"
        ssl="$(toml_get "$config" node use_ssl)"
        printf '  %-16s %s\n' "Panel" "${url:-not configured}"
        if [[ -n "$(toml_get "$config" panel daemon_secret)" ]]; then
            printf '  %-16s %s\n' "Enrolled" "yes"
        else
            printf '  %-16s %s\n' "Enrolled" "no"
        fi
        [[ -n "$api_port" ]] && printf '  %-16s %s (SSL: %s)\n' "API port" "$api_port" "${ssl:-false}"
        [[ -n "$sftp_port" ]] && printf '  %-16s %s\n' "SFTP port" "$sftp_port"
    else
        info "Run with sudo to see the panel connection details."
    fi

    if has docker && containers="$(docker ps --filter 'name=^skyport-server-' --format '{{.Names}}' 2>/dev/null)"; then
        printf '  %-16s %s\n' "Running servers" "$(printf '%s' "$containers" | grep -c . || true)"
    else
        printf '  %-16s %s\n' "Running servers" "unknown (cannot reach Docker)"
    fi
    printf '\n'

    [[ "$state" == "active" ]]
}

cmd_logs() {
    require_linux
    require_installed

    if [[ $# -gt 0 ]]; then
        journalctl -u "$SERVICE" "$@"
    else
        journalctl -u "$SERVICE" -n 100 -f
    fi
}

cmd_update() {
    require_root update
    require_linux
    require_installed

    local current target was_active=false
    current="$(installed_version)"

    if [[ -n "${1:-}" ]]; then
        target="$(normalize_version "$1")" || die "Invalid version: $1"
    else
        target="$(latest_version)" || die "Could not find the latest release on GitHub."
    fi

    info "Installed: ${current:-unknown}"
    info "Target:    $target"

    if [[ "$current" == "$target" ]]; then
        ok "Already on $target."
        confirm "Reinstall it anyway?" n || return 0
    fi

    warn "The panel must run the same version ($target), otherwise it cannot manage this node."
    confirm "Continue with the update?" || return 0

    fetch_release "$target"

    systemctl is-active --quiet "$SERVICE" && was_active=true

    cp -p "$INSTALL_DIR/skyportd" "$INSTALL_DIR/skyportd.previous"
    cp -p "$INSTALL_DIR/config/default.toml" "$INSTALL_DIR/config/default.toml.previous"
    # The registry is only copied while the daemon is stopped, so it is consistent.
    systemctl stop "$SERVICE"
    local file
    rm -f -- "$INSTALL_DIR"/skyportd.db*.previous
    for file in "$INSTALL_DIR"/skyportd.db*; do
        [[ -f "$file" && "$file" != *.previous ]] && cp -p "$file" "$file.previous"
    done

    install -m 0755 "$TMP_DIR/skyportd" "$INSTALL_DIR/skyportd"
    install -m 0644 "$TMP_DIR/default.toml" "$INSTALL_DIR/config/default.toml"

    if [[ "$was_active" == false ]]; then
        ok "Updated to $target. The daemon was stopped and stays stopped."
        return 0
    fi

    systemctl reset-failed "$SERVICE" 2>/dev/null || true
    systemctl start "$SERVICE" || true

    if service_is_stable; then
        ok "Updated to $target"
        return 0
    fi

    warn "The new version failed to start:"
    show_recent_logs
    confirm "Roll back to ${current:-the previous version}?" || die "Update kept, but skyportd is not running."

    systemctl stop "$SERVICE" || true
    install -m 0755 "$INSTALL_DIR/skyportd.previous" "$INSTALL_DIR/skyportd"
    install -m 0644 "$INSTALL_DIR/config/default.toml.previous" "$INSTALL_DIR/config/default.toml"
    if [[ -f "$INSTALL_DIR/skyportd.db.previous" ]]; then
        rm -f -- "$INSTALL_DIR/skyportd.db" "$INSTALL_DIR/skyportd.db-wal" "$INSTALL_DIR/skyportd.db-shm"
        for file in "$INSTALL_DIR"/skyportd.db*.previous; do
            cp -p "$file" "${file%.previous}"
        done
    fi
    systemctl reset-failed "$SERVICE" 2>/dev/null || true
    systemctl start "$SERVICE" || true
    service_is_stable || die "Rollback done, but skyportd still does not start."
    die "Update failed. Rolled back to ${current:-the previous version}."
}

cmd_uninstall() {
    require_root uninstall
    require_linux
    load_settings

    [[ -f "$UNIT_FILE" || -f "$SETTINGS_FILE" ]] || die "skyportd is not installed."
    valid_install_dir "$INSTALL_DIR" || die "Refusing to touch the unexpected directory: $INSTALL_DIR"

    confirm "Uninstall skyportd?" n || return 0

    systemctl disable --now "$SERVICE" 2>/dev/null || true
    rm -f -- "$UNIT_FILE" /etc/letsencrypt/renewal-hooks/deploy/skyportd.sh
    systemctl daemon-reload
    rm -f -- "$INSTALL_DIR/skyportd" "$INSTALL_DIR/skyportd.previous"
    ok "Service and binary removed. Game server containers were not touched."

    warn "$INSTALL_DIR still holds your server files, backups, database and configuration."
    local answer
    ask answer "To delete ALL of it permanently, type DELETE (press enter to keep it)"
    if [[ "$answer" == "DELETE" ]]; then
        rm -rf -- "$INSTALL_DIR/volumes" "$INSTALL_DIR/backups" "$INSTALL_DIR/config"
        rm -f -- "$INSTALL_DIR"/skyportd.db*
        rmdir -- "$INSTALL_DIR" 2>/dev/null || warn "$INSTALL_DIR is not empty and was kept."
        rm -f -- "$SETTINGS_FILE"
        ok "Data deleted"
    else
        ok "Data kept in $INSTALL_DIR"
    fi

    rm -f -- "$CLI_PATH"
}

cmd_help() {
    cat <<HELP
skyport - manage the Skyport node daemon

Usage: sudo skyport <command>

  install            Install and configure skyportd
  configure          Change options, panel connection, SSL or start at boot
  status             Show the daemon state
  start              Start the daemon
  stop               Stop the daemon
  restart            Restart the daemon
  logs [options]     Follow the logs (options are passed to journalctl)
  update [version]   Update to the latest release, or to a given version
  uninstall          Remove skyportd
  help               Show this help

Stopping, restarting or updating the daemon does not stop your game servers.
HELP
}

main() {
    local command="${1:-}"
    [[ $# -gt 0 ]] && shift

    if [[ -z "$command" ]]; then
        # Running the downloaded script without arguments starts the installation.
        if [[ -f "$SETTINGS_FILE" ]]; then command="help"; else command="install"; fi
    fi

    case "$command" in
        install) cmd_install ;;
        configure | config) cmd_configure ;;
        status) cmd_status ;;
        start) cmd_start ;;
        stop) cmd_stop ;;
        restart) cmd_restart ;;
        logs) cmd_logs "$@" ;;
        update | upgrade) cmd_update "$@" ;;
        uninstall | remove) cmd_uninstall ;;
        help | -h | --help) cmd_help ;;
        *)
            cmd_help >&2
            die "Unknown command: $command"
            ;;
    esac
}

main "$@"
