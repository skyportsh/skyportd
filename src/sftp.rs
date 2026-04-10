use std::collections::HashMap;
use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use russh::server::{Auth, Handler, Msg, Server as SshServer, Session};
use russh::{Channel, ChannelId, CryptoVec};
use serde::Deserialize;
use ssh_key::PrivateKey;
use tokio::sync::{Mutex, watch};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::{DaemonConfig, managed_server_volume_path};

#[derive(Debug, Deserialize)]
struct SftpAuthResponse {
    server_id: u64,
    #[allow(dead_code)]
    user_id: u64,
    #[allow(dead_code)]
    permissions: Vec<String>,
}

struct SftpServerInstance {
    config: DaemonConfig,
}

impl SshServer for SftpServerInstance {
    type Handler = SftpSession;

    fn new_client(&mut self, _peer_addr: Option<std::net::SocketAddr>) -> Self::Handler {
        SftpSession {
            config: self.config.clone(),
            server_id: None,
            handles: Arc::new(Mutex::new(HashMap::new())),
            next_handle_id: Arc::new(Mutex::new(0u64)),
        }
    }
}

struct SftpSession {
    config: DaemonConfig,
    server_id: Option<u64>,
    handles: Arc<Mutex<HashMap<String, SftpHandle>>>,
    next_handle_id: Arc<Mutex<u64>>,
}

enum SftpHandle {
    File {
        file: tokio::fs::File,
        path: PathBuf,
    },
    Dir {
        path: PathBuf,
        read: bool,
    },
}

impl SftpSession {
    fn volume_path(&self) -> Result<PathBuf> {
        let server_id = self.server_id.context("not authenticated")?;
        managed_server_volume_path(server_id)
    }

    fn resolve_path(&self, requested: &str) -> Result<PathBuf> {
        let volume = self.volume_path()?;
        let cleaned = requested.trim_start_matches('/');

        if cleaned.is_empty() || cleaned == "." {
            return Ok(volume);
        }

        let joined = volume.join(cleaned);
        let canonical = if joined.exists() {
            fs::canonicalize(&joined)?
        } else {
            let parent = joined.parent().context("invalid path")?;
            let canonical_parent = fs::canonicalize(parent).unwrap_or_else(|_| parent.to_path_buf());
            canonical_parent.join(joined.file_name().context("invalid filename")?)
        };

        if !canonical.starts_with(&volume) {
            bail!("path escapes server volume");
        }

        Ok(canonical)
    }

    fn relative_path(&self, abs_path: &PathBuf) -> String {
        if let Ok(volume) = self.volume_path() {
            if let Ok(rel) = abs_path.strip_prefix(&volume) {
                let s = rel.to_string_lossy().replace('\\', "/");
                return format!("/{s}");
            }
        }
        "/".to_string()
    }

    async fn alloc_handle(&self, handle: SftpHandle) -> String {
        let mut id = self.next_handle_id.lock().await;
        let key = format!("h{}", *id);
        *id += 1;
        self.handles.lock().await.insert(key.clone(), handle);
        key
    }

    fn metadata_to_attrs(metadata: &fs::Metadata) -> Vec<u8> {
        let flags: u32 = 0x01 | 0x02 | 0x04 | 0x08; // SIZE | UIDGID | PERMISSIONS | ACMODTIME
        let size = metadata.len();
        let uid = metadata.uid();
        let gid = metadata.gid();
        let perms = metadata.permissions().mode();
        let atime = metadata
            .accessed()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as u32)
            .unwrap_or(0);
        let mtime = metadata
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as u32)
            .unwrap_or(0);

        let mut buf = Vec::with_capacity(28);
        buf.extend_from_slice(&flags.to_be_bytes());
        buf.extend_from_slice(&size.to_be_bytes());
        buf.extend_from_slice(&uid.to_be_bytes());
        buf.extend_from_slice(&gid.to_be_bytes());
        buf.extend_from_slice(&perms.to_be_bytes());
        buf.extend_from_slice(&atime.to_be_bytes());
        buf.extend_from_slice(&mtime.to_be_bytes());
        buf
    }
}

async fn authenticate_sftp(
    config: &DaemonConfig,
    username: &str,
    password: &str,
) -> Result<SftpAuthResponse> {
    let panel_url = config.panel.url.trim_end_matches('/');
    let callback_token = config
        .panel
        .daemon_callback_token
        .as_deref()
        .context("daemon not enrolled")?;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("{panel_url}/api/daemon/sftp/auth"))
        .bearer_auth(callback_token)
        .json(&serde_json::json!({
            "username": username,
            "password": password,
        }))
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .context("failed to reach panel for SFTP auth")?;

    if !response.status().is_success() {
        bail!("authentication failed");
    }

    response
        .json::<SftpAuthResponse>()
        .await
        .context("invalid auth response from panel")
}

// SFTP packet type constants
const SSH_FXP_INIT: u8 = 1;
const SSH_FXP_VERSION: u8 = 2;
const SSH_FXP_OPEN: u8 = 3;
const SSH_FXP_CLOSE: u8 = 4;
const SSH_FXP_READ: u8 = 5;
const SSH_FXP_WRITE: u8 = 6;
const SSH_FXP_LSTAT: u8 = 7;
const SSH_FXP_FSTAT: u8 = 8;
const SSH_FXP_OPENDIR: u8 = 11;
const SSH_FXP_READDIR: u8 = 12;
const SSH_FXP_REMOVE: u8 = 13;
const SSH_FXP_MKDIR: u8 = 14;
const SSH_FXP_RMDIR: u8 = 15;
const SSH_FXP_REALPATH: u8 = 16;
const SSH_FXP_STAT: u8 = 17;
const SSH_FXP_RENAME: u8 = 18;
const SSH_FXP_STATUS: u8 = 101;
const SSH_FXP_HANDLE: u8 = 102;
const SSH_FXP_DATA: u8 = 103;
const SSH_FXP_NAME: u8 = 104;
const SSH_FXP_ATTRS: u8 = 105;

const SSH_FX_OK: u32 = 0;
const SSH_FX_EOF: u32 = 1;
const SSH_FX_NO_SUCH_FILE: u32 = 2;
const SSH_FX_PERMISSION_DENIED: u32 = 3;
const SSH_FX_FAILURE: u32 = 4;
const SSH_FX_OP_UNSUPPORTED: u32 = 8;

fn read_u32(data: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ])
}

fn read_string(data: &[u8], offset: usize) -> (&[u8], usize) {
    let len = read_u32(data, offset) as usize;
    (&data[offset + 4..offset + 4 + len], offset + 4 + len)
}

fn write_u32(buf: &mut Vec<u8>, val: u32) {
    buf.extend_from_slice(&val.to_be_bytes());
}

fn write_string(buf: &mut Vec<u8>, s: &[u8]) {
    write_u32(buf, s.len() as u32);
    buf.extend_from_slice(s);
}

fn sftp_packet(packet_type: u8, payload: &[u8]) -> Vec<u8> {
    let len = (payload.len() + 1) as u32;
    let mut buf = Vec::with_capacity(4 + 1 + payload.len());
    write_u32(&mut buf, len);
    buf.push(packet_type);
    buf.extend_from_slice(payload);
    buf
}

fn status_packet(id: u32, code: u32, msg: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    write_u32(&mut payload, id);
    write_u32(&mut payload, code);
    write_string(&mut payload, msg.as_bytes());
    write_string(&mut payload, b"en");
    sftp_packet(SSH_FXP_STATUS, &payload)
}

fn handle_packet(id: u32, handle: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    write_u32(&mut payload, id);
    write_string(&mut payload, handle.as_bytes());
    sftp_packet(SSH_FXP_HANDLE, &payload)
}

#[async_trait]
impl Handler for SftpSession {
    type Error = anyhow::Error;

    async fn auth_password(
        &mut self,
        user: &str,
        password: &str,
    ) -> std::result::Result<Auth, Self::Error> {
        match authenticate_sftp(&self.config, user, password).await {
            Ok(auth) => {
                info!(server_id = auth.server_id, "SFTP session authenticated");
                self.server_id = Some(auth.server_id);
                Ok(Auth::Accept)
            }
            Err(error) => {
                warn!(user = user, error = %error, "SFTP auth rejected");
                Ok(Auth::Reject {
                    proceed_with_methods: None,
                })
            }
        }
    }

    async fn channel_open_session(
        &mut self,
        _channel: Channel<Msg>,
        _session: &mut Session,
    ) -> std::result::Result<bool, Self::Error> {
        Ok(self.server_id.is_some())
    }

    async fn subsystem_request(
        &mut self,
        channel_id: ChannelId,
        name: &str,
        session: &mut Session,
    ) -> std::result::Result<(), Self::Error> {
        if name == "sftp" {
            session.channel_success(channel_id)?;
        } else {
            session.channel_failure(channel_id)?;
        }
        Ok(())
    }

    async fn data(
        &mut self,
        channel_id: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> std::result::Result<(), Self::Error> {
        if self.server_id.is_none() || data.len() < 5 {
            return Ok(());
        }

        let _pkt_len = read_u32(data, 0);
        let pkt_type = data[4];

        let response = match pkt_type {
            SSH_FXP_INIT => {
                // Send version 3
                let mut payload = Vec::new();
                write_u32(&mut payload, 3);
                sftp_packet(SSH_FXP_VERSION, &payload)
            }
            SSH_FXP_REALPATH => {
                let id = read_u32(data, 5);
                let (path_bytes, _) = read_string(data, 9);
                let path_str = std::str::from_utf8(path_bytes).unwrap_or("/");

                let resolved = self
                    .resolve_path(path_str)
                    .map(|p| self.relative_path(&p))
                    .unwrap_or_else(|_| "/".to_string());

                let mut payload = Vec::new();
                write_u32(&mut payload, id);
                write_u32(&mut payload, 1); // count
                write_string(&mut payload, resolved.as_bytes());
                write_string(&mut payload, resolved.as_bytes()); // longname
                // Empty attrs
                payload.extend_from_slice(&0u32.to_be_bytes());
                sftp_packet(SSH_FXP_NAME, &payload)
            }
            SSH_FXP_STAT | SSH_FXP_LSTAT => {
                let id = read_u32(data, 5);
                let (path_bytes, _) = read_string(data, 9);
                let path_str = std::str::from_utf8(path_bytes).unwrap_or("/");

                match self.resolve_path(path_str).and_then(|p| Ok(fs::metadata(&p)?)) {
                    Ok(metadata) => {
                        let mut payload = Vec::new();
                        write_u32(&mut payload, id);
                        payload.extend_from_slice(&SftpSession::metadata_to_attrs(&metadata));
                        sftp_packet(SSH_FXP_ATTRS, &payload)
                    }
                    Err(_) => status_packet(id, SSH_FX_NO_SUCH_FILE, "No such file"),
                }
            }
            SSH_FXP_OPENDIR => {
                let id = read_u32(data, 5);
                let (path_bytes, _) = read_string(data, 9);
                let path_str = std::str::from_utf8(path_bytes).unwrap_or("/");

                match self.resolve_path(path_str) {
                    Ok(path) if path.is_dir() => {
                        let h = self
                            .alloc_handle(SftpHandle::Dir {
                                path,
                                read: false,
                            })
                            .await;
                        handle_packet(id, &h)
                    }
                    _ => status_packet(id, SSH_FX_NO_SUCH_FILE, "Not a directory"),
                }
            }
            SSH_FXP_READDIR => {
                let id = read_u32(data, 5);
                let (handle_bytes, _) = read_string(data, 9);
                let handle_key = std::str::from_utf8(handle_bytes).unwrap_or("").to_string();

                let mut handles = self.handles.lock().await;
                if let Some(SftpHandle::Dir { path, read }) = handles.get_mut(&handle_key) {
                    if *read {
                        status_packet(id, SSH_FX_EOF, "End of directory")
                    } else {
                        *read = true;
                        let entries: Vec<_> = fs::read_dir(&path)
                            .into_iter()
                            .flatten()
                            .filter_map(|e| e.ok())
                            .collect();

                        let mut payload = Vec::new();
                        write_u32(&mut payload, id);
                        write_u32(&mut payload, entries.len() as u32);

                        for entry in entries {
                            let name = entry.file_name().to_string_lossy().to_string();
                            let meta = entry.metadata().ok();
                            write_string(&mut payload, name.as_bytes());
                            // longname
                            write_string(&mut payload, name.as_bytes());
                            if let Some(m) = &meta {
                                payload.extend_from_slice(&SftpSession::metadata_to_attrs(m));
                            } else {
                                payload.extend_from_slice(&0u32.to_be_bytes());
                            }
                        }

                        sftp_packet(SSH_FXP_NAME, &payload)
                    }
                } else {
                    status_packet(id, SSH_FX_FAILURE, "Invalid handle")
                }
            }
            SSH_FXP_OPEN => {
                let id = read_u32(data, 5);
                let (path_bytes, next) = read_string(data, 9);
                let path_str = std::str::from_utf8(path_bytes).unwrap_or("");
                let pflags = read_u32(data, next);

                match self.resolve_path(path_str) {
                    Ok(path) => {
                        let mut opts = tokio::fs::OpenOptions::new();

                        if pflags & 0x01 != 0 {
                            opts.read(true);
                        }
                        if pflags & 0x02 != 0 {
                            opts.write(true);
                        }
                        if pflags & 0x08 != 0 {
                            opts.create(true);
                        }
                        if pflags & 0x10 != 0 {
                            opts.truncate(true);
                        }
                        if pflags & 0x04 != 0 {
                            opts.append(true);
                        }

                        match opts.open(&path).await {
                            Ok(file) => {
                                let h = self
                                    .alloc_handle(SftpHandle::File { file, path })
                                    .await;
                                handle_packet(id, &h)
                            }
                            Err(_) => status_packet(id, SSH_FX_FAILURE, "Failed to open file"),
                        }
                    }
                    Err(_) => status_packet(id, SSH_FX_NO_SUCH_FILE, "No such file"),
                }
            }
            SSH_FXP_READ => {
                let id = read_u32(data, 5);
                let (handle_bytes, next) = read_string(data, 9);
                let handle_key = std::str::from_utf8(handle_bytes).unwrap_or("").to_string();
                let offset = u64::from_be_bytes([
                    data[next],
                    data[next + 1],
                    data[next + 2],
                    data[next + 3],
                    data[next + 4],
                    data[next + 5],
                    data[next + 6],
                    data[next + 7],
                ]);
                let len = read_u32(data, next + 8) as usize;

                let mut handles = self.handles.lock().await;
                if let Some(SftpHandle::File { file, .. }) = handles.get_mut(&handle_key) {
                    use tokio::io::{AsyncReadExt, AsyncSeekExt};
                    let _ = file.seek(std::io::SeekFrom::Start(offset)).await;
                    let mut buf = vec![0u8; len.min(32768)];
                    match file.read(&mut buf).await {
                        Ok(0) => status_packet(id, SSH_FX_EOF, "End of file"),
                        Ok(n) => {
                            let mut payload = Vec::new();
                            write_u32(&mut payload, id);
                            write_string(&mut payload, &buf[..n]);
                            sftp_packet(SSH_FXP_DATA, &payload)
                        }
                        Err(_) => status_packet(id, SSH_FX_FAILURE, "Read failed"),
                    }
                } else {
                    status_packet(id, SSH_FX_FAILURE, "Invalid handle")
                }
            }
            SSH_FXP_WRITE => {
                let id = read_u32(data, 5);
                let (handle_bytes, next) = read_string(data, 9);
                let handle_key = std::str::from_utf8(handle_bytes).unwrap_or("").to_string();
                let offset = u64::from_be_bytes([
                    data[next],
                    data[next + 1],
                    data[next + 2],
                    data[next + 3],
                    data[next + 4],
                    data[next + 5],
                    data[next + 6],
                    data[next + 7],
                ]);
                let (write_data, _) = read_string(data, next + 8);

                let mut handles = self.handles.lock().await;
                if let Some(SftpHandle::File { file, .. }) = handles.get_mut(&handle_key) {
                    use tokio::io::{AsyncSeekExt, AsyncWriteExt};
                    let _ = file.seek(std::io::SeekFrom::Start(offset)).await;
                    match file.write_all(write_data).await {
                        Ok(()) => status_packet(id, SSH_FX_OK, "OK"),
                        Err(_) => status_packet(id, SSH_FX_FAILURE, "Write failed"),
                    }
                } else {
                    status_packet(id, SSH_FX_FAILURE, "Invalid handle")
                }
            }
            SSH_FXP_CLOSE => {
                let id = read_u32(data, 5);
                let (handle_bytes, _) = read_string(data, 9);
                let handle_key = std::str::from_utf8(handle_bytes).unwrap_or("").to_string();
                self.handles.lock().await.remove(&handle_key);
                status_packet(id, SSH_FX_OK, "OK")
            }
            SSH_FXP_FSTAT => {
                let id = read_u32(data, 5);
                let (handle_bytes, _) = read_string(data, 9);
                let handle_key = std::str::from_utf8(handle_bytes).unwrap_or("").to_string();

                let handles = self.handles.lock().await;
                let path = match handles.get(&handle_key) {
                    Some(SftpHandle::File { path, .. }) => Some(path.clone()),
                    Some(SftpHandle::Dir { path, .. }) => Some(path.clone()),
                    None => None,
                };
                drop(handles);

                match path.and_then(|p| fs::metadata(&p).ok()) {
                    Some(metadata) => {
                        let mut payload = Vec::new();
                        write_u32(&mut payload, id);
                        payload.extend_from_slice(&SftpSession::metadata_to_attrs(&metadata));
                        sftp_packet(SSH_FXP_ATTRS, &payload)
                    }
                    None => status_packet(id, SSH_FX_FAILURE, "Invalid handle"),
                }
            }
            SSH_FXP_REMOVE => {
                let id = read_u32(data, 5);
                let (path_bytes, _) = read_string(data, 9);
                let path_str = std::str::from_utf8(path_bytes).unwrap_or("");

                match self.resolve_path(path_str) {
                    Ok(path) => match fs::remove_file(&path) {
                        Ok(()) => status_packet(id, SSH_FX_OK, "OK"),
                        Err(_) => status_packet(id, SSH_FX_FAILURE, "Failed to remove file"),
                    },
                    Err(_) => status_packet(id, SSH_FX_NO_SUCH_FILE, "No such file"),
                }
            }
            SSH_FXP_MKDIR => {
                let id = read_u32(data, 5);
                let (path_bytes, _) = read_string(data, 9);
                let path_str = std::str::from_utf8(path_bytes).unwrap_or("");

                match self.resolve_path(path_str) {
                    Ok(path) => match fs::create_dir(&path) {
                        Ok(()) => status_packet(id, SSH_FX_OK, "OK"),
                        Err(_) => status_packet(id, SSH_FX_FAILURE, "Failed to create directory"),
                    },
                    Err(_) => status_packet(id, SSH_FX_PERMISSION_DENIED, "Permission denied"),
                }
            }
            SSH_FXP_RMDIR => {
                let id = read_u32(data, 5);
                let (path_bytes, _) = read_string(data, 9);
                let path_str = std::str::from_utf8(path_bytes).unwrap_or("");

                match self.resolve_path(path_str) {
                    Ok(path) => match fs::remove_dir_all(&path) {
                        Ok(()) => status_packet(id, SSH_FX_OK, "OK"),
                        Err(_) => status_packet(id, SSH_FX_FAILURE, "Failed to remove directory"),
                    },
                    Err(_) => status_packet(id, SSH_FX_NO_SUCH_FILE, "No such file"),
                }
            }
            SSH_FXP_RENAME => {
                let id = read_u32(data, 5);
                let (old_bytes, next) = read_string(data, 9);
                let (new_bytes, _) = read_string(data, next);
                let old_str = std::str::from_utf8(old_bytes).unwrap_or("");
                let new_str = std::str::from_utf8(new_bytes).unwrap_or("");

                match (self.resolve_path(old_str), self.resolve_path(new_str)) {
                    (Ok(old_path), Ok(new_path)) => match fs::rename(&old_path, &new_path) {
                        Ok(()) => status_packet(id, SSH_FX_OK, "OK"),
                        Err(_) => status_packet(id, SSH_FX_FAILURE, "Failed to rename"),
                    },
                    _ => status_packet(id, SSH_FX_NO_SUCH_FILE, "No such file"),
                }
            }
            _ => {
                let id = if data.len() >= 9 { read_u32(data, 5) } else { 0 };
                status_packet(id, SSH_FX_OP_UNSUPPORTED, "Operation not supported")
            }
        };

        session.data(channel_id, CryptoVec::from_slice(&response))?;
        Ok(())
    }
}

pub struct SftpService {
    config_rx: watch::Receiver<DaemonConfig>,
    cancellation: CancellationToken,
}

impl SftpService {
    pub fn new(
        config_rx: watch::Receiver<DaemonConfig>,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            config_rx,
            cancellation,
        }
    }

    pub async fn run(self) -> Result<()> {
        let config = self.config_rx.borrow().clone();
        let node = match config.node.as_ref() {
            Some(node) => node,
            None => {
                info!("SFTP server waiting for node configuration");
                // Wait until cancelled — the heartbeat service will configure the node
                self.cancellation.cancelled().await;
                return Ok(());
            }
        };

        let sftp_port = node.sftp_port;
        let listen_addr = format!("0.0.0.0:{sftp_port}");

        let key_pair = PrivateKey::random(&mut rand::thread_rng(), ssh_key::Algorithm::Ed25519)
            .context("failed to generate SSH host key")?;

        let ssh_config = russh::server::Config {
            auth_rejection_time: Duration::from_secs(1),
            auth_rejection_time_initial: Some(Duration::from_secs(0)),
            keys: vec![key_pair],
            ..Default::default()
        };

        let mut server = SftpServerInstance {
            config: config.clone(),
        };

        info!(address = %listen_addr, "SFTP server starting");

        let cancellation = self.cancellation.clone();

        tokio::select! {
            result = server.run_on_address(Arc::new(ssh_config), &listen_addr) => {
                if let Err(error) = result {
                    if !cancellation.is_cancelled() {
                        warn!(error = %error, "SFTP server stopped unexpectedly");
                    }
                }
            }
            _ = cancellation.cancelled() => {
                info!("SFTP server stopping");
            }
        }

        Ok(())
    }
}
