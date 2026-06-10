use std::sync::Arc;
use std::time::Duration;
use std::{env, path::PathBuf};

use russh::client;
use russh::keys::{PrivateKeyWithHashAlg, load_secret_key};
use russh::{ChannelMsg, Disconnect};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::timeout;

use crate::domain::errors::{PortixError, Result};
use crate::domain::events::{ConnectionStatusEvent, ErrorEvent, TerminalOutputEvent};
use crate::domain::profile::SshProfile;
use crate::domain::session::ConnectionStatus;

pub enum SshCommand {
    Input(Vec<u8>),
    Resize {
        cols: u32,
        rows: u32,
    },
    Exec {
        command: String,
        response_tx: oneshot::Sender<Result<String>>,
    },
    Disconnect,
}

pub struct SshRuntime {
    profile: SshProfile,
    session_id: String,
    output_tx: broadcast::Sender<TerminalOutputEvent>,
    status_tx: broadcast::Sender<ConnectionStatusEvent>,
    error_tx: broadcast::Sender<ErrorEvent>,
}

struct Client;

type ExecRequest = (String, oneshot::Sender<Result<String>>);

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const AUTH_TIMEOUT: Duration = Duration::from_secs(15);
const MIN_COLS: u32 = 20;
const MIN_ROWS: u32 = 5;
const MAX_COLS: u32 = 512;
const MAX_ROWS: u32 = 256;

impl client::Handler for Client {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &russh::keys::ssh_key::PublicKey,
    ) -> std::result::Result<bool, Self::Error> {
        Ok(true)
    }
}

impl SshRuntime {
    pub fn new(
        profile: SshProfile,
        session_id: String,
        output_tx: broadcast::Sender<TerminalOutputEvent>,
        status_tx: broadcast::Sender<ConnectionStatusEvent>,
        error_tx: broadcast::Sender<ErrorEvent>,
    ) -> Self {
        Self {
            profile,
            session_id,
            output_tx,
            status_tx,
            error_tx,
        }
    }

    pub async fn run(
        self,
        mut command_rx: mpsc::Receiver<SshCommand>,
        cols: u32,
        rows: u32,
    ) -> Result<()> {
        let session = self.connect_and_authenticate().await?;
        let mut channel = session.channel_open_session().await?;
        let (cols, rows) = normalize_terminal_size(cols, rows);
        channel
            .request_pty(false, "xterm-256color", cols, rows, 0, 0, &[])
            .await?;
        channel.request_shell(true).await?;
        let (exec_tx, exec_rx) = mpsc::channel::<ExecRequest>(64);
        tokio::spawn(run_exec_worker(self.profile.clone(), exec_rx));
        self.emit_status(ConnectionStatus::Connected, Some("connected"));

        loop {
            tokio::select! {
                command = command_rx.recv() => {
                    match command {
                        Some(SshCommand::Input(data)) => channel.data(&data[..]).await?,
                        Some(SshCommand::Resize { cols, rows }) => {
                            let (cols, rows) = normalize_terminal_size(cols, rows);
                            channel.window_change(cols, rows, 0, 0).await?
                        },
                        Some(SshCommand::Exec { command, response_tx }) => {
                            exec_tx
                                .send((command, response_tx))
                                .await
                                .map_err(|_| PortixError::SessionNotFound(self.session_id.clone()))?;
                        }
                        Some(SshCommand::Disconnect) | None => {
                            channel.eof().await.ok();
                            channel.close().await.ok();
                            session.disconnect(Disconnect::ByApplication, "", "en").await.ok();
                            break;
                        }
                    }
                }
                msg = channel.wait() => {
                    match msg {
                        Some(ChannelMsg::Data { data }) | Some(ChannelMsg::ExtendedData { data, .. }) => {
                            let _ = self.output_tx.send(TerminalOutputEvent {
                                session_id: self.session_id.clone(),
                                data: data.to_vec(),
                            });
                        }
                        Some(ChannelMsg::Eof) | Some(ChannelMsg::Close) | None => break,
                        Some(ChannelMsg::ExitStatus { .. }) => break,
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }

    async fn connect_and_authenticate(&self) -> Result<client::Handle<Client>> {
        connect_and_authenticate_profile(&self.profile).await
    }

    fn emit_status(&self, status: ConnectionStatus, message: Option<&str>) {
        let _ = self.status_tx.send(ConnectionStatusEvent {
            session_id: self.session_id.clone(),
            status,
            message: message.map(str::to_owned),
        });
    }

    #[allow(dead_code)]
    fn emit_error(&self, message: impl Into<String>) {
        let _ = self.error_tx.send(ErrorEvent {
            session_id: Some(self.session_id.clone()),
            message: message.into(),
        });
    }
}

async fn run_exec(session: &client::Handle<Client>, command: String) -> Result<String> {
    let mut channel = session.channel_open_session().await?;
    channel.exec(true, command).await?;
    let mut output = Vec::new();
    let mut exit_status = None;

    while let Some(msg) = channel.wait().await {
        match msg {
            ChannelMsg::Data { data } | ChannelMsg::ExtendedData { data, .. } => {
                output.extend_from_slice(&data);
            }
            ChannelMsg::Eof | ChannelMsg::Close => break,
            ChannelMsg::ExitStatus {
                exit_status: status,
            } => {
                exit_status = Some(status);
            }
            _ => {}
        }
    }

    channel.close().await.ok();
    let output = String::from_utf8_lossy(&output).to_string();
    if let Some(status) = exit_status
        && status != 0
    {
        let detail = output
            .lines()
            .rev()
            .find(|line| !line.trim().is_empty())
            .map(str::trim)
            .unwrap_or("remote command failed");
        return Err(PortixError::InvalidRequest(format!(
            "remote command exited with {status}: {detail}"
        )));
    }
    Ok(output)
}

async fn run_exec_worker(profile: SshProfile, mut rx: mpsc::Receiver<ExecRequest>) {
    let mut session = connect_and_authenticate_profile(&profile).await.ok();
    while let Some((command, response_tx)) = rx.recv().await {
        // Try to establish session if not connected (with one retry).
        if session.is_none() {
            session = connect_and_authenticate_profile(&profile).await.ok();
            if session.is_none() {
                // Retry once after a brief delay.
                tokio::time::sleep(Duration::from_millis(500)).await;
                session = connect_and_authenticate_profile(&profile).await.ok();
            }
        }

        let result = if let Some(handle) = session.as_ref() {
            let result = run_exec(handle, command.clone()).await;
            if result.is_err() {
                // Connection might be broken — try a fresh one for the next request.
                session = None;
            }
            result
        } else {
            Err(PortixError::ConnectionTimeout)
        };
        let _ = response_tx.send(result);
    }

    if let Some(handle) = session {
        handle
            .disconnect(Disconnect::ByApplication, "", "en")
            .await
            .ok();
    }
}

async fn connect_and_authenticate_profile(profile: &SshProfile) -> Result<client::Handle<Client>> {
    let config = Arc::new(client::Config {
        inactivity_timeout: None,
        ..Default::default()
    });
    let mut session = timeout(
        CONNECT_TIMEOUT,
        client::connect(config, profile.socket_addr(), Client),
    )
    .await
    .map_err(|_| PortixError::ConnectionTimeout)??;

    let auth_result = timeout(AUTH_TIMEOUT, async {
        if let Some(path) = profile.private_key_path.as_deref() {
            let key_path = expand_user_path(path);
            let key_pair = load_secret_key(key_path, None)?;
            session
                .authenticate_publickey(
                    profile.username.clone(),
                    PrivateKeyWithHashAlg::new(
                        Arc::new(key_pair),
                        session.best_supported_rsa_hash().await?.flatten(),
                    ),
                )
                .await
        } else if let Some(password) = profile.password.clone() {
            session
                .authenticate_password(profile.username.clone(), password)
                .await
        } else {
            Err(russh::Error::NotAuthenticated)
        }
    })
    .await
    .map_err(|_| PortixError::AuthenticationTimeout)??;

    if !auth_result.success() {
        return Err(PortixError::AuthenticationFailed);
    }
    Ok(session)
}

fn normalize_terminal_size(cols: u32, rows: u32) -> (u32, u32) {
    (
        cols.clamp(MIN_COLS, MAX_COLS),
        rows.clamp(MIN_ROWS, MAX_ROWS),
    )
}

/// Returns the user's home directory, supporting both Unix (HOME) and Windows (USERPROFILE).
fn home_dir() -> Option<PathBuf> {
    env::var("HOME")
        .or_else(|_| env::var("USERPROFILE"))
        .ok()
        .map(PathBuf::from)
}

fn expand_user_path(path: &str) -> PathBuf {
    // Normalize forward slashes on Windows so PathBuf joins work correctly.
    let normalized = if cfg!(windows) {
        path.replace('/', "\\")
    } else {
        path.to_owned()
    };

    if normalized == "~" || normalized == "~\\" {
        if let Some(home) = home_dir() {
            return home;
        }
    }

    if let Some(rest) = normalized
        .strip_prefix("~/")
        .or_else(|| normalized.strip_prefix("~\\"))
    {
        if let Some(home) = home_dir() {
            return home.join(rest);
        }
    }

    PathBuf::from(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_terminal_size_clamps_tiny_values() {
        assert_eq!(normalize_terminal_size(1, 1), (MIN_COLS, MIN_ROWS));
    }

    #[test]
    fn normalize_terminal_size_clamps_large_values() {
        assert_eq!(normalize_terminal_size(999, 999), (MAX_COLS, MAX_ROWS));
    }
}
