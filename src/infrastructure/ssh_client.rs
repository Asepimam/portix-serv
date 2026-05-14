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

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const AUTH_TIMEOUT: Duration = Duration::from_secs(15);
const INACTIVITY_TIMEOUT: Duration = Duration::from_secs(120);
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
                            let result = run_exec(&session, command).await;
                            let _ = response_tx.send(result);
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
        let config = Arc::new(client::Config {
            inactivity_timeout: Some(INACTIVITY_TIMEOUT),
            ..Default::default()
        });
        let mut session = timeout(
            CONNECT_TIMEOUT,
            client::connect(config, self.profile.socket_addr(), Client),
        )
        .await
        .map_err(|_| PortixError::ConnectionTimeout)??;

        let auth_result = timeout(AUTH_TIMEOUT, async {
            if let Some(path) = self.profile.private_key_path.as_deref() {
                let key_path = expand_user_path(path);
                let key_pair = load_secret_key(key_path, None)?;
                session
                    .authenticate_publickey(
                        self.profile.username.clone(),
                        PrivateKeyWithHashAlg::new(
                            Arc::new(key_pair),
                            session.best_supported_rsa_hash().await?.flatten(),
                        ),
                    )
                    .await
            } else if let Some(password) = self.profile.password.clone() {
                session
                    .authenticate_password(self.profile.username.clone(), password)
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

    while let Some(msg) = channel.wait().await {
        match msg {
            ChannelMsg::Data { data } | ChannelMsg::ExtendedData { data, .. } => {
                output.extend_from_slice(&data);
            }
            ChannelMsg::Eof | ChannelMsg::Close => break,
            ChannelMsg::ExitStatus { .. } => break,
            _ => {}
        }
    }

    channel.close().await.ok();
    Ok(String::from_utf8_lossy(&output).to_string())
}

fn normalize_terminal_size(cols: u32, rows: u32) -> (u32, u32) {
    (
        cols.clamp(MIN_COLS, MAX_COLS),
        rows.clamp(MIN_ROWS, MAX_ROWS),
    )
}

fn expand_user_path(path: &str) -> PathBuf {
    if path == "~" {
        if let Ok(home) = env::var("HOME") {
            return PathBuf::from(home);
        }
    }

    if let Some(rest) = path.strip_prefix("~/") {
        if let Ok(home) = env::var("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }

    PathBuf::from(path)
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
