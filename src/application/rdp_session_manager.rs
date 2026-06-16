use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{RwLock, broadcast, mpsc, oneshot};
use uuid::Uuid;

use crate::domain::errors::{PortixError, Result};
use crate::domain::events::{ConnectionStatusEvent, ErrorEvent};
use crate::domain::rdp_profile::RdpProfile;
use crate::domain::session::ConnectionStatus;
use crate::infrastructure::rdp_client::{
    MouseButton, RdpClipboardEvent, RdpCommand, RdpFrameEvent, RdpRuntime,
};

/// Information about an active RDP session.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RdpSessionInfo {
    pub id: String,
    pub profile_id: String,
    pub width: u16,
    pub height: u16,
}

#[derive(Clone)]
struct ManagedRdpSession {
    command_tx: mpsc::Sender<RdpCommand>,
}

pub struct RdpSessionManager {
    sessions: Arc<RwLock<HashMap<String, ManagedRdpSession>>>,
    frame_tx: broadcast::Sender<RdpFrameEvent>,
    clipboard_tx: broadcast::Sender<RdpClipboardEvent>,
    status_tx: broadcast::Sender<ConnectionStatusEvent>,
    error_tx: broadcast::Sender<ErrorEvent>,
}

impl RdpSessionManager {
    pub fn new() -> Self {
        let (frame_tx, _) = broadcast::channel(16);
        let (clipboard_tx, _) = broadcast::channel(64);
        let (status_tx, _) = broadcast::channel(256);
        let (error_tx, _) = broadcast::channel(256);
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            frame_tx,
            clipboard_tx,
            status_tx,
            error_tx,
        }
    }

    pub fn frame_stream(&self) -> broadcast::Receiver<RdpFrameEvent> {
        self.frame_tx.subscribe()
    }

    pub fn connection_status_stream(&self) -> broadcast::Receiver<ConnectionStatusEvent> {
        self.status_tx.subscribe()
    }

    pub fn clipboard_stream(&self) -> broadcast::Receiver<RdpClipboardEvent> {
        self.clipboard_tx.subscribe()
    }

    pub fn error_event_stream(&self) -> broadcast::Receiver<ErrorEvent> {
        self.error_tx.subscribe()
    }

    pub async fn connect(&self, profile: RdpProfile) -> Result<RdpSessionInfo> {
        profile.validate()?;

        let session_id = Uuid::new_v4().to_string();
        let (command_tx, command_rx) = mpsc::channel(256);

        let info = RdpSessionInfo {
            id: session_id.clone(),
            profile_id: profile.id.clone(),
            width: profile.width,
            height: profile.height,
        };

        self.sessions.write().await.insert(
            session_id.clone(),
            ManagedRdpSession {
                command_tx: command_tx.clone(),
            },
        );

        // Emit connecting status
        let _ = self.status_tx.send(ConnectionStatusEvent {
            session_id: session_id.clone(),
            status: ConnectionStatus::Connecting,
            message: Some("connecting".to_owned()),
        });

        let sessions = self.sessions.clone();
        let frame_tx = self.frame_tx.clone();
        let clipboard_tx = self.clipboard_tx.clone();
        let status_tx = self.status_tx.clone();
        let error_tx = self.error_tx.clone();
        let sid = session_id.clone();

        tokio::spawn(async move {
            let runtime = RdpRuntime::new(
                profile,
                sid.clone(),
                frame_tx,
                clipboard_tx,
                status_tx.clone(),
            );

            let result = runtime.run(command_rx).await;

            // Send status/error events BEFORE removing the session so that
            // any in-flight requestFrame calls get a proper response instead
            // of a SessionNotFound error.
            match &result {
                Ok(()) => {
                    let _ = status_tx.send(ConnectionStatusEvent {
                        session_id: sid.clone(),
                        status: ConnectionStatus::Disconnected,
                        message: Some("disconnected".to_owned()),
                    });
                }
                Err(error) => {
                    let msg = error.to_string();
                    let _ = error_tx.send(ErrorEvent {
                        session_id: Some(sid.clone()),
                        message: msg.clone(),
                    });
                    let _ = status_tx.send(ConnectionStatusEvent {
                        session_id: sid.clone(),
                        status: ConnectionStatus::Error,
                        message: Some(msg),
                    });
                }
            }

            // Cleanup session after events are emitted
            sessions.write().await.remove(&sid);

            // Result already handled above — nothing more to do
            let _ = result;
        });

        Ok(info)
    }

    pub async fn disconnect(&self, session_id: String) -> Result<()> {
        let session = self.session(&session_id).await?;
        let _ = session.command_tx.send(RdpCommand::Disconnect).await;
        Ok(())
    }

    pub async fn send_keyboard_input(
        &self,
        session_id: String,
        scancode: u16,
        is_pressed: bool,
    ) -> Result<()> {
        let session = self.session(&session_id).await?;
        session
            .command_tx
            .send(RdpCommand::KeyboardInput {
                scancode,
                is_pressed,
            })
            .await
            .map_err(|_| PortixError::SessionNotFound(session_id))?;
        Ok(())
    }

    pub async fn send_mouse_input(
        &self,
        session_id: String,
        x: u16,
        y: u16,
        button: u8,
        is_pressed: bool,
    ) -> Result<()> {
        let mouse_button = match button {
            0 => MouseButton::Left,
            1 => MouseButton::Right,
            2 => MouseButton::Middle,
            _ => MouseButton::Left,
        };
        let session = self.session(&session_id).await?;
        session
            .command_tx
            .send(RdpCommand::MouseInput {
                x,
                y,
                button: mouse_button,
                is_pressed,
            })
            .await
            .map_err(|_| PortixError::SessionNotFound(session_id))?;
        Ok(())
    }

    pub async fn send_mouse_move(&self, session_id: String, x: u16, y: u16) -> Result<()> {
        let session = self.session(&session_id).await?;
        session
            .command_tx
            .send(RdpCommand::MouseMove { x, y })
            .await
            .map_err(|_| PortixError::SessionNotFound(session_id))?;
        Ok(())
    }

    pub async fn set_clipboard_text(&self, session_id: String, text: String) -> Result<()> {
        let session = self.session(&session_id).await?;
        session
            .command_tx
            .send(RdpCommand::SetClipboardText { text })
            .await
            .map_err(|_| PortixError::SessionNotFound(session_id))?;
        Ok(())
    }

    pub async fn request_frame(&self, session_id: String) -> Result<Vec<u8>> {
        let session = self.session(&session_id).await?;
        let (response_tx, response_rx) = oneshot::channel();
        session
            .command_tx
            .send(RdpCommand::RequestFrame { response_tx })
            .await
            .map_err(|_| PortixError::SessionNotFound(session_id.clone()))?;
        response_rx
            .await
            .map(|frame| (*frame).clone())
            .map_err(|_| PortixError::SessionNotFound(session_id))
    }

    async fn session(&self, session_id: &str) -> Result<ManagedRdpSession> {
        self.sessions
            .read()
            .await
            .get(session_id)
            .cloned()
            .ok_or_else(|| PortixError::SessionNotFound(session_id.to_owned()))
    }
}

impl Default for RdpSessionManager {
    fn default() -> Self {
        Self::new()
    }
}
