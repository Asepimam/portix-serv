use flutter_rust_bridge::frb;
use once_cell::sync::Lazy;
use serde::Serialize;
use tokio::sync::broadcast;

use crate::application::session_manager::SessionManager;
use crate::domain::profile::SshProfile;
use crate::domain::session::{RemoteFileEntry, RemoteSystemSnapshot, SessionInfo};
use crate::frb_generated::StreamSink;

static SESSION_MANAGER: Lazy<SessionManager> = Lazy::new(SessionManager::new);

#[frb(init)]
pub fn init_app() {
    flutter_rust_bridge::setup_default_user_utils();
}

pub async fn connect(profile: SshProfile, cols: u32, rows: u32) -> anyhow::Result<SessionInfo> {
    Ok(SESSION_MANAGER.connect(profile, cols, rows).await?)
}

pub async fn disconnect(session_id: String) -> anyhow::Result<()> {
    Ok(SESSION_MANAGER.disconnect(session_id).await?)
}

pub async fn send_terminal_input(session_id: String, data: Vec<u8>) -> anyhow::Result<()> {
    Ok(SESSION_MANAGER
        .send_terminal_input(session_id, data)
        .await?)
}

pub async fn resize_terminal(session_id: String, cols: u32, rows: u32) -> anyhow::Result<()> {
    Ok(SESSION_MANAGER
        .resize_terminal(session_id, cols, rows)
        .await?)
}

pub async fn remote_system_snapshot(session_id: String) -> anyhow::Result<RemoteSystemSnapshot> {
    Ok(SESSION_MANAGER.remote_system_snapshot(session_id).await?)
}

pub async fn list_remote_directory(
    session_id: String,
    path: String,
) -> anyhow::Result<Vec<RemoteFileEntry>> {
    Ok(SESSION_MANAGER
        .list_remote_directory(session_id, path)
        .await?)
}

pub async fn resolve_remote_directory(session_id: String, path: String) -> anyhow::Result<String> {
    Ok(SESSION_MANAGER
        .resolve_remote_directory(session_id, path)
        .await?)
}

pub async fn terminal_output_stream(sink: StreamSink<String>) -> anyhow::Result<()> {
    let mut rx = SESSION_MANAGER.terminal_output_stream();
    tokio::spawn(async move {
        forward_json_stream(&mut rx, sink).await;
    });
    Ok(())
}

pub async fn connection_status_stream(sink: StreamSink<String>) -> anyhow::Result<()> {
    let mut rx = SESSION_MANAGER.connection_status_stream();
    tokio::spawn(async move {
        forward_json_stream(&mut rx, sink).await;
    });
    Ok(())
}

pub async fn error_event_stream(sink: StreamSink<String>) -> anyhow::Result<()> {
    let mut rx = SESSION_MANAGER.error_event_stream();
    tokio::spawn(async move {
        forward_json_stream(&mut rx, sink).await;
    });
    Ok(())
}

async fn forward_json_stream<T>(rx: &mut broadcast::Receiver<T>, sink: StreamSink<String>)
where
    T: Clone + Serialize,
{
    loop {
        match rx.recv().await {
            Ok(event) => {
                let Ok(json) = serde_json::to_string(&event) else {
                    continue;
                };
                if sink.add(json).is_err() {
                    break;
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}
