use flutter_rust_bridge::frb;
use once_cell::sync::Lazy;
use serde::Serialize;
use tokio::sync::broadcast;

use crate::application::autocomplete_service::AutocompleteService;
use crate::application::session_manager::SessionManager;
use crate::domain::autocomplete::TerminalCompleteRequest;
use crate::domain::errors::PortixError;
use crate::domain::profile::SshProfile;
use crate::domain::session::{RemoteFileEntry, RemoteSystemSnapshot, SessionInfo};
use crate::frb_generated::StreamSink;

static SESSION_MANAGER: Lazy<SessionManager> = Lazy::new(SessionManager::new);
static AUTOCOMPLETE_SERVICE: Lazy<AutocompleteService> = Lazy::new(AutocompleteService::new);

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

pub async fn command_help_suggestions(
    session_id: String,
    input: String,
) -> anyhow::Result<Vec<String>> {
    Ok(SESSION_MANAGER
        .command_help_suggestions(session_id, input)
        .await?)
}

pub async fn terminal_complete(req_json: String) -> anyhow::Result<String> {
    let request = serde_json::from_str::<TerminalCompleteRequest>(&req_json)
        .map_err(|error| PortixError::InvalidRequest(error.to_string()))?;
    if request.session_id.is_some() {
        let mut response = SESSION_MANAGER.terminal_complete(request.clone()).await?;
        // Always merge local static completions (options, paths, etc.)
        let local = AUTOCOMPLETE_SERVICE.complete(request).await?;
        if response.suggestion.is_none() {
            response.suggestion = local.suggestion;
        }
        // Merge: local static options first (more concise descriptions),
        // then remote completions fill remaining slots.
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut merged = Vec::new();
        for item in local.items {
            if merged.len() >= 24 {
                break;
            }
            if seen.insert(item.insert_text.clone()) {
                merged.push(item);
            }
        }
        for item in response.items {
            if merged.len() >= 24 {
                break;
            }
            if seen.insert(item.insert_text.clone()) {
                merged.push(item);
            }
        }
        response.items = merged;
        return Ok(serde_json::to_string(&response)?);
    }
    Ok(AUTOCOMPLETE_SERVICE
        .complete(request)
        .await
        .and_then(|response| {
            serde_json::to_string(&response)
                .map_err(|error| PortixError::InvalidRequest(error.to_string()))
        })?)
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

pub async fn read_remote_file(session_id: String, path: String) -> anyhow::Result<String> {
    Ok(SESSION_MANAGER.read_remote_file(session_id, path).await?)
}

pub async fn read_remote_file_bytes(session_id: String, path: String) -> anyhow::Result<Vec<u8>> {
    Ok(SESSION_MANAGER
        .read_remote_file_bytes(session_id, path)
        .await?)
}

pub async fn write_remote_file(
    session_id: String,
    path: String,
    content: String,
) -> anyhow::Result<()> {
    Ok(SESSION_MANAGER
        .write_remote_file(session_id, path, content)
        .await?)
}

pub async fn upload_remote_file(
    session_id: String,
    path: String,
    data: Vec<u8>,
) -> anyhow::Result<()> {
    Ok(SESSION_MANAGER
        .upload_remote_file(session_id, path, data)
        .await?)
}

pub async fn create_remote_directory(session_id: String, path: String) -> anyhow::Result<()> {
    Ok(SESSION_MANAGER
        .create_remote_directory(session_id, path)
        .await?)
}

pub async fn create_remote_file(session_id: String, path: String) -> anyhow::Result<()> {
    Ok(SESSION_MANAGER.create_remote_file(session_id, path).await?)
}

pub async fn chmod_remote_path(
    session_id: String,
    path: String,
    mode: String,
) -> anyhow::Result<()> {
    Ok(SESSION_MANAGER
        .chmod_remote_path(session_id, path, mode)
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
