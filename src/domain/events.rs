use serde::{Deserialize, Serialize};

use super::session::ConnectionStatus;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TerminalOutputEvent {
    pub session_id: String,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ConnectionStatusEvent {
    pub session_id: String,
    pub status: ConnectionStatus,
    pub message: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ErrorEvent {
    pub session_id: Option<String>,
    pub message: String,
}
