use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ConnectionStatus {
    Disconnected,
    Connecting,
    Connected,
    Error,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SessionInfo {
    pub id: String,
    pub profile_id: String,
    pub status: ConnectionStatus,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RemoteSystemSnapshot {
    pub os: String,
    pub hostname: String,
    pub uptime: String,
    pub memory: String,
    pub disk: String,
    pub memory_used_bytes: u64,
    pub memory_free_bytes: u64,
    pub memory_total_bytes: u64,
    pub disk_used_bytes: u64,
    pub disk_free_bytes: u64,
    pub disk_total_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RemoteFileEntry {
    pub name: String,
    pub path: String,
    pub is_directory: bool,
    pub size_bytes: u64,
    pub modified_unix_seconds: i64,
}
