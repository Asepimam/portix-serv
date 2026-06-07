use thiserror::Error;

#[derive(Debug, Error)]
pub enum PortixError {
    #[error("invalid profile: {0}")]
    InvalidProfile(String),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("session not found: {0}")]
    SessionNotFound(String),
    #[error("authentication failed")]
    AuthenticationFailed,
    #[error("missing authentication method")]
    MissingAuthentication,
    #[error("connection timed out")]
    ConnectionTimeout,
    #[error("authentication timed out")]
    AuthenticationTimeout,
    #[error("remote command timed out")]
    CommandTimeout,
    #[error(transparent)]
    Russh(#[from] russh::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, PortixError>;
