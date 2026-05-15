use std::collections::HashMap;
use std::sync::Arc;

use base64::{Engine as _, engine::general_purpose};
use tokio::sync::{RwLock, broadcast, mpsc, oneshot};
use uuid::Uuid;

use crate::domain::errors::{PortixError, Result};
use crate::domain::events::{ConnectionStatusEvent, ErrorEvent, TerminalOutputEvent};
use crate::domain::profile::SshProfile;
use crate::domain::session::{
    ConnectionStatus, RemoteFileEntry, RemoteSystemSnapshot, SessionInfo,
};
use crate::infrastructure::ssh_client::{SshCommand, SshRuntime};

#[derive(Clone)]
pub struct SessionManager {
    sessions: Arc<RwLock<HashMap<String, ManagedSession>>>,
    output_tx: broadcast::Sender<TerminalOutputEvent>,
    status_tx: broadcast::Sender<ConnectionStatusEvent>,
    error_tx: broadcast::Sender<ErrorEvent>,
}

#[derive(Clone)]
struct ManagedSession {
    command_tx: mpsc::Sender<SshCommand>,
}

impl SessionManager {
    pub fn new() -> Self {
        let (output_tx, _) = broadcast::channel(1024);
        let (status_tx, _) = broadcast::channel(256);
        let (error_tx, _) = broadcast::channel(256);
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            output_tx,
            status_tx,
            error_tx,
        }
    }

    pub fn terminal_output_stream(&self) -> broadcast::Receiver<TerminalOutputEvent> {
        self.output_tx.subscribe()
    }

    pub fn connection_status_stream(&self) -> broadcast::Receiver<ConnectionStatusEvent> {
        self.status_tx.subscribe()
    }

    pub fn error_event_stream(&self) -> broadcast::Receiver<ErrorEvent> {
        self.error_tx.subscribe()
    }

    pub async fn connect(&self, profile: SshProfile, cols: u32, rows: u32) -> Result<SessionInfo> {
        profile.validate()?;

        let session_id = Uuid::new_v4().to_string();
        let (command_tx, command_rx) = mpsc::channel(512);
        let info = SessionInfo {
            id: session_id.clone(),
            profile_id: profile.id.clone(),
            status: ConnectionStatus::Connecting,
        };

        self.sessions
            .write()
            .await
            .insert(session_id.clone(), ManagedSession { command_tx });
        self.emit_status(
            &session_id,
            ConnectionStatus::Connecting,
            Some("connecting"),
        );

        let sessions = self.sessions.clone();
        let output_tx = self.output_tx.clone();
        let status_tx = self.status_tx.clone();
        let error_tx = self.error_tx.clone();
        tokio::spawn(async move {
            let mut final_status = ConnectionStatus::Disconnected;
            let mut final_message = None;
            let runtime = SshRuntime::new(
                profile,
                session_id.clone(),
                output_tx,
                status_tx.clone(),
                error_tx.clone(),
            );
            let result = runtime.run(command_rx, cols, rows).await;
            if let Err(error) = result {
                final_status = ConnectionStatus::Error;
                final_message = Some(error.to_string());
                let _ = error_tx.send(ErrorEvent {
                    session_id: Some(session_id.clone()),
                    message: error.to_string(),
                });
            }
            sessions.write().await.remove(&session_id);
            let _ = status_tx.send(ConnectionStatusEvent {
                session_id,
                status: final_status,
                message: final_message,
            });
        });

        Ok(info)
    }

    pub async fn disconnect(&self, session_id: String) -> Result<()> {
        let session = self.session(&session_id).await?;
        session
            .command_tx
            .send(SshCommand::Disconnect)
            .await
            .map_err(|_| PortixError::SessionNotFound(session_id.clone()))?;
        Ok(())
    }

    pub async fn send_terminal_input(&self, session_id: String, data: Vec<u8>) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let session = self.session(&session_id).await?;
        session
            .command_tx
            .send(SshCommand::Input(data))
            .await
            .map_err(|_| PortixError::SessionNotFound(session_id.clone()))?;
        Ok(())
    }

    pub async fn resize_terminal(&self, session_id: String, cols: u32, rows: u32) -> Result<()> {
        let session = self.session(&session_id).await?;
        session
            .command_tx
            .send(SshCommand::Resize { cols, rows })
            .await
            .map_err(|_| PortixError::SessionNotFound(session_id.clone()))?;
        Ok(())
    }

    pub async fn remote_system_snapshot(&self, session_id: String) -> Result<RemoteSystemSnapshot> {
        let output = self.exec(session_id, remote_system_command()).await?;
        Ok(parse_remote_system_snapshot(&output))
    }

    pub async fn list_remote_directory(
        &self,
        session_id: String,
        path: String,
    ) -> Result<Vec<RemoteFileEntry>> {
        let command = list_directory_command(&path);
        let output = self.exec(session_id, command).await?;
        Ok(parse_remote_directory(&path, &output))
    }

    pub async fn resolve_remote_directory(
        &self,
        session_id: String,
        path: String,
    ) -> Result<String> {
        let command = resolve_directory_command(&path);
        let output = self.exec(session_id, command).await?;
        Ok(resolve_directory_from_output(&path, &output))
    }

    pub async fn read_remote_file(&self, session_id: String, path: String) -> Result<String> {
        let bytes = self.read_remote_file_bytes(session_id, path).await?;
        Ok(String::from_utf8_lossy(&bytes).to_string())
    }

    pub async fn read_remote_file_bytes(
        &self,
        session_id: String,
        path: String,
    ) -> Result<Vec<u8>> {
        let output = self.exec(session_id, read_file_command(&path)).await?;
        let normalized = output
            .lines()
            .filter(|line| !line.trim().is_empty())
            .collect::<String>();
        Ok(general_purpose::STANDARD
            .decode(normalized.as_bytes())
            .unwrap_or_default())
    }

    pub async fn write_remote_file(
        &self,
        session_id: String,
        path: String,
        content: String,
    ) -> Result<()> {
        let data = content.into_bytes();
        self.upload_remote_file(session_id, path, data).await
    }

    pub async fn upload_remote_file(
        &self,
        session_id: String,
        path: String,
        data: Vec<u8>,
    ) -> Result<()> {
        self.exec(session_id, write_file_command(&path, &data))
            .await?;
        Ok(())
    }

    pub async fn create_remote_directory(&self, session_id: String, path: String) -> Result<()> {
        self.exec(session_id, create_directory_command(&path))
            .await?;
        Ok(())
    }

    pub async fn create_remote_file(&self, session_id: String, path: String) -> Result<()> {
        self.exec(session_id, write_file_command(&path, &[]))
            .await?;
        Ok(())
    }

    pub async fn chmod_remote_path(
        &self,
        session_id: String,
        path: String,
        mode: String,
    ) -> Result<()> {
        let trimmed = mode.trim();
        if trimmed.len() != 3 && trimmed.len() != 4 {
            return Err(PortixError::InvalidProfile(
                "chmod mode must be 3 or 4 octal digits".to_owned(),
            ));
        }
        if !trimmed.chars().all(|char| matches!(char, '0'..='7')) {
            return Err(PortixError::InvalidProfile(
                "chmod mode must contain only octal digits".to_owned(),
            ));
        }
        self.exec(session_id, chmod_command(&path, trimmed)).await?;
        Ok(())
    }

    async fn exec(&self, session_id: String, command: String) -> Result<String> {
        let session = self.session(&session_id).await?;
        let (response_tx, response_rx) = oneshot::channel();
        session
            .command_tx
            .send(SshCommand::Exec {
                command,
                response_tx,
            })
            .await
            .map_err(|_| PortixError::SessionNotFound(session_id.clone()))?;
        response_rx
            .await
            .map_err(|_| PortixError::SessionNotFound(session_id))?
    }

    async fn session(&self, session_id: &str) -> Result<ManagedSession> {
        self.sessions
            .read()
            .await
            .get(session_id)
            .cloned()
            .ok_or_else(|| PortixError::SessionNotFound(session_id.to_owned()))
    }

    fn emit_status(&self, session_id: &str, status: ConnectionStatus, message: Option<&str>) {
        let _ = self.status_tx.send(ConnectionStatusEvent {
            session_id: session_id.to_owned(),
            status,
            message: message.map(str::to_owned),
        });
    }
}

fn remote_system_command() -> String {
    r#"if [ -r /etc/redhat-release ]; then
  os="$(cat /etc/redhat-release 2>/dev/null)"
else
  os="$(uname -srm 2>/dev/null)"
fi
printf 'OS=%s\n' "$os"
printf 'HOST=%s\n' "$(hostname 2>/dev/null)"
printf 'UPTIME=%s\n' "$(uptime 2>/dev/null)"
awk '
  /MemTotal:/ {total=$2 * 1024}
  /MemFree:/ {mem_free=$2 * 1024}
  /Buffers:/ {buffers=$2 * 1024}
  /^Cached:/ {cached=$2 * 1024}
  /MemAvailable:/ {available=$2 * 1024}
  END {
    if (available == 0) available = mem_free + buffers + cached
    used = total - available
    if (used < 0) used = 0
    printf "MEM=%s / %s\n", human(used), human(total)
    printf "MEM_USED_BYTES=%.0f\nMEM_FREE_BYTES=%.0f\nMEM_TOTAL_BYTES=%.0f\n", used, available, total
  }
  function human(bytes, value, unit) {
    value = bytes
    unit = "B"
    if (value >= 1024) { value = value / 1024; unit = "KB" }
    if (value >= 1024) { value = value / 1024; unit = "MB" }
    if (value >= 1024) { value = value / 1024; unit = "GB" }
    return sprintf("%.1f %s", value, unit)
  }
' /proc/meminfo 2>/dev/null
df -P -k / 2>/dev/null | awk '
  NR==2 {
    total=$2 * 1024
    used=$3 * 1024
    free=$4 * 1024
    printf "DISK=%s free / %s\n", human(free), human(total)
    printf "DISK_USED_BYTES=%.0f\nDISK_FREE_BYTES=%.0f\nDISK_TOTAL_BYTES=%.0f\n", used, free, total
  }
  function human(bytes, value, unit) {
    value = bytes
    unit = "B"
    if (value >= 1024) { value = value / 1024; unit = "KB" }
    if (value >= 1024) { value = value / 1024; unit = "MB" }
    if (value >= 1024) { value = value / 1024; unit = "GB" }
    return sprintf("%.1f %s", value, unit)
  }
'
true
"#
    .to_owned()
}

fn list_directory_command(path: &str) -> String {
    let quoted = shell_quote(path);
    format!(
        r#"p={quoted}
if [ -d "$p" ]; then
  if find "$p" -mindepth 1 -maxdepth 1 -printf '%y\t%s\t%T@\t%f\t%p\n' >/tmp/portix_ls_$$ 2>/dev/null; then
    cat /tmp/portix_ls_$$
    rm -f /tmp/portix_ls_$$
  else
    rm -f /tmp/portix_ls_$$
    for item in "$p"/.[!.]* "$p"/..?* "$p"/*; do
      [ -e "$item" ] || continue
      name=$(basename "$item")
      modified=$(stat -c %Y "$item" 2>/dev/null || stat -f %m "$item" 2>/dev/null || printf 0)
      if [ -d "$item" ]; then
        printf 'd\t0\t%s\t%s\t%s\n' "$modified" "$name" "$item"
      else
        size=$(wc -c < "$item" 2>/dev/null || printf 0)
        printf 'f\t%s\t%s\t%s\t%s\n' "$size" "$modified" "$name" "$item"
      fi
    done
  fi
fi
"#
    )
}

fn resolve_directory_command(path: &str) -> String {
    let quoted = shell_quote(path);
    format!(
        r#"p={quoted}
if [ -d "$p" ]; then
  (cd "$p" 2>/dev/null && pwd -P) || printf '%s\n' "$p"
else
  parent=$(dirname "$p")
  for item in "$parent"/* "$parent"/.[!.]* "$parent"/..?*; do
    [ -d "$item" ] || continue
    printf '%s\t%s\n' "$(basename "$item")" "$item"
  done
fi
"#
    )
}

fn read_file_command(path: &str) -> String {
    let quoted = shell_quote(path);
    format!(
        r#"p={quoted}
if [ -f "$p" ] && [ -r "$p" ]; then
  if command -v base64 >/dev/null 2>&1; then
    base64 "$p"
  else
    openssl base64 -in "$p"
  fi
fi
"#
    )
}

fn write_file_command(path: &str, data: &[u8]) -> String {
    let quoted = shell_quote(path);
    let encoded = general_purpose::STANDARD.encode(data);
    format!(
        r#"p={quoted}
mkdir -p "$(dirname "$p")"
cat > "$p".portix.b64 <<'PORTIX_FILE'
{encoded}
PORTIX_FILE
if command -v base64 >/dev/null 2>&1; then
  base64 -d "$p".portix.b64 > "$p" 2>/dev/null || base64 --decode "$p".portix.b64 > "$p"
else
  openssl base64 -d -in "$p".portix.b64 -out "$p"
fi
rm -f "$p".portix.b64
"#
    )
}

fn create_directory_command(path: &str) -> String {
    let quoted = shell_quote(path);
    format!("mkdir -p {quoted}\n")
}

fn chmod_command(path: &str, mode: &str) -> String {
    let quoted = shell_quote(path);
    format!("chmod {mode} {quoted}\n")
}

fn shell_quote(value: &str) -> String {
    if value.trim().is_empty() || value == "~" {
        return "\"$HOME\"".to_owned();
    }
    if value == "." {
        return "\".\"".to_owned();
    }
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn parse_remote_system_snapshot(output: &str) -> RemoteSystemSnapshot {
    fn value<'a>(output: &'a str, key: &str) -> &'a str {
        output
            .lines()
            .find_map(|line| line.strip_prefix(key))
            .unwrap_or("")
            .trim()
    }

    RemoteSystemSnapshot {
        os: value(output, "OS=").to_owned(),
        hostname: value(output, "HOST=").to_owned(),
        uptime: value(output, "UPTIME=").to_owned(),
        memory: value(output, "MEM=").to_owned(),
        disk: value(output, "DISK=").to_owned(),
        memory_used_bytes: value(output, "MEM_USED_BYTES=").parse().unwrap_or(0),
        memory_free_bytes: value(output, "MEM_FREE_BYTES=").parse().unwrap_or(0),
        memory_total_bytes: value(output, "MEM_TOTAL_BYTES=").parse().unwrap_or(0),
        disk_used_bytes: value(output, "DISK_USED_BYTES=").parse().unwrap_or(0),
        disk_free_bytes: value(output, "DISK_FREE_BYTES=").parse().unwrap_or(0),
        disk_total_bytes: value(output, "DISK_TOTAL_BYTES=").parse().unwrap_or(0),
    }
}

fn parse_remote_directory(base_path: &str, output: &str) -> Vec<RemoteFileEntry> {
    let mut entries = output
        .lines()
        .filter_map(|line| {
            let mut parts = line.splitn(5, '\t');
            let kind = parts.next()?;
            let size = parts.next()?.parse::<u64>().unwrap_or(0);
            let modified_unix_seconds = parts
                .next()?
                .split('.')
                .next()
                .unwrap_or("0")
                .parse::<i64>()
                .unwrap_or(0);
            let name = parts.next()?.to_owned();
            let path = parts.next()?.to_owned();
            Some(RemoteFileEntry {
                name,
                path,
                is_directory: kind == "d" || kind == "dir",
                size_bytes: size,
                modified_unix_seconds,
            })
        })
        .collect::<Vec<_>>();
    entries.sort_by(|a, b| {
        b.is_directory
            .cmp(&a.is_directory)
            .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
    });
    if entries.is_empty() && !base_path.is_empty() {
        return entries;
    }
    entries
}

fn resolve_directory_from_output(requested_path: &str, output: &str) -> String {
    let trimmed = output.trim();
    if trimmed.is_empty() {
        return requested_path.to_owned();
    }
    if !trimmed.contains('\t') {
        return trimmed
            .lines()
            .next()
            .unwrap_or(requested_path)
            .trim()
            .to_owned();
    }

    let requested_name = requested_path
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or(requested_path)
        .to_lowercase();
    if requested_name.is_empty() {
        return requested_path.to_owned();
    }

    let candidates = output
        .lines()
        .filter_map(|line| {
            let (name, path) = line.split_once('\t')?;
            Some((name.to_lowercase(), path.trim().to_owned()))
        })
        .collect::<Vec<_>>();

    unique_match(
        candidates
            .iter()
            .filter(|(name, _)| name.starts_with(&requested_name))
            .map(|(_, path)| path),
    )
    .or_else(|| {
        unique_match(
            candidates
                .iter()
                .filter(|(name, _)| fuzzy_subsequence_match(&requested_name, name))
                .map(|(_, path)| path),
        )
    })
    .cloned()
    .unwrap_or_else(|| requested_path.to_owned())
}

fn unique_match<'a>(mut paths: impl Iterator<Item = &'a String>) -> Option<&'a String> {
    let first = paths.next()?;
    if paths.next().is_some() {
        return None;
    }
    Some(first)
}

fn fuzzy_subsequence_match(needle: &str, haystack: &str) -> bool {
    if needle.is_empty() {
        return false;
    }
    let mut wanted = needle.chars();
    let mut current = wanted.next();
    for candidate in haystack.chars() {
        if Some(candidate) == current {
            current = wanted.next();
            if current.is_none() {
                return true;
            }
        }
    }
    false
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_directory_supports_unique_prefix_directory_match() {
        let output = "igate-core\t/opt/igate-core\nlogs\t/opt/logs\n";

        assert_eq!(
            resolve_directory_from_output("/opt/igate", output),
            "/opt/igate-core"
        );
    }

    #[test]
    fn resolve_directory_supports_unique_fuzzy_directory_match() {
        let output = "igate-core\t/opt/igate-core\nigloo\t/opt/igloo\n";

        assert_eq!(
            resolve_directory_from_output("/opt/igc", output),
            "/opt/igate-core"
        );
    }

    #[test]
    fn resolve_directory_keeps_requested_path_when_fuzzy_match_is_ambiguous() {
        let output = "igate-core\t/opt/igate-core\nignore-cache\t/opt/ignore-cache\n";

        assert_eq!(
            resolve_directory_from_output("/opt/igc", output),
            "/opt/igc"
        );
    }
}
