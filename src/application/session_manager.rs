use std::collections::HashMap;
use std::sync::Arc;

use base64::{Engine as _, engine::general_purpose};
use tokio::sync::{RwLock, broadcast, mpsc, oneshot};
use tokio::time::{Duration, timeout};
use uuid::Uuid;

use crate::domain::autocomplete::{
    CompletionItem, CompletionKind, TerminalCompleteRequest, TerminalCompleteResponse,
};
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

const COMPLETION_TIMEOUT: Duration = Duration::from_secs(1);
const UPLOAD_BASE64_CHUNK_SIZE: usize = 12 * 1024;

#[derive(Clone)]
struct ManagedSession {
    command_tx: mpsc::Sender<SshCommand>,
    /// None = not yet detected, Some(platform) = cached
    remote_platform: Arc<RwLock<Option<RemotePlatform>>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RemotePlatform {
    Unix,
    WindowsCmd,
    WindowsPowerShell,
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

        self.sessions.write().await.insert(
            session_id.clone(),
            ManagedSession {
                command_tx,
                remote_platform: Arc::new(RwLock::new(None)),
            },
        );
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
        let is_windows = self.is_remote_windows(&session_id).await;
        let command = if is_windows {
            remote_system_command_windows()
        } else {
            remote_system_command()
        };
        let output = self.exec(session_id, command).await?;
        Ok(parse_remote_system_snapshot(&output))
    }

    pub async fn command_help_suggestions(
        &self,
        session_id: String,
        input: String,
    ) -> Result<Vec<String>> {
        let Some(request) = HelpSuggestionRequest::parse(&input) else {
            return Ok(Vec::new());
        };

        if !is_allowed_help_command(request.command) {
            return Ok(Vec::new());
        }

        if request.completing_command {
            let command = command_name_suggestions_command(request.current_token);
            let output = timeout(COMPLETION_TIMEOUT, self.exec(session_id, command))
                .await
                .map_err(|_| PortixError::CommandTimeout)??;
            return Ok(parse_command_name_suggestions(&input, &request, &output));
        }

        let command = help_suggestions_command(&request.help_tokens);
        let output = timeout(COMPLETION_TIMEOUT, self.exec(session_id, command))
            .await
            .map_err(|_| PortixError::CommandTimeout)??;
        Ok(parse_help_suggestions(&input, request, &output))
    }

    pub async fn terminal_complete(
        &self,
        request: TerminalCompleteRequest,
    ) -> Result<TerminalCompleteResponse> {
        let Some(session_id) = request.session_id.clone() else {
            return Ok(TerminalCompleteResponse {
                suggestion: None,
                items: Vec::new(),
            });
        };
        let input = request.prefix();
        let max_items = request.max_items();
        let suggestions = match self
            .command_help_suggestions(session_id.clone(), input.clone())
            .await
        {
            Ok(suggestions) => suggestions,
            Err(PortixError::CommandTimeout) => Vec::new(),
            Err(error) => return Err(error),
        };
        let suggestion = self
            .remote_history_suggestion(session_id, input)
            .await
            .unwrap_or(None);
        let mut items = suggestions
            .into_iter()
            .filter_map(|wire| completion_item_from_wire(&wire))
            .take(max_items)
            .collect::<Vec<_>>();
        items.truncate(max_items);
        Ok(TerminalCompleteResponse { suggestion, items })
    }

    async fn remote_history_suggestion(
        &self,
        session_id: String,
        prefix: String,
    ) -> Result<Option<String>> {
        let prefix = prefix.trim_start().to_owned();
        if prefix.len() < 3 || contains_shell_control(&prefix) || is_sensitive_autocomplete(&prefix)
        {
            return Ok(None);
        }
        let command = remote_history_suggestion_command(&prefix);
        let output = timeout(COMPLETION_TIMEOUT, self.exec(session_id, command))
            .await
            .map_err(|_| PortixError::CommandTimeout)??;
        let suffix = output.trim();
        if suffix.is_empty() || is_sensitive_autocomplete(suffix) {
            return Ok(None);
        }
        Ok(Some(suffix.to_owned()))
    }

    pub async fn list_remote_directory(
        &self,
        session_id: String,
        path: String,
    ) -> Result<Vec<RemoteFileEntry>> {
        let is_windows = self.is_remote_windows(&session_id).await;
        let command = if is_windows {
            list_directory_command_windows(&path)
        } else {
            list_directory_command(&path)
        };
        let output = self.exec(session_id, command).await?;
        Ok(parse_remote_directory(&path, &output))
    }

    pub async fn resolve_remote_directory(
        &self,
        session_id: String,
        path: String,
    ) -> Result<String> {
        let is_windows = self.is_remote_windows(&session_id).await;
        let command = if is_windows {
            resolve_directory_command_windows(&path)
        } else {
            resolve_directory_command(&path)
        };
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
        let is_windows = self.is_remote_windows(&session_id).await;
        let command = if is_windows {
            read_file_command_windows(&path)
        } else {
            read_file_command(&path)
        };
        let output = self.exec(session_id, command).await?;
        let encoded = extract_portix_payload(&output, "PORTIX_FILE_BEGIN", "PORTIX_FILE_END")?;
        general_purpose::STANDARD
            .decode(encoded.as_bytes())
            .map_err(|error| {
                PortixError::InvalidRequest(format!("remote download decode failed: {error}"))
            })
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
        let is_windows = self.is_remote_windows(&session_id).await;
        if is_windows {
            return self
                .upload_remote_file_windows(session_id, path, data)
                .await;
        }
        let encoded = general_purpose::STANDARD.encode(&data);
        self.exec(session_id.clone(), begin_file_upload_command(&path))
            .await?;
        for chunk in encoded.as_bytes().chunks(UPLOAD_BASE64_CHUNK_SIZE) {
            let chunk = std::str::from_utf8(chunk).map_err(|error| {
                PortixError::InvalidRequest(format!("invalid upload chunk: {error}"))
            })?;
            self.exec(
                session_id.clone(),
                append_file_upload_chunk_command(&path, chunk),
            )
            .await?;
        }
        let output = self
            .exec(session_id, finish_file_upload_command(&path, data.len()))
            .await?;
        if !output.lines().any(|line| line.trim() == "PORTIX_UPLOAD_OK") {
            let detail = output
                .lines()
                .rev()
                .find(|line| !line.trim().is_empty())
                .map(str::trim)
                .unwrap_or("remote did not confirm upload");
            return Err(PortixError::InvalidRequest(format!(
                "remote upload failed: {detail}"
            )));
        }
        Ok(())
    }

    async fn upload_remote_file_windows(
        &self,
        session_id: String,
        path: String,
        data: Vec<u8>,
    ) -> Result<()> {
        let encoded = general_purpose::STANDARD.encode(&data);
        let escaped_path = path.replace('\'', "''");
        let temp_path = format!("{}.portix.b64", &path);
        let escaped_temp = temp_path.replace('\'', "''");

        // Clear temp file
        let clear_script = format!(
            "Set-Content -Path '{}' -Value '' -NoNewline",
            escaped_temp
        );
        self.exec(session_id.clone(), encode_powershell_command(&clear_script))
            .await?;

        // Write chunks
        for chunk in encoded.as_bytes().chunks(UPLOAD_BASE64_CHUNK_SIZE) {
            let chunk_str = std::str::from_utf8(chunk).map_err(|error| {
                PortixError::InvalidRequest(format!("invalid upload chunk: {error}"))
            })?;
            let append_script = format!(
                "Add-Content -Path '{}' -Value '{}' -NoNewline",
                escaped_temp, chunk_str
            );
            self.exec(session_id.clone(), encode_powershell_command(&append_script))
                .await?;
        }

        // Decode and write final file
        let finish_script = format!(
            r#"
$b64 = Get-Content -Path '{escaped_temp}' -Raw
$bytes = [Convert]::FromBase64String($b64)
[IO.File]::WriteAllBytes('{escaped_path}', $bytes)
Remove-Item -Path '{escaped_temp}' -Force -ErrorAction SilentlyContinue
if ((Get-Item '{escaped_path}').Length -eq {expected}) {{
  Write-Output 'PORTIX_UPLOAD_OK'
}} else {{
  Write-Output ('PORTIX_UPLOAD_ERR size mismatch expected={expected} actual=' + (Get-Item '{escaped_path}').Length)
}}
"#,
            expected = data.len()
        );
        let output = self
            .exec(session_id, encode_powershell_command(&finish_script))
            .await?;
        if !output.lines().any(|line| line.trim() == "PORTIX_UPLOAD_OK") {
            let detail = output
                .lines()
                .rev()
                .find(|line| !line.trim().is_empty())
                .map(str::trim)
                .unwrap_or("remote did not confirm upload");
            return Err(PortixError::InvalidRequest(format!(
                "remote upload failed: {detail}"
            )));
        }
        Ok(())
    }

    pub async fn create_remote_directory(&self, session_id: String, path: String) -> Result<()> {
        let is_windows = self.is_remote_windows(&session_id).await;
        let command = if is_windows {
            let escaped = path.replace('\'', "''");
            encode_powershell_command(&format!(
                "New-Item -ItemType Directory -Path '{}' -Force | Out-Null",
                escaped
            ))
        } else {
            create_directory_command(&path)
        };
        self.exec(session_id, command).await?;
        Ok(())
    }

    pub async fn create_remote_file(&self, session_id: String, path: String) -> Result<()> {
        let is_windows = self.is_remote_windows(&session_id).await;
        if is_windows {
            let escaped = path.replace('\'', "''");
            self.exec(
                session_id,
                encode_powershell_command(&format!(
                    "New-Item -ItemType File -Path '{}' -Force | Out-Null",
                    escaped
                )),
            )
            .await?;
        } else {
            self.exec(session_id, write_file_command(&path, &[]))
                .await?;
        }
        Ok(())
    }

    pub async fn chmod_remote_path(
        &self,
        session_id: String,
        path: String,
        mode: String,
    ) -> Result<()> {
        // chmod is not available on Windows
        if self.is_remote_windows(&session_id).await {
            return Ok(());
        }
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

    /// Detect if the remote host is Windows.
    /// Works for both cmd.exe and PowerShell default shells.
    /// Result is cached per session to avoid repeated detection.
    async fn is_remote_windows(&self, session_id: &str) -> bool {
        let platform = self.detect_remote_platform(session_id).await;
        matches!(
            platform,
            RemotePlatform::WindowsCmd | RemotePlatform::WindowsPowerShell
        )
    }

    async fn detect_remote_platform(&self, session_id: &str) -> RemotePlatform {
        // Check cache first
        if let Ok(session) = self.session(session_id).await {
            let cached = session.remote_platform.read().await;
            if let Some(platform) = *cached {
                return platform;
            }
        }

        // Detection strategy:
        // 1. Try `echo $PSVersionTable.PSVersion` — if it returns a version, it's PowerShell
        // 2. Try `echo %OS%` — if it returns Windows_NT, it's cmd.exe
        // 3. Otherwise it's Unix
        let result = self
            .exec(
                session_id.to_owned(),
                "echo PORTIX_DETECT && echo %OS% && echo $env:OS".to_owned(),
            )
            .await;

        let platform = match result {
            Ok(output) => {
                let lines: Vec<&str> = output.lines().map(str::trim).collect();
                // Check if %OS% was expanded (cmd.exe) or $env:OS returned value (PowerShell)
                let has_windows_nt = lines
                    .iter()
                    .any(|line| line.contains("Windows_NT"));
                let has_unexpanded_percent = lines
                    .iter()
                    .any(|line| *line == "%OS%");
                let _has_unexpanded_env = lines
                    .iter()
                    .any(|line| *line == "$env:OS");

                if has_windows_nt && !has_unexpanded_percent {
                    // %OS% expanded to Windows_NT → cmd.exe shell
                    RemotePlatform::WindowsCmd
                } else if has_windows_nt && has_unexpanded_percent {
                    // $env:OS returned Windows_NT but %OS% didn't expand → PowerShell
                    RemotePlatform::WindowsPowerShell
                } else {
                    RemotePlatform::Unix
                }
            }
            Err(_) => RemotePlatform::Unix,
        };

        // Store in cache
        if let Ok(session) = self.session(session_id).await {
            let mut cached = session.remote_platform.write().await;
            *cached = Some(platform);
        }

        platform
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
if [ "$(uname -s 2>/dev/null)" = "Darwin" ]; then
  _mem_total=$(sysctl -n hw.memsize 2>/dev/null)
  _page_size=$(sysctl -n hw.pagesize 2>/dev/null || echo 4096)
  _vm_stat=$(vm_stat 2>/dev/null)
  _pages_free=$(echo "$_vm_stat" | awk '/Pages free:/ {gsub(/\./,"",$3); print $3}')
  _pages_inactive=$(echo "$_vm_stat" | awk '/Pages inactive:/ {gsub(/\./,"",$3); print $3}')
  _pages_purgeable=$(echo "$_vm_stat" | awk '/Pages purgeable:/ {gsub(/\./,"",$3); print $3}')
  _mem_free=$(( (_pages_free + _pages_inactive + _pages_purgeable) * _page_size ))
  _mem_used=$(( _mem_total - _mem_free ))
  if [ "$_mem_used" -lt 0 ] 2>/dev/null; then _mem_used=0; fi
  printf "MEM_USED_BYTES=%s\nMEM_FREE_BYTES=%s\nMEM_TOTAL_BYTES=%s\n" "$_mem_used" "$_mem_free" "$_mem_total"
else
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
      printf "MEM_USED_BYTES=%.0f\nMEM_FREE_BYTES=%.0f\nMEM_TOTAL_BYTES=%.0f\n", used, available, total
    }
  ' /proc/meminfo 2>/dev/null
fi
df -P -k / 2>/dev/null | awk '
  NR==2 {
    total=$2 * 1024
    used=$3 * 1024
    free=$4 * 1024
    printf "DISK_USED_BYTES=%.0f\nDISK_FREE_BYTES=%.0f\nDISK_TOTAL_BYTES=%.0f\n", used, free, total
  }
'
true
"#
    .to_owned()
}

fn remote_system_command_windows() -> String {
    // Use PowerShell encoded command to avoid quoting issues with cmd.exe wrapping.
    let ps_script = r#"
$os = (Get-CimInstance Win32_OperatingSystem).Caption
$host_ = $env:COMPUTERNAME
$upObj = (Get-CimInstance Win32_OperatingSystem).LastBootUpTime
$up = (New-TimeSpan -Start $upObj -End (Get-Date))
$upStr = '{0}d {1}h {2}m' -f $up.Days, $up.Hours, $up.Minutes
$mem = Get-CimInstance Win32_OperatingSystem
$memTotal = $mem.TotalVisibleMemorySize * 1024
$memFree = $mem.FreePhysicalMemory * 1024
$memUsed = $memTotal - $memFree
$disk = Get-CimInstance Win32_LogicalDisk -Filter "DeviceID='C:'"
$diskTotal = $disk.Size
$diskFree = $disk.FreeSpace
$diskUsed = $diskTotal - $diskFree
Write-Output "OS=$os"
Write-Output "HOST=$host_"
Write-Output "UPTIME=$upStr"
Write-Output "MEM_USED_BYTES=$([math]::Max(0, $memUsed))"
Write-Output "MEM_FREE_BYTES=$([math]::Max(0, $memFree))"
Write-Output "MEM_TOTAL_BYTES=$([math]::Max(0, $memTotal))"
Write-Output "DISK_USED_BYTES=$([math]::Max(0, $diskUsed))"
Write-Output "DISK_FREE_BYTES=$([math]::Max(0, $diskFree))"
Write-Output "DISK_TOTAL_BYTES=$([math]::Max(0, $diskTotal))"
"#;
    encode_powershell_command(ps_script)
}

/// Encode a PowerShell script as a base64 UTF-16LE encoded command.
/// This avoids all quoting/escaping issues when launching via cmd.exe or PowerShell.
fn encode_powershell_command(script: &str) -> String {
    let utf16: Vec<u8> = script
        .encode_utf16()
        .flat_map(|c| c.to_le_bytes())
        .collect();
    let encoded = general_purpose::STANDARD.encode(&utf16);
    format!("powershell -NoProfile -EncodedCommand {encoded}")
}

fn list_directory_command_windows(path: &str) -> String {
    let ps_script = format!(
        r#"
$p = '{}'
if (Test-Path $p -PathType Container) {{
  Get-ChildItem -Path $p -Force | ForEach-Object {{
    $kind = if ($_.PSIsContainer) {{ 'd' }} else {{ 'f' }}
    $size = if ($_.PSIsContainer) {{ 0 }} else {{ $_.Length }}
    $mod = [int][double]::Parse((Get-Date $_.LastWriteTimeUtc -UFormat '%s'))
    $name = $_.Name
    $full = $_.FullName
    Write-Output "$kind`t$size`t$mod`t$name`t$full"
  }}
}}
"#,
        path.replace('\'', "''")
    );
    encode_powershell_command(&ps_script)
}

fn resolve_directory_command_windows(path: &str) -> String {
    let ps_script = format!(
        r#"
$p = '{}'
if (Test-Path $p -PathType Container) {{
  Write-Output (Resolve-Path $p).Path
}} else {{
  $parent = Split-Path $p -Parent
  Get-ChildItem -Path $parent -Directory -Force | ForEach-Object {{
    Write-Output "$($_.Name)`t$($_.FullName)"
  }}
}}
"#,
        path.replace('\'', "''")
    );
    encode_powershell_command(&ps_script)
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
    ls -1A "$p" 2>/dev/null | while IFS= read -r name; do
      [ -z "$name" ] && continue
      item="$p/$name"
      modified=$(stat -c %Y "$item" 2>/dev/null || stat -f %m "$item" 2>/dev/null || printf '0')
      if [ -d "$item" ]; then
        printf 'd\t0\t%s\t%s\t%s\n' "$modified" "$name" "$item"
      else
        size=$(stat -c %s "$item" 2>/dev/null || stat -f %z "$item" 2>/dev/null || printf '0')
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
if [ ! -f "$p" ]; then
  printf 'PORTIX_DOWNLOAD_ERR file not found\n'
  exit 1
fi
if [ ! -r "$p" ]; then
  printf 'PORTIX_DOWNLOAD_ERR file not readable\n'
  exit 1
fi
printf 'PORTIX_FILE_BEGIN\n'
if [ -f "$p" ] && [ -r "$p" ]; then
  if command -v base64 >/dev/null 2>&1; then
    base64 "$p"
  else
    openssl base64 -in "$p"
  fi
fi
printf 'PORTIX_FILE_END\n'
"#
    )
}

fn read_file_command_windows(path: &str) -> String {
    let ps_script = format!(
        r#"
$p = '{}'
if (-not (Test-Path $p -PathType Leaf)) {{
  Write-Output 'PORTIX_DOWNLOAD_ERR file not found'
  exit 1
}}
Write-Output 'PORTIX_FILE_BEGIN'
[Convert]::ToBase64String([IO.File]::ReadAllBytes($p))
Write-Output 'PORTIX_FILE_END'
"#,
        path.replace('\'', "''")
    );
    encode_powershell_command(&ps_script)
}

fn write_file_command(path: &str, data: &[u8]) -> String {
    if data.is_empty() {
        return format!(
            "{}{}",
            begin_file_upload_command(path),
            finish_file_upload_command(path, 0),
        );
    }
    let encoded = general_purpose::STANDARD.encode(data);
    format!(
        "{}{}{}",
        begin_file_upload_command(path),
        append_file_upload_chunk_command(path, &encoded),
        finish_file_upload_command(path, data.len()),
    )
}

fn upload_temp_path() -> &'static str {
    "$p.portix.b64"
}

fn begin_file_upload_command(path: &str) -> String {
    let quoted = shell_quote(path);
    format!(
        r#"p={quoted}
tmp="{tmp}"
mkdir -p "$(dirname "$p")" || {{
  printf 'PORTIX_UPLOAD_ERR mkdir failed\n'
  exit 1
}}
: > "$tmp" || {{
  printf 'PORTIX_UPLOAD_ERR temp file failed\n'
  exit 1
}}
"#,
        tmp = upload_temp_path(),
    )
}

fn append_file_upload_chunk_command(path: &str, encoded_chunk: &str) -> String {
    let quoted = shell_quote(path);
    format!(
        r#"p={quoted}
tmp="{tmp}"
cat >> "$tmp" <<'PORTIX_FILE'
{encoded_chunk}
PORTIX_FILE
"#,
        tmp = upload_temp_path(),
    )
}

fn finish_file_upload_command(path: &str, expected_bytes: usize) -> String {
    let quoted = shell_quote(path);
    format!(
        r#"p={quoted}
tmp="{tmp}"
if command -v base64 >/dev/null 2>&1; then
  if ! (base64 -d "$tmp" > "$p" 2>/dev/null || base64 --decode "$tmp" > "$p" 2>/dev/null); then
    rm -f "$tmp"
    printf 'PORTIX_UPLOAD_ERR base64 decode failed\n'
    exit 1
  fi
else
  if ! openssl base64 -d -in "$tmp" -out "$p" 2>/dev/null; then
    rm -f "$tmp"
    printf 'PORTIX_UPLOAD_ERR openssl decode failed\n'
    exit 1
  fi
fi
rm -f "$tmp"
actual=$(wc -c < "$p" 2>/dev/null | tr -d '[:space:]')
if [ "$actual" = "{expected_bytes}" ]; then
  printf 'PORTIX_UPLOAD_OK\n'
else
  printf 'PORTIX_UPLOAD_ERR size mismatch expected={expected_bytes} actual=%s\n' "$actual"
  exit 1
fi
"#,
        tmp = upload_temp_path(),
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

fn extract_portix_payload(output: &str, begin: &str, end: &str) -> Result<String> {
    let Some(after_begin) = output.split_once(begin).map(|(_, rest)| rest) else {
        let detail = output
            .lines()
            .rev()
            .find(|line| !line.trim().is_empty())
            .map(str::trim)
            .unwrap_or("remote did not return file payload");
        return Err(PortixError::InvalidRequest(format!(
            "remote download failed: {detail}"
        )));
    };
    let Some(payload) = after_begin.split_once(end).map(|(payload, _)| payload) else {
        return Err(PortixError::InvalidRequest(
            "remote download failed: incomplete file payload".to_owned(),
        ));
    };
    Ok(payload
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect::<String>())
}

fn help_suggestions_command(tokens: &[&str]) -> String {
    let Some(command) = tokens.first() else {
        return String::new();
    };
    let quoted_command = shell_quote(command);
    let quoted_invocation = tokens
        .iter()
        .map(|token| shell_quote(token))
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        r#"if command -v {quoted_command} >/dev/null 2>&1; then
  PORTIX_HELP_TIMEOUT=1
  if command -v timeout >/dev/null 2>&1; then
    GIT_PAGER=cat timeout "$PORTIX_HELP_TIMEOUT" {quoted_invocation} --help 2>&1 | head -c 20000
  else
    GIT_PAGER=cat {quoted_invocation} --help 2>&1 | head -c 20000
  fi
fi
"#
    )
}

fn command_name_suggestions_command(prefix: &str) -> String {
    let quoted_prefix = shell_quote(prefix);
    format!(
        r#"PORTIX_COMPLETE_PREFIX={quoted_prefix}
if command -v bash >/dev/null 2>&1; then
  bash -lc 'compgen -c -- "$1" | sort -u | head -n 80' _ "$PORTIX_COMPLETE_PREFIX"
else
  awk -v p="$PORTIX_COMPLETE_PREFIX" 'BEGIN {{
    n=split(ENVIRON["PATH"], dirs, ":")
    for (i=1; i<=n; i++) {{
      cmd="find \"" dirs[i] "\" -maxdepth 1 -type f -perm -111 -printf \"%f\n\" 2>/dev/null"
      while ((cmd | getline name) > 0) if (index(name, p) == 1) print name
      close(cmd)
    }}
  }}' | sort -u | head -n 80
fi
"#
    )
}

fn remote_history_suggestion_command(prefix: &str) -> String {
    let quoted_prefix = shell_quote(prefix);
    format!(
        r#"PORTIX_HISTORY_PREFIX={quoted_prefix}
PORTIX_HISTORY_FILE="${{HISTFILE:-$HOME/.zsh_history}}"
if [ -r "$PORTIX_HISTORY_FILE" ]; then
  tail -n 500 "$PORTIX_HISTORY_FILE" 2>/dev/null | awk -v p="$PORTIX_HISTORY_PREFIX" '
    {{
      line=$0
      sub(/^: [0-9]+:[0-9]+;/, "", line)
      if (index(line, p) == 1 && length(line) > length(p)) match_line=line
    }}
    END {{
      if (match_line != "") print substr(match_line, length(p) + 1)
    }}
  ' | head -n 1
fi
"#
    )
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

struct HelpSuggestionRequest<'a> {
    command: &'a str,
    help_tokens: Vec<&'a str>,
    current_token: &'a str,
    completing_command: bool,
    completing_option: bool,
}

impl<'a> HelpSuggestionRequest<'a> {
    fn parse(input: &'a str) -> Option<Self> {
        let trimmed = input.trim_start();
        if trimmed.len() < 2 || contains_shell_control(trimmed) {
            return None;
        }

        let tokens = trimmed.split_whitespace().collect::<Vec<_>>();
        let command = tokens.first().copied()?;
        if !is_safe_command_name(command) {
            return None;
        }

        let current_token = if trimmed.ends_with(char::is_whitespace) {
            ""
        } else {
            trimmed.split_whitespace().last().unwrap_or("")
        };
        let completing_command = tokens.len() == 1 && !trimmed.ends_with(char::is_whitespace);
        let completing_option = current_token.starts_with('-');
        let mut help_tokens = vec![command];
        if let Some(subcommand) = tokens.get(1).copied() {
            let subcommand_is_current =
                !trimmed.ends_with(char::is_whitespace) && current_token == subcommand;
            if !subcommand_is_current
                && is_safe_command_name(subcommand)
                && !subcommand.starts_with('-')
            {
                help_tokens.push(subcommand);
            }
        }

        Some(Self {
            command,
            help_tokens,
            current_token,
            completing_command,
            completing_option,
        })
    }
}

fn contains_shell_control(value: &str) -> bool {
    value
        .chars()
        .any(|char| matches!(char, ';' | '&' | '|' | '`' | '$' | '<' | '>' | '\n' | '\r'))
}

fn is_safe_command_name(value: &str) -> bool {
    if value.is_empty() || value.len() > 64 || value.contains('/') {
        return false;
    }
    value
        .chars()
        .all(|char| char.is_ascii_alphanumeric() || matches!(char, '-' | '_' | '.' | '+'))
}

fn is_allowed_help_command(command: &str) -> bool {
    const BLOCKED: &[&str] = &[
        "bash", "cat", "dd", "env", "fish", "ftp", "nc", "netcat", "perl", "python", "python3",
        "ruby", "scp", "sh", "sftp", "socat", "ssh", "sshpass", "sudo", "su", "zsh",
    ];
    !BLOCKED.contains(&command)
}

fn is_sensitive_autocomplete(value: &str) -> bool {
    let lower = value.to_lowercase();
    lower.contains("password")
        || lower.contains("passphrase")
        || lower.contains("passwd")
        || lower.contains("token")
        || lower.contains("secret")
        || lower.contains("api_key")
        || lower.contains("apikey")
        || lower.contains("private_key")
        || lower.contains("sshpass")
        || lower.contains("sudo -s")
        || lower.contains("sudo -S")
        || lower.contains("--password")
        || lower.contains("--token")
        || lower.contains("--secret")
}

fn parse_help_suggestions(
    input: &str,
    request: HelpSuggestionRequest<'_>,
    output: &str,
) -> Vec<String> {
    let mut suggestions = if request.completing_option {
        parse_option_candidates(input, &request, output)
    } else {
        parse_subcommand_candidates(input, &request, output)
    };

    if suggestions.is_empty() {
        suggestions = parse_option_candidates(input, &request, output);
    }

    suggestions.sort_by(|a, b| a.to_lowercase().cmp(&b.to_lowercase()));
    suggestions.dedup();
    suggestions.truncate(12);
    suggestions
}

fn parse_command_name_suggestions(
    input: &str,
    request: &HelpSuggestionRequest<'_>,
    output: &str,
) -> Vec<String> {
    let mut suggestions = output
        .lines()
        .map(str::trim)
        .filter(|candidate| {
            !candidate.is_empty()
                && candidate.starts_with(request.current_token)
                && is_safe_command_name(candidate)
        })
        .map(|candidate| {
            encode_completion_candidate(
                &replace_current_token(input, candidate),
                candidate,
                "PATH command",
                "command",
            )
        })
        .collect::<Vec<_>>();
    suggestions.sort_by(|a, b| a.to_lowercase().cmp(&b.to_lowercase()));
    suggestions.dedup();
    suggestions.truncate(24);
    suggestions
}

fn completion_item_from_wire(value: &str) -> Option<CompletionItem> {
    const PREFIX: &str = "PORTIX_COMPLETION\t";
    if !value.starts_with(PREFIX) {
        let label = value.trim();
        if label.is_empty() {
            return None;
        }
        return Some(CompletionItem {
            label: label.to_owned(),
            insert_text: label.to_owned(),
            kind: CompletionKind::History,
            description: None,
            score: 20,
        });
    }

    let mut parts = value[PREFIX.len()..].split('\t');
    let replacement = parts.next()?.trim();
    if replacement.is_empty() {
        return None;
    }
    let display = parts.next().map(str::trim).filter(|part| !part.is_empty());
    let description = parts.next().map(str::trim).filter(|part| !part.is_empty());
    let source = parts.next().map(str::trim).unwrap_or_default();
    let kind = match source {
        "command" => CompletionKind::Command,
        "path" => CompletionKind::Path,
        "directory" => CompletionKind::Directory,
        "file" => CompletionKind::File,
        "env" => CompletionKind::Env,
        "git" => CompletionKind::Git,
        "help" => CompletionKind::Command,
        _ => CompletionKind::Command,
    };
    let token = display.unwrap_or(replacement);
    Some(CompletionItem {
        label: token.to_owned(),
        insert_text: token.to_owned(),
        kind,
        description: description.map(str::to_owned),
        score: if source == "command" { 90 } else { 80 },
    })
}

fn parse_option_candidates(
    input: &str,
    request: &HelpSuggestionRequest<'_>,
    output: &str,
) -> Vec<String> {
    output
        .lines()
        .filter_map(clean_help_option_line)
        .filter(|candidate| {
            request.current_token.is_empty() || candidate.token.starts_with(request.current_token)
        })
        .map(|candidate| {
            encode_completion_candidate(
                &replace_current_token(input, &candidate.token),
                &candidate.token,
                &candidate.description,
                "help",
            )
        })
        .filter(|suggestion| !suggestion.trim().is_empty())
        .collect()
}

fn parse_subcommand_candidates(
    input: &str,
    request: &HelpSuggestionRequest<'_>,
    output: &str,
) -> Vec<String> {
    output
        .lines()
        .filter_map(clean_help_subcommand_line)
        .filter(|candidate| {
            request.current_token.is_empty() || candidate.token.starts_with(request.current_token)
        })
        .map(|candidate| {
            encode_completion_candidate(
                &replace_current_token(input, &candidate.token),
                &candidate.token,
                &candidate.description,
                "help",
            )
        })
        .collect()
}

struct HelpCandidate {
    token: String,
    description: String,
}

fn clean_help_option_line(line: &str) -> Option<HelpCandidate> {
    let trimmed = line.trim_start();
    if !trimmed.starts_with('-') {
        return None;
    }
    let tokens = trimmed
        .split(|char: char| char.is_whitespace() || matches!(char, ',' | '[' | ']'))
        .filter(|part| part.starts_with('-'))
        .collect::<Vec<_>>();
    let raw_token = tokens
        .iter()
        .find(|part| part.starts_with("--"))
        .or_else(|| tokens.first())?;
    let token = clean_help_option(raw_token)?.to_owned();
    let description = description_after_token(trimmed, raw_token).unwrap_or("command option");
    Some(HelpCandidate {
        token,
        description: normalize_description(description),
    })
}

fn clean_help_subcommand_line(line: &str) -> Option<HelpCandidate> {
    let trimmed = line.trim();
    if trimmed.is_empty()
        || trimmed.starts_with('-')
        || trimmed.starts_with("usage")
        || trimmed.starts_with("Usage")
        || trimmed.starts_with("Examples")
        || trimmed.starts_with("Options")
    {
        return None;
    }

    let mut split = trimmed.splitn(2, |char: char| char.is_whitespace());
    let token = split.next()?.trim();
    if !is_safe_command_name(token) || token.len() < 2 || token.starts_with('-') {
        return None;
    }
    let description = split.next()?.trim();
    if description.len() < 3 {
        return None;
    }

    Some(HelpCandidate {
        token: token.to_owned(),
        description: normalize_description(description),
    })
}

fn description_after_token<'a>(line: &'a str, raw_token: &str) -> Option<&'a str> {
    let index = line.find(raw_token)? + raw_token.len();
    let rest = line[index..]
        .trim_start_matches(|char: char| char == ',' || char.is_whitespace())
        .trim_start_matches(|char: char| {
            char.is_ascii_alphanumeric() || matches!(char, '<' | '>' | '[' | ']' | '=' | '-' | '_')
        })
        .trim_start();
    if rest.is_empty() { None } else { Some(rest) }
}

fn normalize_description(value: &str) -> String {
    value
        .trim_matches(|char: char| matches!(char, '-' | ':' | ';' | ',' | '.'))
        .split_whitespace()
        .take(18)
        .collect::<Vec<_>>()
        .join(" ")
}

fn encode_completion_candidate(
    replacement: &str,
    display: &str,
    description: &str,
    source: &str,
) -> String {
    format!(
        "PORTIX_COMPLETION\t{}\t{}\t{}\t{}",
        sanitize_completion_field(replacement),
        sanitize_completion_field(display),
        sanitize_completion_field(description),
        sanitize_completion_field(source)
    )
}

fn sanitize_completion_field(value: &str) -> String {
    value
        .chars()
        .map(|char| {
            if matches!(char, '\t' | '\n' | '\r') {
                ' '
            } else {
                char
            }
        })
        .collect::<String>()
        .trim()
        .to_owned()
}

fn clean_help_option(raw: &str) -> Option<&str> {
    let option = raw
        .trim_matches(|char: char| matches!(char, ':' | ';' | '.' | '=' | '"' | '\''))
        .trim();
    if option.len() < 2 || option.len() > 48 {
        return None;
    }
    if !option.starts_with('-') {
        return None;
    }
    if option.chars().all(|char| char == '-') {
        return None;
    }
    if option
        .chars()
        .any(|char| !char.is_ascii_alphanumeric() && !matches!(char, '-' | '_' | '.'))
    {
        return None;
    }
    Some(option)
}

fn replace_current_token(input: &str, option: &str) -> String {
    let trimmed_end = input.trim_end();
    let starts_new_token = input.ends_with(char::is_whitespace);
    if starts_new_token {
        return format!("{trimmed_end}{option}");
    }

    let token_start = trimmed_end
        .rfind(char::is_whitespace)
        .map(|index| index + 1)
        .unwrap_or(0);
    format!("{}{}", &trimmed_end[..token_start], option)
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
