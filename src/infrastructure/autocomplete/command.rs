use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Duration;

use tokio::task;

use crate::domain::autocomplete::{CompletionItem, CompletionKind};
use crate::domain::errors::{PortixError, Result};

use super::cache::TtlCache;

pub struct CommandProvider {
    cache: TtlCache<String, Vec<String>>,
}

impl CommandProvider {
    pub fn new() -> Self {
        Self {
            cache: TtlCache::new(Duration::from_secs(30)),
        }
    }

    pub async fn complete(
        &self,
        token: &str,
        env: &HashMap<String, String>,
        limit: usize,
    ) -> Result<Vec<CompletionItem>> {
        let path = env
            .get("PATH")
            .cloned()
            .or_else(|| std::env::var("PATH").ok())
            .unwrap_or_default();
        if let Some(commands) = self.cache.get(&path).await {
            return Ok(filter_commands(commands, token, limit));
        }

        let path_for_task = path.clone();
        let commands = task::spawn_blocking(move || read_path_commands(&path_for_task))
            .await
            .map_err(|error| PortixError::Anyhow(error.into()))??;
        self.cache.put(path, commands.clone()).await;
        Ok(filter_commands(commands, token, limit))
    }
}

impl Default for CommandProvider {
    fn default() -> Self {
        Self::new()
    }
}

fn read_path_commands(path: &str) -> Result<Vec<String>> {
    let mut seen = HashSet::new();
    let mut commands = Vec::new();
    for entry in std::env::split_paths(path) {
        if !entry.is_dir() {
            continue;
        }
        let Ok(read_dir) = std::fs::read_dir(entry) else {
            continue;
        };
        for entry in read_dir.flatten() {
            let path = entry.path();
            if !is_executable_file(&path) {
                continue;
            }
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if seen.insert(name.to_owned()) {
                commands.push(name.to_owned());
            }
        }
    }
    commands.sort();
    Ok(commands)
}

#[cfg(unix)]
fn is_executable_file(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;

    path.is_file()
        && std::fs::metadata(path)
            .map(|metadata| metadata.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
}

#[cfg(not(unix))]
fn is_executable_file(path: &Path) -> bool {
    const EXECUTABLE_EXTENSIONS: &[&str] = &["exe", "bat", "cmd", "ps1"];
    path.is_file()
        && path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| {
                EXECUTABLE_EXTENSIONS
                    .iter()
                    .any(|candidate| candidate.eq_ignore_ascii_case(extension))
            })
}

fn filter_commands(commands: Vec<String>, token: &str, limit: usize) -> Vec<CompletionItem> {
    commands
        .into_iter()
        .filter(|command| command.starts_with(token))
        .take(limit)
        .map(|command| CompletionItem {
            label: command.clone(),
            insert_text: command,
            kind: CompletionKind::Command,
            description: Some("PATH command".to_owned()),
            score: 80,
        })
        .collect()
}
