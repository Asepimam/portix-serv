use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use tokio::task;

use crate::domain::autocomplete::{CompletionItem, CompletionKind};
use crate::domain::errors::{PortixError, Result};

use super::cache::TtlCache;

pub struct HistoryProvider {
    cache: TtlCache<String, Vec<String>>,
}

impl HistoryProvider {
    pub fn new() -> Self {
        Self {
            cache: TtlCache::new(Duration::from_secs(10)),
        }
    }

    pub async fn autosuggestion(
        &self,
        buffer: &str,
        env: &HashMap<String, String>,
    ) -> Result<Option<String>> {
        if buffer.trim().is_empty() {
            return Ok(None);
        }

        let history = self.load(env).await?;
        Ok(history
            .iter()
            .rev()
            .find(|entry| entry.starts_with(buffer) && entry.len() > buffer.len())
            .map(|entry| entry[buffer.len()..].to_owned()))
    }

    pub async fn complete(
        &self,
        prefix: &str,
        env: &HashMap<String, String>,
        limit: usize,
    ) -> Result<Vec<CompletionItem>> {
        if prefix.trim().is_empty() {
            return Ok(Vec::new());
        }

        let history = self.load(env).await?;
        let mut items = history
            .iter()
            .rev()
            .filter(|entry| entry.starts_with(prefix) && entry.len() > prefix.len())
            .map(|entry| CompletionItem {
                label: entry.clone(),
                insert_text: entry.clone(),
                kind: CompletionKind::History,
                description: Some("history".to_owned()),
                score: 20,
            })
            .collect::<Vec<_>>();
        items.dedup_by(|a, b| a.insert_text == b.insert_text);
        items.truncate(limit);
        Ok(items)
    }

    async fn load(&self, env: &HashMap<String, String>) -> Result<Vec<String>> {
        let cache_key = history_cache_key(env);
        if let Some(history) = self.cache.get(&cache_key).await {
            return Ok(history);
        }

        let env = env.clone();
        let history = task::spawn_blocking(move || read_history(&env))
            .await
            .map_err(|error| PortixError::Anyhow(error.into()))??;
        self.cache.put(cache_key, history.clone()).await;
        Ok(history)
    }
}

impl Default for HistoryProvider {
    fn default() -> Self {
        Self::new()
    }
}

fn history_cache_key(env: &HashMap<String, String>) -> String {
    env.get("PORTIX_HISTORY")
        .map(|value| format!("inline:{}", value.len()))
        .or_else(|| env.get("HISTFILE").map(|value| format!("file:{value}")))
        .or_else(|| env.get("HOME").map(|value| format!("home:{value}")))
        .unwrap_or_else(|| "empty".to_owned())
}

fn read_history(env: &HashMap<String, String>) -> Result<Vec<String>> {
    if let Some(inline) = env.get("PORTIX_HISTORY") {
        return Ok(parse_history(inline));
    }

    let path = env.get("HISTFILE").map(PathBuf::from).or_else(|| {
        env.get("HOME")
            .map(|home| PathBuf::from(home).join(".zsh_history"))
    });

    let Some(path) = path else {
        return Ok(Vec::new());
    };
    let Ok(content) = std::fs::read_to_string(path) else {
        return Ok(Vec::new());
    };
    Ok(parse_history(&content))
}

fn parse_history(content: &str) -> Vec<String> {
    content
        .lines()
        .filter_map(|line| {
            let command = line
                .rsplit_once(';')
                .map(|(_, command)| command)
                .unwrap_or(line);
            let command = command.trim();
            if command.is_empty() || is_sensitive(command) {
                None
            } else {
                Some(command.to_owned())
            }
        })
        .collect()
}

fn is_sensitive(command: &str) -> bool {
    let lower = command.to_lowercase();
    lower.contains("password")
        || lower.contains("passphrase")
        || lower.contains("token")
        || lower.contains("secret")
        || lower.contains("api_key")
        || lower.contains("apikey")
        || lower.contains("private_key")
        || lower.contains("sshpass")
        || lower.contains("sudo -s")
}
