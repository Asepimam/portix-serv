use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::task;

use crate::domain::autocomplete::{CompletionItem, CompletionKind};
use crate::domain::errors::{PortixError, Result};

use super::cache::TtlCache;

pub struct PathProvider {
    cache: TtlCache<String, Vec<PathCandidate>>,
}

#[derive(Clone)]
struct PathCandidate {
    name: String,
    is_dir: bool,
}

impl PathProvider {
    pub fn new() -> Self {
        Self {
            cache: TtlCache::new(Duration::from_secs(5)),
        }
    }

    pub async fn complete(
        &self,
        cwd: &str,
        token: &str,
        limit: usize,
    ) -> Result<Vec<CompletionItem>> {
        let (dir, needle, insert_prefix) = split_path_token(cwd, token);
        let cache_key = dir.to_string_lossy().to_string();
        let candidates = if let Some(cached) = self.cache.get(&cache_key).await {
            cached
        } else {
            let dir_for_task = dir.clone();
            let candidates = task::spawn_blocking(move || read_directory_candidates(&dir_for_task))
                .await
                .map_err(|error| PortixError::Anyhow(error.into()))??;
            self.cache.put(cache_key, candidates.clone()).await;
            candidates
        };

        let mut items = candidates
            .into_iter()
            .filter(|candidate| candidate.name.starts_with(&needle))
            .map(|candidate| {
                let suffix = if candidate.is_dir { "/" } else { "" };
                CompletionItem {
                    label: format!("{}{suffix}", candidate.name),
                    insert_text: format!("{insert_prefix}{}{suffix}", candidate.name),
                    kind: if candidate.is_dir {
                        CompletionKind::Directory
                    } else {
                        CompletionKind::File
                    },
                    description: Some(
                        if candidate.is_dir {
                            "directory"
                        } else {
                            "file"
                        }
                        .to_owned(),
                    ),
                    score: if candidate.is_dir { 75 } else { 65 },
                }
            })
            .collect::<Vec<_>>();
        items.sort_by(|a, b| {
            b.score
                .cmp(&a.score)
                .then_with(|| a.label.to_lowercase().cmp(&b.label.to_lowercase()))
        });
        items.truncate(limit);
        Ok(items)
    }
}

impl Default for PathProvider {
    fn default() -> Self {
        Self::new()
    }
}

fn split_path_token(cwd: &str, token: &str) -> (PathBuf, String, String) {
    let token = token.trim();
    let (dir_part, needle) = token.rsplit_once('/').unwrap_or(("", token));
    let insert_prefix = if dir_part.is_empty() {
        String::new()
    } else {
        format!("{dir_part}/")
    };

    let dir = if dir_part.is_empty() {
        PathBuf::from(cwd)
    } else if dir_part == "~" {
        std::env::var("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(cwd))
    } else {
        let path = PathBuf::from(dir_part);
        if path.is_absolute() {
            path
        } else {
            PathBuf::from(cwd).join(path)
        }
    };

    (dir, needle.to_owned(), insert_prefix)
}

fn read_directory_candidates(dir: &Path) -> Result<Vec<PathCandidate>> {
    let Ok(read_dir) = std::fs::read_dir(dir) else {
        return Ok(Vec::new());
    };
    let mut candidates = read_dir
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.is_empty() {
                return None;
            }
            let is_dir = entry.file_type().map(|kind| kind.is_dir()).unwrap_or(false);
            Some(PathCandidate { name, is_dir })
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|a, b| {
        b.is_dir
            .cmp(&a.is_dir)
            .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
    });
    Ok(candidates)
}
