use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::task;

use crate::domain::autocomplete::{CompletionItem, CompletionKind};
use crate::domain::errors::{PortixError, Result};

use super::cache::TtlCache;

pub struct GitProvider {
    branch_cache: TtlCache<String, Vec<String>>,
}

impl GitProvider {
    pub fn new() -> Self {
        Self {
            branch_cache: TtlCache::new(Duration::from_secs(5)),
        }
    }

    pub async fn complete(
        &self,
        cwd: &str,
        token_index: usize,
        token: &str,
        limit: usize,
    ) -> Result<Vec<CompletionItem>> {
        if token_index <= 1 {
            return Ok(git_subcommands(token, limit));
        }

        let repo = find_git_dir(Path::new(cwd));
        let Some(repo) = repo else {
            return Ok(Vec::new());
        };
        let cache_key = repo.to_string_lossy().to_string();
        let branches = if let Some(branches) = self.branch_cache.get(&cache_key).await {
            branches
        } else {
            let repo_for_task = repo.clone();
            let branches = task::spawn_blocking(move || read_branches(&repo_for_task))
                .await
                .map_err(|error| PortixError::Anyhow(error.into()))??;
            self.branch_cache.put(cache_key, branches.clone()).await;
            branches
        };

        Ok(branches
            .into_iter()
            .filter(|branch| branch.starts_with(token))
            .take(limit)
            .map(|branch| CompletionItem {
                label: branch.clone(),
                insert_text: branch,
                kind: CompletionKind::Git,
                description: Some("git branch".to_owned()),
                score: 85,
            })
            .collect())
    }
}

impl Default for GitProvider {
    fn default() -> Self {
        Self::new()
    }
}

fn git_subcommands(token: &str, limit: usize) -> Vec<CompletionItem> {
    const COMMANDS: &[(&str, &str)] = &[
        ("add", "add file contents to the index"),
        ("branch", "list, create, or delete branches"),
        ("checkout", "switch branches or restore files"),
        ("clone", "clone a repository into a new directory"),
        ("commit", "record changes to the repository"),
        ("diff", "show changes between commits and working tree"),
        ("fetch", "download objects and refs from another repository"),
        ("log", "show commit logs"),
        ("merge", "join development histories together"),
        ("pull", "fetch from and merge with another repository"),
        ("push", "update remote refs along with associated objects"),
        ("rebase", "reapply commits on top of another base tip"),
        ("remote", "manage tracked repositories"),
        ("restore", "restore working tree files"),
        ("status", "show the working tree status"),
        ("switch", "switch branches"),
    ];

    COMMANDS
        .iter()
        .filter(|(command, _)| command.starts_with(token))
        .take(limit)
        .map(|(command, description)| CompletionItem {
            label: (*command).to_owned(),
            insert_text: (*command).to_owned(),
            kind: CompletionKind::Git,
            description: Some((*description).to_owned()),
            score: 95,
        })
        .collect()
}

fn find_git_dir(start: &Path) -> Option<PathBuf> {
    let mut current = if start.is_dir() {
        start.to_path_buf()
    } else {
        start.parent()?.to_path_buf()
    };

    loop {
        let dot_git = current.join(".git");
        if dot_git.is_dir() {
            return Some(dot_git);
        }
        if !current.pop() {
            return None;
        }
    }
}

fn read_branches(git_dir: &Path) -> Result<Vec<String>> {
    let mut branches = Vec::new();
    read_ref_dir(&git_dir.join("refs").join("heads"), "", &mut branches);
    read_packed_refs(&git_dir.join("packed-refs"), &mut branches);
    branches.sort();
    branches.dedup();
    Ok(branches)
}

fn read_ref_dir(dir: &Path, prefix: &str, branches: &mut Vec<String>) {
    let Ok(read_dir) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in read_dir.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        let branch = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}/{name}")
        };
        if entry.file_type().map(|kind| kind.is_dir()).unwrap_or(false) {
            read_ref_dir(&entry.path(), &branch, branches);
        } else {
            branches.push(branch);
        }
    }
}

fn read_packed_refs(path: &Path, branches: &mut Vec<String>) {
    let Ok(content) = std::fs::read_to_string(path) else {
        return;
    };
    for line in content.lines() {
        if line.starts_with('#') || line.starts_with('^') {
            continue;
        }
        let Some((_, reference)) = line.split_once(' ') else {
            continue;
        };
        if let Some(branch) = reference.strip_prefix("refs/heads/") {
            branches.push(branch.to_owned());
        }
    }
}
