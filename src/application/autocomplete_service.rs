use std::collections::HashSet;

use crate::domain::autocomplete::{
    CompletionItem, CompletionKind, TerminalCompleteRequest, TerminalCompleteResponse,
};
use crate::domain::errors::{PortixError, Result};
use crate::infrastructure::autocomplete::AutocompleteProviders;
use crate::infrastructure::autocomplete::env::EnvProvider;
use crate::infrastructure::autocomplete::option::OptionProvider;

pub struct AutocompleteService {
    providers: AutocompleteProviders,
}

impl AutocompleteService {
    pub fn new() -> Self {
        Self {
            providers: AutocompleteProviders::new(),
        }
    }

    pub async fn complete_json(&self, req_json: String) -> Result<String> {
        let request = serde_json::from_str::<TerminalCompleteRequest>(&req_json)
            .map_err(|error| PortixError::InvalidRequest(error.to_string()))?;
        let response = self.complete(request).await?;
        serde_json::to_string(&response)
            .map_err(|error| PortixError::InvalidRequest(error.to_string()))
    }

    pub async fn complete(
        &self,
        request: TerminalCompleteRequest,
    ) -> Result<TerminalCompleteResponse> {
        let context = request.context();
        let max_items = request.max_items();
        let mut items = Vec::new();

        let suggestion = self
            .providers
            .history
            .autosuggestion(&context.prefix, &request.env)
            .await?;

        if context.current_token.starts_with('$') {
            items.extend(EnvProvider::complete(
                &context.current_token,
                &request.env,
                max_items,
            ));
        } else if context.command.as_deref() == Some("git") {
            items.extend(
                self.providers
                    .git
                    .complete(
                        &request.cwd,
                        context.token_index,
                        &context.current_token,
                        max_items,
                    )
                    .await?,
            );
        } else if should_complete_option(&context) {
            if let Some(command) = context.command.as_deref() {
                let token = if context.current_token.is_empty() {
                    "-" // Show all options when no token typed yet
                } else {
                    &context.current_token
                };
                items.extend(OptionProvider::complete(command, token, max_items));
            }
        } else if should_complete_subcommand(&context) {
            if let Some(command) = context.command.as_deref() {
                items.extend(OptionProvider::complete(
                    command,
                    &context.current_token,
                    max_items,
                ));
            }
        } else if should_complete_command(&context) {
            items.extend(
                self.providers
                    .commands
                    .complete(&context.current_token, &request.env, max_items)
                    .await?,
            );
        } else if should_complete_path(&context) {
            items.extend(
                self.providers
                    .paths
                    .complete(&request.cwd, &context.current_token, max_items)
                    .await?,
            );
        }

        if items.len() < max_items {
            items.extend(
                self.providers
                    .history
                    .complete(&context.prefix, &request.env, max_items - items.len())
                    .await?,
            );
        }

        let items = ranked_dedup(items, max_items);

        // If no history suggestion, try option inline suggestion (e.g. "-lr" → "th")
        let final_suggestion = if suggestion.is_some() {
            suggestion
        } else if should_complete_option(&context) {
            context.command.as_deref().and_then(|cmd| {
                OptionProvider::inline_suggestion(cmd, &context.current_token)
            })
        } else {
            None
        };

        Ok(TerminalCompleteResponse { suggestion: final_suggestion, items })
    }
}

impl Default for AutocompleteService {
    fn default() -> Self {
        Self::new()
    }
}

fn should_complete_command(context: &crate::domain::autocomplete::CompletionContext) -> bool {
    context.token_index == 0 && !context.prefix.ends_with(char::is_whitespace)
}

fn should_complete_option(context: &crate::domain::autocomplete::CompletionContext) -> bool {
    if context.token_index == 0 {
        return false;
    }
    // Trigger option completion when:
    // - Token starts with '-' (user typing a flag)
    // - Token is empty and command has known options (user just pressed space)
    let token = &context.current_token;
    if token.starts_with('-') {
        return true;
    }
    if token.is_empty() {
        // Only show options hint if command is known
        if let Some(cmd) = context.command.as_deref() {
            return matches!(
                cmd,
                "ls" | "grep" | "rg" | "find" | "chmod" | "chown" | "cp" | "mv" | "rm"
                    | "mkdir" | "cat" | "tail" | "head" | "less" | "tar" | "zip" | "gzip"
                    | "curl" | "wget" | "ssh" | "scp" | "rsync" | "ps" | "kill"
                    | "pgrep" | "pkill" | "du" | "df" | "awk" | "sed" | "jq"
                    | "xargs" | "sort" | "uniq" | "wc" | "cut" | "watch"
                    | "ss" | "lsof" | "ip" | "ping" | "dig" | "openssl" | "ufw"
                    | "nginx" | "make" | "psql" | "mysql" | "crontab"
            );
        }
    }
    false
}

fn should_complete_subcommand(context: &crate::domain::autocomplete::CompletionContext) -> bool {
    // Show subcommands/options when token_index > 0 and either:
    // - current_token is empty (user pressed space after command)
    // - current_token doesn't look like a path
    let token = &context.current_token;
    if context.token_index == 0 {
        return false;
    }
    if token.starts_with('-') || token.starts_with('.') || token.starts_with('/')
        || token.starts_with('~') || token.contains('/') || token.starts_with('$') {
        return false;
    }
    matches!(
        context.command.as_deref(),
        Some("docker" | "docker compose" | "systemctl" | "journalctl" | "kubectl"
            | "apt" | "apt-get" | "yum" | "dnf" | "npm" | "cargo" | "pip" | "pip3"
            | "git" | "python" | "python3" | "redis-cli" | "tmux" | "make"
            | "openssl" | "ufw")
    )
}

fn should_complete_path(context: &crate::domain::autocomplete::CompletionContext) -> bool {
    context.token_index > 0
        || context.current_token.starts_with('.')
        || context.current_token.starts_with('/')
        || context.current_token.starts_with('~')
        || context.current_token.contains('/')
}

fn ranked_dedup(mut items: Vec<CompletionItem>, limit: usize) -> Vec<CompletionItem> {
    items.sort_by(|a, b| {
        b.score
            .cmp(&a.score)
            .then_with(|| kind_rank(&a.kind).cmp(&kind_rank(&b.kind)))
            .then_with(|| a.label.to_lowercase().cmp(&b.label.to_lowercase()))
    });

    let mut seen = HashSet::new();
    let mut deduped = Vec::new();
    for item in items {
        if seen.insert((item.kind.clone(), item.insert_text.clone())) {
            deduped.push(item);
        }
        if deduped.len() >= limit {
            break;
        }
    }
    deduped
}

fn kind_rank(kind: &CompletionKind) -> i32 {
    match kind {
        CompletionKind::Git => 0,
        CompletionKind::Env => 1,
        CompletionKind::Command => 2,
        CompletionKind::Directory => 3,
        CompletionKind::File => 4,
        CompletionKind::Path => 5,
        CompletionKind::History => 6,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tempfile::TempDir;

    use super::*;
    use crate::domain::autocomplete::TerminalCompleteRequest;

    fn request(buffer: &str, cwd: String, env: HashMap<String, String>) -> TerminalCompleteRequest {
        TerminalCompleteRequest {
            buffer: buffer.to_owned(),
            cursor: buffer.len(),
            cwd,
            shell: Some("zsh".to_owned()),
            env,
            max_items: Some(20),
            session_id: None,
        }
    }

    #[tokio::test]
    async fn history_autosuggestion_returns_suffix() {
        let service = AutocompleteService::new();
        let mut env = HashMap::new();
        env.insert(
            "PORTIX_HISTORY".to_owned(),
            "git status\ngit push --force-with-lease origin main\n".to_owned(),
        );

        let response = service
            .complete(request("git pu", ".".to_owned(), env))
            .await
            .unwrap();

        assert_eq!(
            response.suggestion,
            Some("sh --force-with-lease origin main".to_owned())
        );
    }

    #[tokio::test]
    async fn env_completion_returns_matching_variables() {
        let service = AutocompleteService::new();
        let mut env = HashMap::new();
        env.insert("PORTIX_TOKEN".to_owned(), "redacted".to_owned());
        env.insert("PATH".to_owned(), "/bin".to_owned());

        let response = service
            .complete(request("echo $PORT", ".".to_owned(), env))
            .await
            .unwrap();

        assert!(response.items.iter().any(|item| {
            item.kind == CompletionKind::Env && item.insert_text == "$PORTIX_TOKEN"
        }));
    }

    #[tokio::test]
    async fn path_completion_returns_file_and_directory() {
        let service = AutocompleteService::new();
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("alpha.txt"), "demo").unwrap();
        std::fs::create_dir(dir.path().join("apps")).unwrap();

        let response = service
            .complete(request(
                "cat a",
                dir.path().to_string_lossy().to_string(),
                HashMap::new(),
            ))
            .await
            .unwrap();

        assert!(
            response.items.iter().any(|item| {
                item.kind == CompletionKind::Directory && item.insert_text == "apps/"
            })
        );
        assert!(
            response.items.iter().any(|item| {
                item.kind == CompletionKind::File && item.insert_text == "alpha.txt"
            })
        );
    }

    #[tokio::test]
    async fn git_branch_completion_reads_refs_without_executing_git() {
        let service = AutocompleteService::new();
        let dir = TempDir::new().unwrap();
        let refs = dir.path().join(".git").join("refs").join("heads");
        std::fs::create_dir_all(&refs).unwrap();
        std::fs::write(refs.join("main"), "abc").unwrap();
        std::fs::create_dir_all(refs.join("feature")).unwrap();
        std::fs::write(refs.join("feature").join("login"), "abc").unwrap();

        let response = service
            .complete(request(
                "git checkout fe",
                dir.path().to_string_lossy().to_string(),
                HashMap::new(),
            ))
            .await
            .unwrap();

        assert!(response.items.iter().any(|item| {
            item.kind == CompletionKind::Git && item.insert_text == "feature/login"
        }));
    }
}
