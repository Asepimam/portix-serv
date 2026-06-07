use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TerminalCompleteRequest {
    pub buffer: String,
    pub cursor: usize,
    pub cwd: String,
    pub shell: Option<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    pub max_items: Option<usize>,
    pub session_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TerminalCompleteResponse {
    pub suggestion: Option<String>,
    pub items: Vec<CompletionItem>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CompletionItem {
    pub label: String,
    pub insert_text: String,
    pub kind: CompletionKind,
    pub description: Option<String>,
    pub score: i32,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum CompletionKind {
    Command,
    Path,
    Directory,
    File,
    Env,
    Git,
    History,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletionContext {
    pub prefix: String,
    pub current_token: String,
    pub command: Option<String>,
    pub token_index: usize,
}

impl TerminalCompleteRequest {
    pub fn max_items(&self) -> usize {
        self.max_items.unwrap_or(24).clamp(1, 80)
    }

    pub fn prefix(&self) -> String {
        self.buffer
            .chars()
            .take(self.cursor.min(self.buffer.chars().count()))
            .collect()
    }

    pub fn context(&self) -> CompletionContext {
        let prefix = self.prefix();
        let ends_with_space = prefix.chars().last().is_some_and(char::is_whitespace);
        let tokens = prefix.split_whitespace().collect::<Vec<_>>();
        let current_token = if ends_with_space {
            String::new()
        } else {
            tokens.last().copied().unwrap_or("").to_owned()
        };
        let command = tokens.first().map(|value| (*value).to_owned());
        let token_index = if ends_with_space {
            tokens.len()
        } else {
            tokens.len().saturating_sub(1)
        };

        CompletionContext {
            prefix,
            current_token,
            command,
            token_index,
        }
    }
}
