use std::collections::HashMap;

use crate::domain::autocomplete::{CompletionItem, CompletionKind};

pub struct EnvProvider;

impl EnvProvider {
    pub fn complete(
        token: &str,
        env: &HashMap<String, String>,
        limit: usize,
    ) -> Vec<CompletionItem> {
        let Some(name_prefix) = token.strip_prefix('$') else {
            return Vec::new();
        };

        let mut items = env
            .keys()
            .filter(|key| key.starts_with(name_prefix))
            .map(|key| CompletionItem {
                label: format!("${key}"),
                insert_text: format!("${key}"),
                kind: CompletionKind::Env,
                description: Some("environment variable".to_owned()),
                score: 90,
            })
            .collect::<Vec<_>>();
        items.sort_by(|a, b| a.label.cmp(&b.label));
        items.truncate(limit);
        items
    }
}
