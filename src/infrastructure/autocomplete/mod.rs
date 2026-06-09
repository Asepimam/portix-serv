pub mod cache;
pub mod command;
pub mod env;
pub mod git;
pub mod history;
pub mod option;
pub mod path;

use command::CommandProvider;
use git::GitProvider;
use history::HistoryProvider;
use path::PathProvider;

pub struct AutocompleteProviders {
    pub commands: CommandProvider,
    pub history: HistoryProvider,
    pub paths: PathProvider,
    pub git: GitProvider,
}

impl AutocompleteProviders {
    pub fn new() -> Self {
        Self {
            commands: CommandProvider::new(),
            history: HistoryProvider::new(),
            paths: PathProvider::new(),
            git: GitProvider::new(),
        }
    }
}

impl Default for AutocompleteProviders {
    fn default() -> Self {
        Self::new()
    }
}
