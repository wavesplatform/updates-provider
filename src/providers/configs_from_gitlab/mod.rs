pub mod configs_updater;
pub mod repo;
mod watchlist;

use crate::error::Error;
use crate::models::ConfigFile;
use crate::resources::repo::ResourcesRepoImpl;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use self::repo::ConfigsFromGitlabRepoImpl;
use self::watchlist::WatchList;

// thread-safe types
type TSConfigsFromGitlabRepoImpl = Arc<RwLock<ConfigsFromGitlabRepoImpl>>;
type TSResourcesRepoImpl = Arc<ResourcesRepoImpl>;
type TSConfigFilesWatchList = Arc<RwLock<WatchList<ConfigFile>>>;
type TSConfigUpdatesProviderLastValues = Arc<RwLock<HashMap<String, String>>>;

#[derive(Clone, Debug)]
pub struct Config {
    pub configs_base_url: String,
    pub polling_delay: Duration,
    pub gitlab_private_token: String,
    pub gitlab_configs_branch: String,
}

#[async_trait]
pub trait ConfigsFromGitlabRepo {
    async fn get(&self, config_file: ConfigFile) -> Result<String, Error>;
}
