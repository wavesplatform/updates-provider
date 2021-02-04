pub mod repo;
pub mod updater;
mod watchlist;

use crate::error::Error;
use crate::models::TestResource;
use crate::resources::repo::ResourcesRepoImpl;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use self::repo::TestResourcesRepoImpl;
use self::watchlist::WatchList;

// thread-safe types
type TSTestResourcesRepoImpl = Arc<RwLock<TestResourcesRepoImpl>>;
type TSResourcesRepoImpl = Arc<ResourcesRepoImpl>;
type TSTestResourcesWatchList = Arc<RwLock<WatchList<TestResource>>>;
type TSTestResourcesUpdatesProviderLastValues = Arc<RwLock<HashMap<String, String>>>;

#[derive(Clone, Debug)]
pub struct Config {
    pub test_resources_base_url: String,
    pub polling_delay: Duration,
}

#[async_trait]
pub trait TestResourcesRepo {
    async fn get(&self, test_resource: TestResource) -> Result<String, Error>;
}
