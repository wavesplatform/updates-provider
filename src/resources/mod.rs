pub mod repo;

use crate::error::Error;
use crate::models::Resource;
use async_trait::async_trait;

#[async_trait]
pub trait ResourcesRepo {
    async fn get(&self, resource: &Resource) -> Result<Option<String>, Error>;

    async fn set(&self, resource: Resource, value: String) -> Result<(), Error>;
}
