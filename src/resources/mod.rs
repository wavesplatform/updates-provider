pub mod repo;

use crate::error::Error;
use async_trait::async_trait;
use wavesexchange_topic::Topic;

#[async_trait]
pub trait ResourcesRepo {
    async fn get(&self, resource: &Topic) -> Result<Option<String>, Error>;

    async fn set(&self, resource: Topic, value: String) -> Result<(), Error>;

    async fn del(&self, resource: Topic) -> Result<(), Error>;

    async fn push(&self, resource: Topic, value: String) -> Result<(), Error>;

    async fn set_and_push(&self, resource: Topic, value: String) -> Result<(), Error> {
        self.set(resource.clone(), value.clone()).await?;
        self.push(resource, value).await?;
        Ok(())
    }
}
