use crate::error::Error;
use async_trait::async_trait;

#[async_trait]
pub trait Requester<T: 'static>: Send + Sync {
    async fn get(&self, data: &T) -> Result<String, Error>;
}
