use super::TSResourcesRepoImpl;
use super::TSUpdatesProviderLastValues;
use crate::error::Error;
use async_trait::async_trait;

#[async_trait]
pub trait Requester<T: 'static>: Send + Sync {
    async fn process<'a, I: Iterator<Item = &'a T> + Send + Sync>(
        &self,
        items: I,
        resources_repo: &TSResourcesRepoImpl,
        last_values: &TSUpdatesProviderLastValues,
    ) -> Result<(), Error>;
}

#[async_trait]
pub trait ErasedRequester<T>: Send + Sync {
    async fn erased_process(
        &self,
        items: &mut (dyn Iterator<Item = &T> + Send + Sync),
        resources_repo: &TSResourcesRepoImpl,
        last_values: &TSUpdatesProviderLastValues,
    ) -> Result<(), Error>;
}

#[async_trait]
impl<T: 'static> Requester<T> for Box<dyn ErasedRequester<T>> {
    async fn process<'a, I: Iterator<Item = &'a T> + Send + Sync>(
        &self,
        mut items: I,
        resources_repo: &TSResourcesRepoImpl,
        last_values: &TSUpdatesProviderLastValues,
    ) -> Result<(), Error> {
        (**self)
            .erased_process(&mut items, resources_repo, last_values)
            .await
    }
}

#[async_trait]
impl<R, T: 'static> ErasedRequester<T> for R
where
    R: Requester<T> + Send + Sync,
{
    async fn erased_process(
        &self,
        items: &mut (dyn Iterator<Item = &T> + Send + Sync),
        resources_repo: &TSResourcesRepoImpl,
        last_values: &TSUpdatesProviderLastValues,
    ) -> Result<(), Error> {
        self.process(items, resources_repo, last_values).await
    }
}
