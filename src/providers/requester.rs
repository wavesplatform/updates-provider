use async_trait::async_trait;

use super::TSResourcesRepoImpl;
use super::TSUpdatesProviderLastValues;
use crate::error::Error;
use std::collections::hash_set::Iter;

#[async_trait]
pub trait Requester<T>: Send + Sync {
    async fn process<'a>(
        &self,
        items_iter: Iter<'a, T>,
        resources_repo: &TSResourcesRepoImpl,
        last_values: &TSUpdatesProviderLastValues,
    ) -> Result<(), Error>;
}
