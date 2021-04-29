use super::super::watchlist_process;
use super::requester::Requester;
use super::{TSResourcesRepoImpl, TSUpdatesProviderLastValues};
use crate::error::Error;
use crate::models::TestResource;
use async_trait::async_trait;
use futures::stream::{self, StreamExt, TryStreamExt};
use reqwest::{Client, ClientBuilder};
use std::time::Duration;
use wavesexchange_log::error;

#[async_trait]
pub trait ConfigsRepo {
    async fn get(&self, test_resource: TestResource) -> Result<String, Error>;
}

#[derive(Clone, Debug)]
pub struct Config {
    pub test_resources_base_url: String,
    pub polling_delay: Duration,
    pub delete_timeout: Duration,
}

pub struct TestResourcesRequester {
    test_resources_base_url: String,
    http_client: Client,
}

impl TestResourcesRequester {
    pub fn new(test_resources_base_url: String) -> Self {
        Self {
            test_resources_base_url,
            http_client: ClientBuilder::new()
                .timeout(std::time::Duration::from_secs(30))
                .connect_timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap(),
        }
    }

    async fn get(&self, test_resource: &TestResource) -> Result<String, Error> {
        let test_resource_url = reqwest::Url::parse(
            format!(
                "{}/{}",
                self.test_resources_base_url,
                test_resource.to_string().strip_prefix("/").unwrap(),
            )
            .as_ref(),
        )?;

        let res = self
            .http_client
            .get(test_resource_url.clone())
            .send()
            .await?;
        let status = res.status();
        let text = res.text().await.map_err(|e| Error::from(e))?;

        if status.is_success() {
            Ok(text.to_owned())
        } else if status == reqwest::StatusCode::NOT_FOUND {
            Ok("null".to_string())
        } else {
            error!(
                "error occured while fetching test resource: {}",
                test_resource_url
            );
            Err(Error::ResourceFetchingError(test_resource_url.to_string()))
        }
    }
}

#[async_trait]
impl Requester<TestResource> for TestResourcesRequester {
    async fn process<'a, I: Iterator<Item = &'a TestResource> + Send + Sync>(
        &self,
        items: I,
        resources_repo: &TSResourcesRepoImpl,
        last_values: &TSUpdatesProviderLastValues<TestResource>,
    ) -> Result<(), Error> {
        stream::iter(items)
            .map(Ok)
            .try_for_each_concurrent(5, |test_resource| async move {
                let current_value = self.get(test_resource).await?;
                watchlist_process(test_resource, current_value, resources_repo, last_values)
                    .await?;
                Ok::<(), Error>(())
            })
            .await?;
        Ok(())
    }
}
