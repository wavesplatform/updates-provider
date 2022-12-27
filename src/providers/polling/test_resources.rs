use super::Requester;
use crate::error::Error;
use async_trait::async_trait;
use reqwest::{Client, ClientBuilder};
use std::time::Duration;
use wavesexchange_log::error;
use wx_topic::{TestResource, TopicData};

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
}

#[async_trait]
impl Requester<TestResource> for TestResourcesRequester {
    async fn get(&self, test_resource: &TestResource) -> Result<String, Error> {
        let topic_data: TopicData = test_resource.clone().into();
        let test_resource_url = reqwest::Url::parse(
            format!(
                "{}/{}",
                self.test_resources_base_url,
                topic_data.as_uri_string()
                    .strip_prefix("topic://test_resource/")
                    .unwrap(),
            )
            .as_ref(),
        )?;

        let res = self
            .http_client
            .get(test_resource_url.clone())
            .send()
            .await?;
        let status = res.status();
        let text = res.text().await?;

        if status.is_success() {
            Ok(text)
        } else if status == reqwest::StatusCode::NOT_FOUND {
            Ok("null".to_string())
        } else {
            error!(
                "error occurred while fetching test resource: {}",
                test_resource_url
            );
            Err(Error::ResourceFetchingError(test_resource_url.to_string()))
        }
    }
}
