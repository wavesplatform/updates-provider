use super::TestResourcesRepo;
use crate::error::Error;
use crate::models::TestResource;
use async_trait::async_trait;
use reqwest::{Client, ClientBuilder};
use wavesexchange_log::error;

pub struct TestResourcesRepoImpl {
    test_resources_base_url: String,
    http_client: Client,
}

impl TestResourcesRepoImpl {
    pub fn new(test_resources_base_url: impl AsRef<str>) -> Self {
        Self {
            test_resources_base_url: test_resources_base_url.as_ref().to_owned(),
            http_client: ClientBuilder::new()
                .timeout(std::time::Duration::from_secs(30))
                .connect_timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap(),
        }
    }
}

#[async_trait]
impl TestResourcesRepo for TestResourcesRepoImpl {
    async fn get(&self, test_resource: TestResource) -> Result<String, Error> {
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
        } else {
            error!(
                "error occured while fetching test resource: {}",
                test_resource_url
            );
            Err(Error::ResourceFetchingError(test_resource_url.to_string()))
        }
    }
}
