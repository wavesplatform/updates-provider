use super::ConfigsRepo;
use crate::error::Error;
use crate::models::ConfigFile;
use async_trait::async_trait;
use reqwest::{Client, ClientBuilder};
use wavesexchange_log::{debug, error};

pub struct ConfigsRepoImpl {
    configs_base_url: String,
    gitlab_private_token: String,
    gitlab_configs_branch: String,
    http_client: Client,
}

impl ConfigsRepoImpl {
    pub fn new(
        configs_base_url: impl AsRef<str>,
        private_token: impl AsRef<str>,
        configs_branch: impl AsRef<str>,
    ) -> Self {
        Self {
            configs_base_url: configs_base_url.as_ref().to_owned(),
            gitlab_private_token: private_token.as_ref().to_owned(),
            gitlab_configs_branch: configs_branch.as_ref().to_owned(),
            http_client: ClientBuilder::new()
                .timeout(std::time::Duration::from_secs(30))
                .connect_timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap(),
        }
    }
}

#[async_trait]
impl ConfigsRepo for ConfigsRepoImpl {
    async fn get(&self, config_file: ConfigFile) -> Result<String, Error> {
        let config_file_path = config_file.to_string();
        let config_file_path = config_file_path.trim_start_matches("/");
        let config_file_path = percent_encoding::percent_encode(
            config_file_path.as_bytes(),
            percent_encoding::NON_ALPHANUMERIC,
        )
        .to_string();

        let config_file_url = reqwest::Url::parse(
            format!(
                "{}/{}/raw?ref={}",
                self.configs_base_url, config_file_path, self.gitlab_configs_branch
            )
            .as_ref(),
        )?;

        let res = self
            .http_client
            .get(config_file_url.clone())
            .header("PRIVATE-TOKEN", self.gitlab_private_token.clone())
            .send()
            .await?;
        let status = res.status();
        let text = res.text().await.map_err(|e| Error::from(e))?;
        debug!("url = {}, status = {}", config_file_url, status);
        if status.is_success() {
            Ok(text.to_owned())
        } else {
            error!(
                "error occured while fetching config file: {}",
                config_file_path
            );
            Err(Error::ResourceFetchingError(config_file_path.to_owned()))
        }
    }
}
