use super::Requester;
use crate::error::Error;
use async_trait::async_trait;
use reqwest::{Client, ClientBuilder};
use std::time::Duration;
use wavesexchange_log::{debug, error};
use wx_topic::ConfigFile;

#[async_trait]
pub trait ConfigsRepo {
    async fn get(&self, config_file: ConfigFile) -> Result<String, Error>;
}

#[derive(Clone, Debug)]
pub struct Config {
    pub configs_base_url: String,
    pub polling_delay: Duration,
    pub gitlab_private_token: String,
    pub gitlab_configs_branch: String,
    pub delete_timeout: Duration,
}

pub struct ConfigRequester {
    gitlab_private_token: String,
    gitlab_configs_branch: String,
    base_url: String,
    http_client: Client,
}

impl ConfigRequester {
    pub fn new(config: Config) -> Self {
        Self {
            base_url: config.configs_base_url.to_owned(),
            http_client: ClientBuilder::new()
                .timeout(std::time::Duration::from_secs(30))
                .connect_timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap(),
            gitlab_private_token: config.gitlab_private_token,
            gitlab_configs_branch: config.gitlab_configs_branch,
        }
    }
}

#[async_trait]
impl Requester<ConfigFile> for ConfigRequester {
    async fn get(&self, config_file: &ConfigFile) -> Result<String, Error> {
        let config_file_path = &config_file.path;
        let config_file_path = config_file_path.trim_start_matches("config/");
        let config_file_path = config_file_path.trim_start_matches("/");
        let config_file_path = percent_encoding::percent_encode(
            config_file_path.as_bytes(),
            percent_encoding::NON_ALPHANUMERIC,
        )
        .to_string();

        let config_file_url = reqwest::Url::parse(
            format!(
                "{}/{}/raw?ref={}",
                self.base_url, config_file_path, self.gitlab_configs_branch
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
        let text = res.text().await?;
        debug!("url = {}, status = {}", config_file_url, status);
        if status.is_success() {
            Ok(text)
        } else if status == reqwest::StatusCode::NOT_FOUND {
            Ok("null".to_string())
        } else {
            error!(
                "error occurred while fetching config file: {}",
                config_file_path
            );
            Err(Error::ResourceFetchingError(config_file_path.to_owned()))
        }
    }
}
