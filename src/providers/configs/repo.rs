use super::ConfigsRepo;
use crate::error::Error;
use crate::models::ConfigFile;
use async_trait::async_trait;
use reqwest::{Client, ClientBuilder};
use wavesexchange_log::error;

pub struct ConfigsRepoImpl {
    configs_base_url: String,
    http_client: Client,
}

impl ConfigsRepoImpl {
    pub fn new(configs_base_url: impl AsRef<str>) -> Self {
        Self {
            configs_base_url: configs_base_url.as_ref().to_owned(),
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
        let config_file_url = reqwest::Url::parse(
            format!("{}/{}", self.configs_base_url, config_file_path).as_ref(),
        )?;

        let res = self.http_client.get(config_file_url).send().await?;
        let status = res.status();
        let text = res.text().await.map_err(|e| Error::from(e))?;

        if status.is_success() {
            Ok(text.to_owned())
        } else {
            error!("error while fetching config file: {}", text);
            Err(Error::ResourceFetchingError(config_file_path.to_owned()))
        }
    }
}
