use crate::error::Error;
use crate::{providers, subscriptions};
use serde::Deserialize;

fn default_redis_port() -> u16 {
    6379
}

#[derive(Deserialize)]
pub struct RedisConfig {
    pub host: String,
    #[serde(default = "default_redis_port")]
    pub port: u16,
    pub username: String,
    pub password: String,
}

#[derive(Deserialize)]
struct FlatSubscriptionsConfig {
    pub key: String,
}

#[derive(Deserialize)]
struct FlatConfigsUpdaterConfig {
    pub configs_base_url: String,
    pub polling_delay: u64,
}

#[derive(Deserialize)]
struct FlatConfigsFromGitlabUpdaterConfig {
    pub configs_base_url: String,
    pub polling_delay: u64,
    pub gitlab_private_token: String,
    pub gitlab_configs_branch: String,
}

pub fn load_redis() -> Result<RedisConfig, Error> {
    envy::prefixed("REDIS__")
        .from_env::<RedisConfig>()
        .map_err(|err| Error::from(err))
}

pub fn load_subscriptions() -> Result<subscriptions::Config, Error> {
    let flat_config = envy::prefixed("SUBSCRIPTIONS__").from_env::<FlatSubscriptionsConfig>()?;

    Ok(subscriptions::Config {
        subscriptions_key: flat_config.key,
    })
}

pub fn load_configs_updater() -> Result<providers::configs::Config, Error> {
    let flat_config = envy::prefixed("CONFIGS_UPDATER__").from_env::<FlatConfigsUpdaterConfig>()?;

    Ok(providers::configs::Config {
        configs_base_url: flat_config.configs_base_url,
        polling_delay: std::time::Duration::from_secs(flat_config.polling_delay),
    })
}

pub fn load_configs_from_gitlab_updater() -> Result<providers::configs_from_gitlab::Config, Error> {
    let flat_config = envy::prefixed("CONFIGS_FROM_GITLAB_UPDATER__")
        .from_env::<FlatConfigsFromGitlabUpdaterConfig>()?;

    Ok(providers::configs_from_gitlab::Config {
        configs_base_url: flat_config.configs_base_url,
        polling_delay: std::time::Duration::from_secs(flat_config.polling_delay),
        gitlab_private_token: flat_config.gitlab_private_token,
        gitlab_configs_branch: flat_config.gitlab_configs_branch,
    })
}
