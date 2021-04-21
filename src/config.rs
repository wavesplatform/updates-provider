use crate::error::Error;
use crate::{providers, subscriptions};
use serde::Deserialize;
use std::time::Duration;

fn default_redis_port() -> u16 {
    6379
}

fn default_delete_timeout() -> u64 {
    60
}

fn default_state_batch_size() -> usize {
    100
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
    pub gitlab_private_token: String,
    pub gitlab_configs_branch: String,
    #[serde(default = "default_delete_timeout")]
    pub delete_timeout_secs: u64,
}

#[derive(Deserialize)]
struct FlatStatesUpdaterConfig {
    pub base_url: String,
    pub polling_delay: u64,
    #[serde(default = "default_delete_timeout")]
    pub delete_timeout_secs: u64,
    #[serde(default = "default_state_batch_size")]
    pub batch_size: usize,
}

#[derive(Deserialize)]
struct FlatTestResourcesUpdaterConfig {
    pub test_resources_base_url: String,
    pub polling_delay: u64,
    #[serde(default = "default_delete_timeout")]
    pub delete_timeout_secs: u64,
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
        polling_delay: Duration::from_secs(flat_config.polling_delay),
        gitlab_private_token: flat_config.gitlab_private_token,
        gitlab_configs_branch: flat_config.gitlab_configs_branch,
        delete_timeout: Duration::from_secs(flat_config.delete_timeout_secs),
    })
}

pub fn load_states_updater() -> Result<providers::states::Config, Error> {
    let flat_config = envy::prefixed("STATES_UPDATER__").from_env::<FlatStatesUpdaterConfig>()?;

    Ok(providers::states::Config {
        base_url: flat_config.base_url,
        polling_delay: Duration::from_secs(flat_config.polling_delay),
        delete_timeout: Duration::from_secs(flat_config.delete_timeout_secs),
        batch_size: flat_config.batch_size,
    })
}

pub fn load_test_resources_updater() -> Result<providers::test_resources::Config, Error> {
    let flat_config =
        envy::prefixed("TEST_RESOURCES_UPDATER__").from_env::<FlatTestResourcesUpdaterConfig>()?;

    Ok(providers::test_resources::Config {
        test_resources_base_url: flat_config.test_resources_base_url,
        polling_delay: Duration::from_secs(flat_config.polling_delay),
        delete_timeout: Duration::from_secs(flat_config.delete_timeout_secs),
    })
}
