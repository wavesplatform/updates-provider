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

fn default_updates_buffer_size() -> usize {
    10
}

fn default_transactions_count_threshold() -> usize {
    1000
}

fn default_associated_addresses_count_threshold() -> usize {
    1000
}

fn default_state_batch_size() -> usize {
    100
}

fn default_start_height() -> i32 {
    0
}

#[derive(Deserialize)]
pub struct RedisConfig {
    pub host: String,
    #[serde(default = "default_redis_port")]
    pub port: u16,
    pub username: String,
    pub password: String,
}

fn default_pgport() -> u16 {
    5432
}

#[derive(Deserialize)]
pub struct PostgresConfig {
    pub host: String,
    #[serde(default = "default_pgport")]
    pub port: u16,
    pub database: String,
    pub user: String,
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

#[derive(Deserialize)]
struct FlatBlockchainUpdaterConfig {
    pub blockchain_updates_node_url: String,
    #[serde(default = "default_delete_timeout")]
    pub transaction_delete_timeout: u64,
    #[serde(default = "default_updates_buffer_size")]
    pub updates_buffer_size: usize,
    #[serde(default = "default_transactions_count_threshold")]
    pub transactions_count_threshold: usize,
    #[serde(default = "default_associated_addresses_count_threshold")]
    pub associated_addresses_count_threshold: usize,
    #[serde(default = "default_start_height")]
    pub start_height: i32,
}

pub fn load_redis() -> Result<RedisConfig, Error> {
    envy::prefixed("REDIS__")
        .from_env::<RedisConfig>()
        .map_err(|err| Error::from(err))
}

pub fn load_postgres() -> Result<PostgresConfig, Error> {
    envy::prefixed("POSTGRES__")
        .from_env::<PostgresConfig>()
        .map_err(|err| Error::from(err))
}

pub fn load_subscriptions() -> Result<subscriptions::Config, Error> {
    let flat_config = envy::prefixed("SUBSCRIPTIONS__").from_env::<FlatSubscriptionsConfig>()?;

    Ok(subscriptions::Config {
        subscriptions_key: flat_config.key,
    })
}

pub fn load_configs_updater() -> Result<providers::polling::configs::Config, Error> {
    let flat_config = envy::prefixed("CONFIGS_UPDATER__").from_env::<FlatConfigsUpdaterConfig>()?;

    Ok(providers::polling::configs::Config {
        configs_base_url: flat_config.configs_base_url,
        polling_delay: Duration::from_secs(flat_config.polling_delay),
        gitlab_private_token: flat_config.gitlab_private_token,
        gitlab_configs_branch: flat_config.gitlab_configs_branch,
        delete_timeout: Duration::from_secs(flat_config.delete_timeout_secs),
    })
}

pub fn load_states_updater() -> Result<providers::polling::states::Config, Error> {
    let flat_config = envy::prefixed("STATES_UPDATER__").from_env::<FlatStatesUpdaterConfig>()?;

    Ok(providers::polling::states::Config {
        base_url: flat_config.base_url,
        polling_delay: Duration::from_secs(flat_config.polling_delay),
        delete_timeout: Duration::from_secs(flat_config.delete_timeout_secs),
        batch_size: flat_config.batch_size,
    })
}

pub fn load_test_resources_updater() -> Result<providers::polling::test_resources::Config, Error> {
    let flat_config =
        envy::prefixed("TEST_RESOURCES_UPDATER__").from_env::<FlatTestResourcesUpdaterConfig>()?;

    Ok(providers::polling::test_resources::Config {
        test_resources_base_url: flat_config.test_resources_base_url,
        polling_delay: Duration::from_secs(flat_config.polling_delay),
        delete_timeout: Duration::from_secs(flat_config.delete_timeout_secs),
    })
}

pub fn load_blockchain() -> Result<providers::blockchain::Config, Error> {
    let flat_config = envy::prefixed("NODE_UPDATER__").from_env::<FlatBlockchainUpdaterConfig>()?;

    Ok(providers::blockchain::Config {
        updates_url: flat_config.blockchain_updates_node_url,
        transaction_delete_timeout: Duration::from_secs(flat_config.transaction_delete_timeout),
        updates_buffer_size: flat_config.updates_buffer_size,
        transactions_count_threshold: flat_config.transactions_count_threshold,
        associated_addresses_count_threshold: flat_config.associated_addresses_count_threshold,
        start_height:  flat_config.start_height,
    })
}
