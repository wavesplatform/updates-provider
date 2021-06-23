use crate::error::Error;
use crate::providers;
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

fn default_start_height() -> i32 {
    0
}

fn default_port() -> u16 {
    8080
}

fn default_waiting_blocks_timeout() -> u64 {
    15
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

fn default_pg_pool_size() -> u8 {
    4
}

#[derive(Deserialize)]
pub struct PostgresConfig {
    pub host: String,
    #[serde(default = "default_pgport")]
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    #[serde(default = "default_pg_pool_size")]
    pub pool_size: u8,
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
struct FlatTestResourcesUpdaterConfig {
    pub test_resources_base_url: String,
    pub polling_delay: u64,
    #[serde(default = "default_delete_timeout")]
    pub delete_timeout_secs: u64,
}

#[derive(Deserialize)]
struct FlatBlockchainUpdaterConfig {
    pub url: String,
    #[serde(default = "default_delete_timeout")]
    pub transaction_delete_timeout: u64,
    #[serde(default = "default_delete_timeout")]
    pub state_delete_timeout: u64,
    #[serde(default = "default_delete_timeout")]
    pub leasing_balance_delete_timeout: u64,
    #[serde(default = "default_updates_buffer_size")]
    pub updates_buffer_size: usize,
    #[serde(default = "default_transactions_count_threshold")]
    pub transactions_count_threshold: usize,
    #[serde(default = "default_associated_addresses_count_threshold")]
    pub associated_addresses_count_threshold: usize,
    #[serde(default = "default_start_height")]
    pub start_height: i32,
    #[serde(default = "default_waiting_blocks_timeout")]
    pub waiting_blocks_timeout: u64,
}

#[derive(Deserialize)]
struct FlatServerConfig {
    #[serde(default = "default_port")]
    pub port: u16,
}

pub fn load_redis() -> Result<RedisConfig, Error> {
    envy::prefixed("REDIS__")
        .from_env::<RedisConfig>()
        .map_err(Error::from)
}

pub fn load_postgres() -> Result<PostgresConfig, Error> {
    envy::prefixed("POSTGRES__")
        .from_env::<PostgresConfig>()
        .map_err(Error::from)
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
    let flat_config =
        envy::prefixed("BLOCKCHAIN_UPDATES__").from_env::<FlatBlockchainUpdaterConfig>()?;

    Ok(providers::blockchain::Config {
        updates_url: flat_config.url,
        transaction_delete_timeout: Duration::from_secs(flat_config.transaction_delete_timeout),
        state_delete_timeout: Duration::from_secs(flat_config.state_delete_timeout),
        leasing_balance_delete_timeout: Duration::from_secs(
            flat_config.leasing_balance_delete_timeout,
        ),
        updates_buffer_size: flat_config.updates_buffer_size,
        transactions_count_threshold: flat_config.transactions_count_threshold,
        associated_addresses_count_threshold: flat_config.associated_addresses_count_threshold,
        start_height: flat_config.start_height,
        waiting_blocks_timeout: Duration::from_secs(flat_config.waiting_blocks_timeout),
    })
}

pub fn load_api() -> Result<crate::api::Config, Error> {
    let flat_config = envy::prefixed("SERVER__").from_env::<FlatServerConfig>()?;

    Ok(crate::api::Config {
        port: flat_config.port,
    })
}
