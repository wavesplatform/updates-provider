pub mod handler;
pub mod height;
pub mod provider;
pub mod updater;
use std::time::Duration;

#[derive(Debug)]
pub struct Config {
    pub updates_url: String,
    pub transaction_delete_timeout: Duration,
    pub state_delete_timeout: Duration,
    pub leasing_balance_delete_timeout: Duration,
    pub updates_buffer_size: usize,
    pub transactions_count_threshold: usize,
    pub associated_addresses_count_threshold: usize,
    pub start_height: i32,
    pub waiting_blocks_timeout: Duration,
    pub start_rollback_depth: u32,
}
