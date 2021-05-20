pub mod handler;
pub mod height;
pub mod state;
pub mod transactions;
pub mod updater;

#[derive(Debug)]
pub struct Config {
    pub updates_url: String,
    pub transaction_delete_timeout: std::time::Duration,
    pub state_delete_timeout: std::time::Duration,
    pub updates_buffer_size: usize,
    pub transactions_count_threshold: usize,
    pub associated_addresses_count_threshold: usize,
    pub start_height: i32,
}
