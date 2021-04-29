pub mod height;
pub mod puller;
pub mod states;
pub mod transactions;
pub mod updater;

#[derive(Debug)]
pub struct Config {
    pub updates_url: String,
    pub transaction_delete_timeout: std::time::Duration,
    pub updates_buffer_size: usize,
    pub transactions_count_threshold: usize,
    pub associated_addresses_count_threshold: usize,
    pub start_height: i32,
}
