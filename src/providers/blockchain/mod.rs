pub mod height;
pub mod transactions;
pub mod updates;

#[derive(Debug)]
pub struct Config {
    pub updates_url: String,
    pub node_url: String,
    pub transaction_delete_timeout: std::time::Duration,
}
