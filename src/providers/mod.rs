pub mod blockchain;
pub mod polling;
pub mod watchlist;

use crate::error::Error;
use async_trait::async_trait;
use tokio::sync::mpsc;
use watchlist::{WatchListItem, WatchListUpdate};

/// Async trait that defines a background process of receiving new updates
/// as well as handling of subscribe/unsubscribe events.
#[async_trait]
pub trait UpdatesProvider<T>
where
    T: WatchListItem + Clone + Send + Sync,
{
    async fn fetch_updates(self) -> Result<mpsc::Sender<WatchListUpdate<T>>, Error>;
}
