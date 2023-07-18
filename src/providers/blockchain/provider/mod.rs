//! Module defining the data provider that gets data from blockchain updates.

pub mod exchange_pair;
pub mod leasing_balance;
pub mod state;
pub mod transaction;

use async_trait::async_trait;
use itertools::Itertools;
use std::collections::HashSet;
use std::iter::FromIterator;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use wavesexchange_log::{debug, error, info, warn};
use wx_topic::{Topic, TopicKind};

use super::super::watchlist::{WatchList, WatchListItem, WatchListUpdate};
use super::super::UpdatesProvider;
use crate::db::{repo_provider::ProviderRepo, BlockchainUpdate};
use crate::error::{Error, Result};
use crate::providers::watchlist::KeyWatchStatus;
use crate::resources::ResourcesRepo;
use crate::waves::{BlockMicroblockAppend, RollbackData};

/// Data item of the blockchain updates data provider.
///
/// Extends the following traits:
/// * `DataFromBlock` to extract the value from blockchain updates block
/// * `LastValue` to load the value from the database (aka `ProviderRepo`)
/// * `WatchListItem` to store the value in a watchlist
pub trait Item<R, C>:
    WatchListItem + Send + Sync + LastValue<R, Context = C> + DataFromBlock<Context = C>
where
    R: ProviderRepo,
{
}

// Blanket implementation of Item
impl<T, R, C> Item<R, C> for T
where
    T: Send + Sync,
    T: WatchListItem,
    T: LastValue<R, Context = C>,
    T: DataFromBlock<Context = C>,
    R: ProviderRepo,
{
}

/// Blockchain updates data provider.
///
/// Type arguments:
/// * `T` defines the value type returned by this provider: State, Transaction, LeasingBalance etc.
/// * `P` is the corresponding repo to load/save values from/to Postgres database
/// * `R` is the repo to publish values in Redis
/// * `S` is the shared state (context) that is available to every data item
pub struct Provider<T: Item<P, S>, R: ResourcesRepo + Clone, P: ProviderRepo, S> {
    watchlist: Arc<RwLock<WatchList<T, R>>>,
    resources_repo: R,
    rx: mpsc::Receiver<Arc<Vec<BlockchainUpdate>>>,
    repo: P,
    clean_timeout: Duration,
    shared_context: Arc<S>,
}

impl<T, R, P, S> Provider<T, R, P, S>
where
    T: Item<P, S> + 'static,
    R: ResourcesRepo + Clone + Send + Sync + 'static,
    P: ProviderRepo + Clone + Send + Sync + 'static,
{
    pub fn new(
        resources_repo: R,
        delete_timeout: Duration,
        repo: P,
        shared_context: S,
        rx: mpsc::Receiver<Arc<Vec<BlockchainUpdate>>>,
    ) -> Self {
        let watchlist = Arc::new(RwLock::new(WatchList::new(
            resources_repo.clone(),
            delete_timeout,
        )));
        let clean_timeout = utils::clean_timeout(delete_timeout);
        let shared_context = Arc::new(shared_context);
        Self {
            watchlist,
            resources_repo,
            rx,
            repo,
            clean_timeout,
            shared_context,
        }
    }

    #[cfg(test)]
    pub(crate) fn watchlist(&self) -> Arc<RwLock<WatchList<T, R>>> {
        self.watchlist.clone()
    }

    /// Blockchain updates receive loop + watchlist cleanup timer
    async fn run(&mut self) -> Result<()> {
        let mut interval = tokio::time::interval(self.clean_timeout);
        loop {
            tokio::select! {
                msg = self.rx.recv() => {
                    if let Some(blockchain_updates) = msg {
                        self.process_updates(blockchain_updates).await?;
                    } else {
                        break;
                    }
                }
                _ = interval.tick() => {
                    let mut watchlist_lock = self.watchlist.write().await;
                    watchlist_lock.delete_old().await;
                }
            }
        }
        Ok(())
    }

    /// Handles updates from blockchain and processes watched ones
    async fn process_updates(&self, blockchain_updates: Arc<Vec<BlockchainUpdate>>) -> Result<()> {
        for blockchain_update in blockchain_updates.iter() {
            match blockchain_update {
                BlockchainUpdate::Block(block) | BlockchainUpdate::Microblock(block) => {
                    self.process_block(block).await?
                }
                BlockchainUpdate::Rollback(rollback) => self.process_rollback(rollback).await?,
            }
        }

        Ok(())
    }

    async fn process_block(&self, block: &BlockMicroblockAppend) -> Result<()> {
        let repo = &self.resources_repo;
        let watchlist = &self.watchlist;
        for block_data in T::data_from_block(block, &self.shared_context) {
            let key = &block_data.data;
            let value = block_data.current_value;
            let watch_status = watchlist.read().await.key_watch_status(key);
            match watch_status {
                KeyWatchStatus::NotWatched => { /* Ignore */ }
                KeyWatchStatus::Watched => {
                    Self::watchlist_process(key, value, repo, watchlist).await?;
                }
                KeyWatchStatus::MatchesPattern(pattern_items) => {
                    Self::watchlist_process(key, value, repo, watchlist).await?;

                    for pattern_item in pattern_items {
                        append_subtopic_to_multitopic(
                            repo,
                            pattern_item,
                            block_data.data.clone(),
                            watchlist,
                        )
                        .await?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn process_rollback(&self, rollback: &RollbackData) -> Result<()> {
        let repo = &self.resources_repo;
        let watchlist = &self.watchlist;
        for block_data in T::data_from_rollback(rollback, &self.shared_context) {
            let key = &block_data.data;
            let value = block_data.current_value;
            let watch_status = watchlist.read().await.key_watch_status(key);
            //TODO should it be different for rollback? (if not - deduplicate)
            match watch_status {
                KeyWatchStatus::NotWatched => { /* Ignore */ }
                KeyWatchStatus::Watched => {
                    Self::watchlist_process(key, value, repo, watchlist).await?;
                }
                KeyWatchStatus::MatchesPattern(pattern_items) => {
                    Self::watchlist_process(key, value, repo, watchlist).await?;

                    for pattern_item in pattern_items {
                        append_subtopic_to_multitopic(
                            repo,
                            pattern_item,
                            block_data.data.clone(),
                            watchlist,
                        )
                        .await?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn watchlist_process(
        data: &T,
        current_value: String,
        resources_repo: &R,
        watchlist: &RwLock<WatchList<T, R>>,
    ) -> Result<()> {
        // A.K.: the corresponding implementation in polling provider is more complex,
        // it contains checks whether the value actually changed,
        // in both watchlist and Redis repo. It is unclear why the implementations are different,
        // and why these checks are dropped here. Maybe it is performance-related? Not sure.
        let resource = data.clone().into().as_topic(); //TODO bad design - cloning is not necessary
        info!("insert new value {:?}", resource);
        watchlist
            .write()
            .await
            .insert_value(data, current_value.clone());
        resources_repo
            .set_and_push(&resource, current_value)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl<T, R, P, S> UpdatesProvider<T> for Provider<T, R, P, S>
where
    T: Item<P, S> + 'static,
    R: ResourcesRepo + Clone + Send + Sync + 'static,
    P: ProviderRepo + Clone + Send + Sync + 'static,
    S: Send + Sync + 'static,
{
    /// Run 2 async tasks: handler of subscribe/unsubscribe events & handler of blockchain updates
    async fn fetch_updates(mut self) -> Result<mpsc::Sender<WatchListUpdate<T>>> {
        let (subscriptions_updates_sender, mut subscriptions_updates_receiver) = mpsc::channel(20);

        // Run handler of subscribe/unsubscribe events
        let watchlist = self.watchlist.clone();
        let resources_repo = self.resources_repo.clone();
        let repo = self.repo.clone();
        let shared_context = self.shared_context.clone();
        tokio::task::spawn(async move {
            info!("starting subscriptions updates handler");
            while let Some(upd) = subscriptions_updates_receiver.recv().await {
                debug!("Subscription: {:?}", upd);
                if let Err(err) = watchlist.write().await.on_update(&upd) {
                    error!("error while updating watchlist: {:?}", err);
                }
                if let WatchListUpdate::Updated { item } = upd {
                    let result = check_and_maybe_insert(
                        &resources_repo,
                        &repo,
                        item.clone(),
                        &shared_context,
                    )
                    .await;
                    match result {
                        Ok(None) => { /* Nothing more to do */ }
                        Ok(Some(subtopics)) => {
                            watchlist.write().await.update_multitopic(item, subtopics);
                        }
                        Err(err) => {
                            error!("error while updating value: {:?}", err);
                        }
                    }
                }
            }
        });

        // Run handler of blockchain updates + watchlist cleanup timer
        tokio::task::spawn(async move {
            let data_type = std::any::type_name::<T>();
            info!("starting {} provider", data_type);
            if let Err(error) = self.run().await {
                error!("provider for {} returned error: {:?}", data_type, error);
            }
        });

        Ok(subscriptions_updates_sender)
    }
}

/// Called when a subscription is added to the watchlist.
/// Ensures that Redis contains the latest value of the subscription's topic,
/// publishes if necessary.
async fn check_and_maybe_insert<T: Item<P, S>, R: ResourcesRepo + Sync, P: ProviderRepo, S>(
    resources_repo: &R,
    repo: &P,
    value: T,
    shared_context: &S,
) -> Result<Option<HashSet<String>>> {
    let topic = value.clone().into().as_topic();
    let existing_value = resources_repo.get(&topic).await?;

    // This is a hack for the ExchangePair topics.
    // When updates-provider restarts, there may be a stale value in Redis, which needs to be refreshed.
    // For other topics this is ok, because the value that is loaded from the database would be the same.
    // But ExchangePair topics are time-dependent (transactions gets evicted from the time window),
    // so potentially we need to recompute (and republish) the value.
    let first_loaded = if topic.kind() == TopicKind::ExchangePair {
        value.init_context(repo, shared_context).await?
    } else {
        false
    };

    let (topic_value, need_to_publish) = match (existing_value, first_loaded) {
        (None, _) => {
            let topic_value = value.last_value(repo, shared_context).await?;
            (topic_value, true)
        }
        (Some(existing_value), false) => (existing_value, false),
        (Some(existing_value), true) => {
            // Recompute time-dependent value
            let topic_value = value.last_value(repo, shared_context).await?;
            let need_to_publish = existing_value != topic_value;
            (topic_value, need_to_publish)
        }
    };

    let subtopics = subtopics_from_topic_value(&topic, &topic_value)?;

    if let Some(ref subtopics) = subtopics {
        let mut missing_values = 0;
        for subtopic in subtopics {
            let subtopic = Topic::parse_str(subtopic.as_str())
                .map_err(|_| Error::InvalidTopic(subtopic.clone()))?;
            if resources_repo.get(&subtopic).await?.is_none() {
                missing_values += 1;
                let subtopic_value = T::maybe_item(&subtopic)
                    .ok_or_else(|| Error::InvalidTopic(subtopic.to_string()))?;
                let new_value = subtopic_value.last_value(repo, shared_context).await?;
                resources_repo.set_and_push(&subtopic, new_value).await?;
            }
        }
        // This is odd when some subtopics exist in redis and some don't
        if subtopics.len() > 0 && missing_values > 0 && missing_values < subtopics.len() {
            warn!(
                "Just refreshed multitopic with {} subtopics, of which {} were missing",
                subtopics.len(),
                missing_values,
            );
        }
    }

    if need_to_publish {
        resources_repo.set_and_push(&topic, topic_value).await?;
    }

    Ok(subtopics)
}

async fn append_subtopic_to_multitopic<T, R, P, S>(
    resources_repo: &R,
    multitopic_item: T,
    subtopic_item: T,
    watchlist: &Arc<RwLock<WatchList<T, R>>>,
) -> Result<()>
where
    T: Item<P, S>,
    R: ResourcesRepo + Sync,
    P: ProviderRepo,
{
    let multitopic = multitopic_item.clone().into().as_topic(); //TODO bad design - cloning is not necessary
    let existing_value = resources_repo.get(&multitopic).await?;
    let mut subtopics = if let Some(existing_value) = existing_value {
        subtopics_from_topic_value(&multitopic, &existing_value)?.expect("must be multitopic")
    } else {
        HashSet::new()
    };
    let subtopic = subtopic_item.clone().into(); //TODO bad design - cloning is not necessary
    let new_subtopic = subtopic.as_uri_string();
    if subtopics.contains(&new_subtopic) {
        return Ok(());
    }
    subtopics.insert(new_subtopic);
    let subtopics_str = {
        let mut subtopics_vec = subtopics.iter().cloned().collect_vec();
        subtopics_vec.sort(); // Stable result, good for tests
        serde_json::to_string(&subtopics_vec)?
    };
    watchlist
        .write()
        .await
        .update_multitopic(multitopic_item, subtopics);
    resources_repo
        .set_and_push(&multitopic, subtopics_str)
        .await?;
    Ok(())
}

fn subtopics_from_topic_value(topic: &Topic, value: &str) -> Result<Option<HashSet<String>>> {
    Ok(if topic.is_multi_topic() {
        let vec = serde_json::from_str::<Vec<String>>(value)?;
        let set = HashSet::from_iter(vec);
        Some(set)
    } else {
        None
    })
}

/// This trait must be implemented on every type that represents a subscription topic.
///
/// Methods of this trait extracts data associated with this topic from a blockchain update/rollback.
pub trait DataFromBlock
where
    Self: Sized,
{
    /// Some context that is shared among all the topics of the same type.
    type Context;

    /// Extract data from a block or microblock.
    fn data_from_block(block: &BlockMicroblockAppend, ctx: &Self::Context) -> Vec<BlockData<Self>>;

    /// Extract data from a rollback.
    fn data_from_rollback(rollback: &RollbackData, ctx: &Self::Context) -> Vec<BlockData<Self>>;
}

pub struct BlockData<T> {
    /// Topic (internal representation)
    pub data: T,
    /// Topic's value (as JSON)
    pub current_value: String,
}

impl<T> BlockData<T> {
    pub fn new(current_value: String, data: T) -> Self {
        BlockData {
            data,
            current_value,
        }
    }
}

/// This trait must be implemented on every type that represents a subscription topic.
///
/// Methods of this trait loads the last know value for this topic from the database.
#[async_trait]
pub trait LastValue<R: ProviderRepo> {
    /// Some context that is shared among all the topics of the same type.
    type Context;

    /// Read last known value of this topic from the database.
    async fn last_value(self, repo: &R, ctx: &Self::Context) -> Result<String>;

    /// Initialize the shared context before first use,
    /// called at least once for every unique topic.
    ///
    /// Currently used for `TopicKind::ExchangePair` only.
    /// This is kind of a hack for the architecture that does not natively support
    /// values recomputed on-the-fly.
    async fn init_context(&self, repo: &R, ctx: &Self::Context) -> Result<bool>;
}

#[cfg(test)]
mod tests;

mod utils {
    use std::time::Duration;

    pub(super) fn clean_timeout(delete_timeout: Duration) -> Duration {
        let temp = delete_timeout / 2;
        let minimum = Duration::from_secs(15);
        if temp > minimum {
            temp
        } else {
            minimum
        }
    }
}
