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
use wavesexchange_topic::TopicKind;
use wx_topic::Topic;

use self::exchange_pair::ExchangePairsStorageProviderRepoTrait;

use super::super::watchlist::{WatchList, WatchListItem, WatchListUpdate};
use super::super::UpdatesProvider;
use crate::db::{repo_provider::ProviderRepo, BlockchainUpdate};
use crate::error::{Error, Result};
use crate::providers::watchlist::KeyWatchStatus;
use crate::resources::ResourcesRepo;
use crate::waves::{BlockMicroblockAppend, RollbackData};

pub trait Item<R: ProviderRepo>:
    WatchListItem + Send + Sync + LastValue<R> + DataFromBlock
{
}

pub struct Provider<T: Item<P>, R: ResourcesRepo, P: ProviderRepo + Clone> {
    watchlist: Arc<RwLock<WatchList<T, R>>>,
    resources_repo: Arc<R>,
    rx: mpsc::Receiver<Arc<Vec<BlockchainUpdate>>>,
    repo: P,
    clean_timeout: Duration,
}

impl<T, R, P> Provider<T, R, P>
where
    T: Item<P> + 'static,
    R: ResourcesRepo + Send + Sync + 'static,
    P: ProviderRepo + Clone + Send + Sync + 'static,
{
    pub fn new(
        resources_repo: Arc<R>,
        delete_timeout: Duration,
        repo: P,
        rx: mpsc::Receiver<Arc<Vec<BlockchainUpdate>>>,
    ) -> Self {
        let watchlist = Arc::new(RwLock::new(WatchList::new(
            resources_repo.clone(),
            delete_timeout,
        )));
        let clean_timeout = utils::clean_timeout(delete_timeout);
        Self {
            watchlist,
            resources_repo,
            rx,
            repo,
            clean_timeout,
        }
    }

    /// Blockchain updates receive loop
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

    #[cfg(test)]
    pub fn watchlist(&self) -> Arc<RwLock<WatchList<T, R>>> {
        self.watchlist.clone()
    }

    /// Handles updates from blockchain and processes watched ones
    async fn process_updates(
        &mut self,
        blockchain_updates: Arc<Vec<BlockchainUpdate>>,
    ) -> Result<()> {
        for blockchain_update in blockchain_updates.iter() {
            match blockchain_update {
                BlockchainUpdate::Block(block) => self.process_block(block).await?,
                BlockchainUpdate::Microblock(block) => {
                    crate::EXCHANGE_PAIRS_STORAGE
                        .load_blocks_rowlog(&self.repo)
                        .await?;

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
        for block_data in T::data_from_block(block) {
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
        for block_data in T::data_from_rollback(rollback) {
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
}

#[async_trait]
impl<T, R, P> UpdatesProvider<T, R> for Provider<T, R, P>
where
    T: Item<P> + 'static,
    R: ResourcesRepo + Send + Sync + 'static,
    P: ProviderRepo + Clone + Send + Sync + 'static,
{
    /// Run 2 async tasks: handler of subscribe/unsubscribe events & handler of blockchain updates
    async fn fetch_updates(mut self) -> Result<mpsc::Sender<WatchListUpdate<T>>> {
        let (subscriptions_updates_sender, mut subscriptions_updates_receiver) = mpsc::channel(20);

        let watchlist = self.watchlist.clone();
        let resources_repo = self.resources_repo.clone();
        let repo = self.repo.clone();
        tokio::task::spawn(async move {
            info!("starting subscriptions updates handler");
            while let Some(upd) = subscriptions_updates_receiver.recv().await {
                debug!("Subscription: {:?}", upd);
                if let Err(err) = watchlist.write().await.on_update(&upd) {
                    error!("error while updating watchlist: {:?}", err);
                }
                if let WatchListUpdate::Updated { item } = upd {
                    let result = check_and_maybe_insert(&resources_repo, &repo, item.clone()).await;
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

        tokio::task::spawn(async move {
            info!("starting provider");
            if let Err(error) = self.run().await {
                error!("transaction provider return error: {:?}", error);
            }
        });

        Ok(subscriptions_updates_sender)
    }

    async fn watchlist_process(
        data: &T,
        current_value: String,
        resources_repo: &R,
        watchlist: &RwLock<WatchList<T, R>>,
    ) -> Result<()> {
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

async fn check_and_maybe_insert<T: Item<P>, R: ResourcesRepo + Sync, P: ProviderRepo>(
    resources_repo: &Arc<R>,
    repo: &P,
    value: T,
) -> Result<Option<HashSet<String>>> {
    let topic = value.clone().into().as_topic();
    let mut existing_value = resources_repo.get(&topic).await?;
    let mut need_to_publish = existing_value.is_none();

    if topic.kind() == TopicKind::ExchangePair {
        let loaded = value.init_last_value(repo.clone()).await?;
        if loaded {
            //if pairs is loaded into EXCHANGE_PAIRS_STORAGE then make redis value invalid and need to replaced by last_value call
            need_to_publish = true;
            existing_value = None;
        }
    }

    let topic_value = if let Some(existing_value) = existing_value {
        existing_value
    } else {
        value.last_value(repo).await?
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
                let new_value = subtopic_value.last_value(repo).await?;
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

async fn append_subtopic_to_multitopic<T: Item<P>, R: ResourcesRepo + Sync, P: ProviderRepo>(
    resources_repo: &Arc<R>,
    multitopic_item: T,
    subtopic_item: T,
    watchlist: &Arc<RwLock<WatchList<T, R>>>,
) -> Result<()> {
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

pub trait DataFromBlock: Sized {
    fn data_from_block(block: &BlockMicroblockAppend) -> Vec<BlockData<Self>>;
    fn data_from_rollback(rollback: &RollbackData) -> Vec<BlockData<Self>>;
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

#[async_trait]
pub trait LastValue<R: ProviderRepo> {
    async fn last_value(self, repo: &R) -> Result<String>;

    // function init_last_value called before last_value
    // it need to load exchange_pairs::ExchangePairsStorage exchange pairs data
    // from database only once per amount_asset/price_asset
    // after restart updates provider
    async fn init_last_value(&self, _repo: &R) -> Result<bool>;
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
