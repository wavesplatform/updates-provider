pub mod leasing_balance;
pub mod state;
pub mod transaction;

use async_trait::async_trait;
use itertools::Itertools;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::iter::FromIterator;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use wavesexchange_log::{debug, error, info, warn};
use wavesexchange_topic::Topic;

use super::super::watchlist::{WatchList, WatchListItem, WatchListUpdate};
use super::super::UpdatesProvider;
use crate::db::repo::RepoImpl;
use crate::db::BlockchainUpdate;
use crate::error::{Error, Result};
use crate::providers::watchlist::KeyWatchStatus;
use crate::resources::ResourcesRepo;
use crate::utils::clean_timeout;
use crate::waves::BlockMicroblockAppend;

pub trait Item: WatchListItem + Send + Sync + LastValue + DataFromBlock {}

pub struct Provider<T: Item, R: ResourcesRepo> {
    watchlist: Arc<RwLock<WatchList<T, R>>>,
    resources_repo: Arc<R>,
    rx: mpsc::Receiver<Arc<Vec<BlockchainUpdate>>>,
    repo: Arc<RepoImpl>,
    clean_timeout: Duration,
}

impl<T, R> Provider<T, R>
where
    T: Item + 'static,
    R: ResourcesRepo + Send + Sync + 'static,
{
    pub fn new(
        resources_repo: Arc<R>,
        delete_timeout: Duration,
        repo: Arc<RepoImpl>,
        rx: mpsc::Receiver<Arc<Vec<BlockchainUpdate>>>,
    ) -> Self {
        let watchlist = Arc::new(RwLock::new(WatchList::new(
            resources_repo.clone(),
            delete_timeout,
        )));
        let clean_timeout = clean_timeout(delete_timeout);
        Self {
            watchlist,
            resources_repo,
            rx,
            repo,
            clean_timeout,
        }
    }

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

    async fn process_updates(
        &mut self,
        blockchain_updates: Arc<Vec<BlockchainUpdate>>,
    ) -> Result<()> {
        for blockchain_update in blockchain_updates.iter() {
            match blockchain_update {
                BlockchainUpdate::Block(block) | BlockchainUpdate::Microblock(block) => {
                    self.check_and_process(block).await?
                }
                BlockchainUpdate::Rollback(_) => (),
            }
        }

        Ok(())
    }

    async fn check_and_process(&self, block: &BlockMicroblockAppend) -> Result<()> {
        for (current_value, data) in T::data_from_block(block) {
            let watch_status = self.watchlist.read().await.key_watch_status(&data);
            match watch_status {
                KeyWatchStatus::NotWatched => { /* Ignore */ }
                KeyWatchStatus::Watched => {
                    Self::watchlist_process(
                        &data,
                        current_value,
                        &self.resources_repo,
                        &self.watchlist,
                    )
                    .await?;
                }
                KeyWatchStatus::MatchesPattern(pattern_items) => {
                    Self::watchlist_process(
                        &data,
                        current_value,
                        &self.resources_repo,
                        &self.watchlist,
                    )
                    .await?;

                    for pattern_item in pattern_items {
                        append_subtopic_to_multitopic(
                            &self.resources_repo,
                            pattern_item,
                            data.clone(),
                            &self.watchlist,
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
impl<T, R> UpdatesProvider<T, R> for Provider<T, R>
where
    T: Item + 'static,
    R: ResourcesRepo + Send + Sync + 'static,
{
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
        resources_repo: &Arc<R>,
        watchlist: &Arc<RwLock<WatchList<T, R>>>,
    ) -> Result<()> {
        let resource: Topic = data.clone().into();
        info!("insert new value {:?}", resource);
        watchlist
            .write()
            .await
            .insert_value(data, current_value.clone());
        resources_repo.set_and_push(resource, current_value)?;
        Ok(())
    }
}

async fn check_and_maybe_insert<T: Item, R: ResourcesRepo>(
    resources_repo: &Arc<R>,
    repo: &Arc<RepoImpl>,
    value: T,
) -> Result<Option<HashSet<String>>> {
    let topic = value.clone().into();
    let existing_value = resources_repo.get(&topic)?;
    let need_to_publish = existing_value.is_none();
    let topic_value = if let Some(existing_value) = existing_value {
        existing_value
    } else {
        value.get_last(repo).await?
    };

    let subtopics = subtopics_from_topic_value(&topic, &topic_value)?;

    if let Some(ref subtopics) = subtopics {
        let mut missing_values = 0;
        for subtopic in subtopics {
            let subtopic = Topic::try_from(subtopic.as_str())
                .map_err(|_| Error::InvalidTopic(subtopic.clone()))?;
            if resources_repo.get(&subtopic)?.is_none() {
                missing_values += 1;
                let subtopic_value = T::maybe_item(&subtopic)
                    .ok_or_else(|| Error::InvalidTopic(subtopic.clone().into()))?;
                let new_value = subtopic_value.get_last(repo).await?;
                resources_repo.set_and_push(subtopic, new_value)?;
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
        resources_repo.set_and_push(topic, topic_value)?;
    }

    Ok(subtopics)
}

async fn append_subtopic_to_multitopic<T: Item, R: ResourcesRepo>(
    resources_repo: &Arc<R>,
    multitopic_item: T,
    subtopic_item: T,
    watchlist: &Arc<RwLock<WatchList<T, R>>>,
) -> Result<()> {
    let multitopic = multitopic_item.clone().into();
    let existing_value = resources_repo.get(&multitopic)?;
    if let Some(existing_value) = existing_value {
        let mut subtopics =
            subtopics_from_topic_value(&multitopic, &existing_value)?.expect("must be multitopic");
        let subtopic: Topic = subtopic_item.clone().into();
        let new_subtopic = String::from(subtopic);
        subtopics.insert(new_subtopic);
        let subtopics_str = {
            let subtopics_vec = subtopics.iter().cloned().collect_vec();
            serde_json::to_string(&subtopics_vec)?
        };
        watchlist
            .write()
            .await
            .update_multitopic(multitopic_item, subtopics);
        resources_repo.set_and_push(multitopic, subtopics_str)?;
    };
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
    fn data_from_block(block: &BlockMicroblockAppend) -> Vec<(String, Self)>;
}

#[async_trait]
pub trait LastValue {
    async fn get_last(self, repo: &Arc<RepoImpl>) -> Result<String>;
}
