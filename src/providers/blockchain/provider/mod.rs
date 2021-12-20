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
use tracing_futures::Instrument;
use wavesexchange_log::{debug, error, info, warn};
use wavesexchange_topic::Topic;

use super::super::watchlist::{WatchList, WatchListItem, WatchListUpdate};
use super::super::UpdatesProvider;
use crate::db::{repo_provider::ProviderRepo, BlockchainUpdate};
use crate::error::{Error, Result};
use crate::providers::watchlist::{KeyWatchStatus, TracingContext};
use crate::resources::ResourcesRepo;
use crate::waves::BlockMicroblockAppend;

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
                    watchlist_lock.delete_old();
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn watchlist(&self) -> Arc<RwLock<WatchList<T, R>>> {
        self.watchlist.clone()
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
impl<T, R, P> UpdatesProvider<T, R> for Provider<T, R, P>
where
    T: Item<P> + 'static,
    R: ResourcesRepo + Send + Sync + 'static,
    P: ProviderRepo + Clone + Send + Sync + 'static,
{
    async fn fetch_updates(mut self) -> Result<mpsc::Sender<WatchListUpdate<T>>> {
        let (subscriptions_updates_sender, mut subscriptions_updates_receiver) = mpsc::channel(20);

        let watchlist = self.watchlist.clone();
        let resources_repo = self.resources_repo.clone();
        let repo = self.repo.clone();
        tokio::task::spawn(async move {
            let type_name = std::any::type_name::<T>();
            info!("starting subscriptions updates handler: {}", type_name);
            while let Some(upd) = subscriptions_updates_receiver.recv().await {
                debug!("Subscription: {:?}", upd);
                if let Err(err) = watchlist.write().await.on_update(&upd) {
                    error!("error while updating watchlist: {:?}", err);
                }
                if let WatchListUpdate::Updated { item, context } = upd {
                    let span = resume_tracing(
                        context,
                        format!("topic://{}", Into::<String>::into(item.clone())),
                    );
                    let result = check_and_maybe_insert(&resources_repo, &repo, item.clone())
                        .instrument(span)
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

        tokio::task::spawn(async move {
            let type_name = std::any::type_name::<T>();
            info!("starting provider: {}", type_name);
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

fn resume_tracing(context: Option<TracingContext>, topic: String) -> tracing::Span {
    use opentelemetry::global;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let span = tracing::debug_span!("subscription_load_values", %topic);

    if let Some(context) = context {
        let context = global::get_text_map_propagator(|propagator| propagator.extract(&context));
        span.set_parent(context);
        debug!("Tracing resumed for {}", topic);
    }

    span
}

async fn check_and_maybe_insert<T: Item<P>, R: ResourcesRepo, P: ProviderRepo>(
    resources_repo: &Arc<R>,
    repo: &P,
    value: T,
) -> Result<Option<HashSet<String>>> {
    let topic = value.clone().into();
    let existing_value = resources_repo.get(&topic)?;
    let need_to_publish = existing_value.is_none();
    let topic_value = if let Some(existing_value) = existing_value {
        existing_value
    } else {
        let span = tracing::debug_span!("get_last_value", ?topic);
        value.last_value(repo).instrument(span).await?
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
                let span = tracing::debug_span!("subtopic_get_last_value", ?topic, ?subtopic);
                let new_value = subtopic_value.last_value(repo).instrument(span).await?;
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
        tracing::event!(tracing::Level::DEBUG, ?topic, "Topic value published");
        resources_repo.set_and_push(topic, topic_value)?;
    }

    Ok(subtopics)
}

async fn append_subtopic_to_multitopic<T: Item<P>, R: ResourcesRepo, P: ProviderRepo>(
    resources_repo: &Arc<R>,
    multitopic_item: T,
    subtopic_item: T,
    watchlist: &Arc<RwLock<WatchList<T, R>>>,
) -> Result<()> {
    let multitopic = multitopic_item.clone().into();
    let existing_value = resources_repo.get(&multitopic)?;
    let mut subtopics = if let Some(existing_value) = existing_value {
        subtopics_from_topic_value(&multitopic, &existing_value)?.expect("must be multitopic")
    } else {
        HashSet::new()
    };
    let subtopic: Topic = subtopic_item.clone().into();
    let new_subtopic = String::from(subtopic);
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
    resources_repo.set_and_push(multitopic, subtopics_str)?;
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
pub trait LastValue<R: ProviderRepo> {
    async fn last_value(self, repo: &R) -> Result<String>;
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
