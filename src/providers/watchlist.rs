use super::TSResourcesRepoImpl;
use crate::subscriptions::SubscriptionEvent;
use crate::{error::Error, resources::ResourcesRepo};
use itertools::Itertools;
use std::convert::TryFrom;
use std::time::{Duration, Instant};
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    hash::Hash,
};
use wavesexchange_log::{debug, warn};
use wavesexchange_topic::Topic;

#[derive(Debug)]
pub struct WatchList<T: WatchListItem> {
    items: HashMap<T, ItemInfo>,
    patterns: HashMap<T, T::PatternMatcher>,
    repo: TSResourcesRepoImpl,
    delete_timeout: Duration,
    type_name: String,
}

#[derive(Debug, Default)]
struct ItemInfo {
    last_value: String,
    maybe_delete: Option<Instant>,
    subtopics: Option<HashSet<String>>,
    watched_directly: bool,
    watched_indirectly: bool,
}

impl ItemInfo {
    #[must_use]
    fn watch_direct(&mut self) -> bool {
        let is_new = self.watched_directly == false;
        self.watched_directly = true;
        return is_new;
    }

    #[must_use]
    fn unwatch_direct(&mut self) -> bool {
        let was_active = self.watched_directly == true;
        self.watched_directly = false;
        return was_active;
    }

    #[must_use]
    fn watch_indirect(&mut self) -> bool {
        let is_new = self.watched_indirectly == false;
        self.watched_indirectly = true;
        return is_new;
    }

    #[must_use]
    fn unwatch_indirect(&mut self) -> bool {
        let was_active = self.watched_indirectly == true;
        self.watched_indirectly = false;
        return was_active;
    }

    fn is_watched(&self) -> bool {
        self.watched_directly || self.watched_indirectly
    }

    fn delete_after(&mut self, delete_timeout: Duration) {
        debug!("Dead key: {:?} (delete after {:?})", self, delete_timeout);
        let delete_timestamp = Instant::now() + delete_timeout;
        self.maybe_delete = Some(delete_timestamp)
    }
}

pub trait WatchListItem:
    Eq + Hash + Into<Topic> + MaybeFromTopic + Into<String> + KeyPattern + Clone + Debug
{
}

#[derive(Debug, Clone)]
pub enum WatchListUpdate<T: WatchListItem> {
    Updated { item: T },
    Removed { item: T },
}

pub trait MaybeFromUpdate: std::fmt::Debug + Send + Sync {
    fn maybe_from_update(update: &SubscriptionEvent) -> Option<Self>
    where
        Self: Sized;
}

impl<T: WatchListItem + std::fmt::Debug + Send + Sync> MaybeFromUpdate for WatchListUpdate<T> {
    fn maybe_from_update(update: &SubscriptionEvent) -> Option<Self> {
        match update {
            SubscriptionEvent::Updated { topic } => {
                T::maybe_item(topic).map(|item| WatchListUpdate::Updated { item })
            }
            SubscriptionEvent::Removed { topic } => {
                T::maybe_item(topic).map(|item| WatchListUpdate::Removed { item })
            }
        }
    }
}

pub trait MaybeFromTopic: Sized {
    fn maybe_item(topic: &Topic) -> Option<Self>;
}

impl<T: WatchListItem> WatchList<T> {
    pub fn new(repo: TSResourcesRepoImpl, delete_timeout: Duration) -> Self {
        let items = HashMap::new();
        let patterns = HashMap::new();
        let type_name = std::any::type_name::<T>().to_string();
        Self {
            items,
            patterns,
            repo,
            delete_timeout,
            type_name,
        }
    }

    fn create_or_refresh_item(&mut self, item: T) -> &mut ItemInfo {
        debug!("Live key: {:?}", item);
        self.items
            .entry(item)
            .and_modify(|ii| ii.maybe_delete = None)
            .or_default()
    }

    fn collect_as_items<'a>(topics: impl IntoIterator<Item = &'a String>) -> Vec<T> {
        topics
            .into_iter()
            .map(String::as_str)
            .map(Topic::try_from)
            .map_ok(|subtopic| T::maybe_item(&subtopic))
            .map(|result| match result {
                Ok(Some(value)) => Ok(value),
                _ => Err(()),
            })
            .filter_map(Result::ok)
            .collect()
    }

    pub fn on_update(&mut self, update: &WatchListUpdate<T>) -> Result<(), Error> {
        match update {
            WatchListUpdate::Updated { item } => {
                let item_info = self.create_or_refresh_item(item.to_owned());
                let update_metric = item_info.watch_direct();
                if update_metric {
                    let topic: Topic = item.clone().into();
                    self.metric_increase(true, topic.is_multi_topic());
                }
            }

            WatchListUpdate::Removed { item } => {
                if let Some(item_info) = self.items.get_mut(item) {
                    let update_metric = item_info.unwatch_direct();

                    if !item_info.is_watched() {
                        item_info.delete_after(self.delete_timeout);
                    }

                    let subtopic_items = item_info.subtopics.as_ref().map(Self::collect_as_items);

                    if update_metric {
                        let topic: Topic = item.clone().into();
                        self.metric_decrease(true, topic.is_multi_topic());
                    }

                    if let Some(subtopic_items) = subtopic_items {
                        for subtopic_item in subtopic_items {
                            if let Some(subtopic_item_info) = self.items.get_mut(&subtopic_item) {
                                let update_metric = subtopic_item_info.unwatch_indirect();
                                if !subtopic_item_info.is_watched() {
                                    subtopic_item_info.delete_after(self.delete_timeout);
                                }
                                if update_metric {
                                    let subtopic: Topic = subtopic_item.clone().into();
                                    self.metric_decrease(false, subtopic.is_multi_topic());
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn update_multitopic(&mut self, item: T, subtopics: HashSet<String>) {
        let item_info = self.create_or_refresh_item(item.clone());
        if let Some(ref existing_subtopics) = item_info.subtopics {
            let updated = Self::collect_as_items(&subtopics);
            let removed = Self::collect_as_items(existing_subtopics.difference(&subtopics));
            if subtopics != *existing_subtopics {
                item_info.subtopics = Some(subtopics);
            }
            for item in updated {
                let topic: Topic = item.clone().into();
                let item_info = self.create_or_refresh_item(item);
                let update_metric = item_info.watch_indirect();
                if update_metric {
                    self.metric_increase(false, topic.is_multi_topic());
                }
            }
            for item in removed {
                if let Some(item_info) = self.items.get_mut(&item) {
                    let update_metric = item_info.unwatch_indirect();
                    if !item_info.is_watched() {
                        item_info.delete_after(self.delete_timeout);
                    }
                    if update_metric {
                        let topic: Topic = item.clone().into();
                        self.metric_decrease(false, topic.is_multi_topic());
                    }
                }
            }
        } else {
            let added = Self::collect_as_items(&subtopics);
            item_info.subtopics = Some(subtopics);
            for item in added {
                let topic: Topic = item.clone().into();
                let item_info = self.create_or_refresh_item(item);
                let update_metric = item_info.watch_indirect();
                if update_metric {
                    self.metric_increase(false, topic.is_multi_topic());
                }
            }
        }
        self.patterns
            .entry(item.clone())
            .or_insert_with(|| item.new_matcher());
    }

    pub async fn delete_old(&mut self) {
        let now = Instant::now();
        let keys = self
            .items
            .iter()
            .filter_map(|(item, item_info)| {
                if let Some(ref delete_timestamp) = item_info.maybe_delete {
                    if delete_timestamp < &now {
                        return Some(item.clone());
                    }
                }
                None
            })
            .collect::<Vec<_>>();
        if !keys.is_empty() {
            debug!("Removing expired keys ({}): {:?}", keys.len(), keys);
        }
        for item in keys {
            self.items.remove(&item);
            let res = self.repo.del(T::into(item));
            if let Some(err) = res.err() {
                warn!("Failed to delete Redis key: '{:?}' (ignoring)", err);
            }
        }
    }

    pub fn key_watch_status(&self, key: &T) -> KeyWatchStatus<T> {
        if self.items.contains_key(key) {
            return KeyWatchStatus::Watched;
        }

        if !T::PATTERNS_SUPPORTED || self.patterns.is_empty() {
            return KeyWatchStatus::NotWatched;
        }

        let matched_patterns = self
            .patterns
            .iter()
            .filter(|&(_, matcher)| matcher.is_match(key))
            .map(|(pattern, _)| pattern.clone())
            .collect_vec();

        if matched_patterns.is_empty() {
            KeyWatchStatus::NotWatched
        } else {
            KeyWatchStatus::MatchesPattern(matched_patterns)
        }
    }

    pub fn get_value(&self, key: &T) -> Option<&String> {
        self.items.get(key).map(|x| &x.last_value)
    }

    pub fn insert_value(&mut self, key: &T, value: String) {
        let item_info = self.items.entry(key.clone()).or_default();
        item_info.last_value = value;
    }
}

#[derive(Debug)]
pub enum KeyWatchStatus<T> {
    NotWatched,
    Watched,
    MatchesPattern(Vec<T>),
}

impl<'a, T: 'a> IntoIterator for &'a WatchList<T>
where
    T: WatchListItem,
{
    type Item = &'a T;
    type IntoIter = WatchListIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        WatchListIter {
            inner: self.items.keys(),
        }
    }
}

pub struct WatchListIter<'a, T: 'a> {
    inner: std::collections::hash_map::Keys<'a, T, ItemInfo>,
}

impl<'a, T> Iterator for WatchListIter<'a, T> {
    type Item = &'a T;

    #[inline]
    fn next(&mut self) -> Option<&'a T> {
        self.inner.next()
    }
}

pub trait KeyPattern {
    const PATTERNS_SUPPORTED: bool;
    type PatternMatcher: PatternMatcher<Self>;

    fn new_matcher(&self) -> Self::PatternMatcher;
}

pub trait PatternMatcher<T: ?Sized>: Send + Sync {
    fn is_match(&self, value: &T) -> bool;
}

impl<T> PatternMatcher<T> for () {
    fn is_match(&self, _: &T) -> bool {
        false
    }
}

mod metrics {
    use super::{WatchList, WatchListItem};
    use crate::metrics::{WATCHLISTS_SUBSCRIPTIONS, WATCHLISTS_TOPICS};

    impl<T: WatchListItem> WatchList<T> {
        pub(super) fn metric_increase(&self, is_direct: bool, is_pattern: bool) {
            let ty = &[self.type_name.as_str()];
            if !is_pattern {
                WATCHLISTS_TOPICS.with_label_values(ty).inc();
            }
            if is_direct {
                WATCHLISTS_SUBSCRIPTIONS.with_label_values(ty).inc();
            }
        }

        pub(super) fn metric_decrease(&self, is_direct: bool, is_pattern: bool) {
            let ty = &[self.type_name.as_str()];
            if !is_pattern {
                WATCHLISTS_TOPICS.with_label_values(ty).dec();
            }
            if is_direct {
                WATCHLISTS_SUBSCRIPTIONS.with_label_values(ty).dec();
            }
        }
    }
}
