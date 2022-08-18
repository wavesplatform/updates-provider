use crate::{error::Error, resources::ResourcesRepo, subscriptions::SubscriptionEvent};
use itertools::Itertools;
use std::{
    collections::{HashMap, HashSet},
    convert::TryFrom,
    fmt::Debug,
    hash::Hash,
    sync::Arc,
    time::{Duration, Instant},
};
use wavesexchange_log::{debug, warn};
use wavesexchange_topic::Topic;

#[derive(Debug)]
pub struct WatchList<T: WatchListItem, R: ResourcesRepo> {
    items: HashMap<T, ItemInfo>,
    patterns: HashMap<T, T::PatternMatcher>,
    repo: Arc<R>,
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

impl<T: WatchListItem, R: ResourcesRepo> WatchList<T, R> {
    pub fn new(repo: Arc<R>, delete_timeout: Duration) -> Self {
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
        if keys.is_empty() {
            return;
        }
        let num_keys = keys.len();
        debug!("Removing expired keys ({}): {:?}", num_keys, keys);
        for item in keys {
            self.items.remove(&item);
            self.patterns.remove(&item);
            let res = self.repo.del(T::into(item)).await;
            if let Some(err) = res.err() {
                warn!("Failed to delete Redis key: '{:?}' (ignoring)", err);
            }
        }
        let elapsed = Instant::now().duration_since(now);
        debug!(
            "delete_old: Removed {} expired keys in {} ms",
            num_keys,
            elapsed.as_millis()
        );
    }

    pub fn key_watch_status(&self, key: &T) -> KeyWatchStatus<T> {
        if T::PATTERNS_SUPPORTED && !self.patterns.is_empty() {
            let matched_patterns = self
                .patterns
                .iter()
                .filter(|&(_, matcher)| matcher.is_match(key))
                .map(|(pattern, _)| pattern.clone())
                .collect_vec();

            if !matched_patterns.is_empty() {
                return KeyWatchStatus::MatchesPattern(matched_patterns);
            }
        }

        if self.items.contains_key(key) {
            return KeyWatchStatus::Watched;
        }

        return KeyWatchStatus::NotWatched;
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

impl<'a, T: 'a, R: ResourcesRepo> IntoIterator for &'a WatchList<T, R>
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
    use crate::resources::ResourcesRepo;

    impl<T: WatchListItem, R: ResourcesRepo> WatchList<T, R> {
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

#[cfg(test)]
pub mod tests {
    use self::{item::TestItem, repo::TestResourcesRepo};
    use super::{KeyWatchStatus, WatchList, WatchListUpdate};
    use std::collections::HashSet;
    use std::{sync::Arc, time::Duration};

    pub mod repo {
        use crate::{error::Error, resources::ResourcesRepo};
        use async_trait::async_trait;
        use std::{
            collections::HashMap,
            sync::{Arc, Mutex},
        };
        use wavesexchange_topic::Topic;

        pub struct TestResourcesRepo(Arc<Mutex<HashMap<Topic, String>>>);

        impl Default for TestResourcesRepo {
            fn default() -> Self {
                TestResourcesRepo(Arc::new(Mutex::new(HashMap::new())))
            }
        }

        #[async_trait]
        impl ResourcesRepo for TestResourcesRepo {
            async fn get(&self, resource: &Topic) -> Result<Option<String>, Error> {
                let data = self.0.lock().unwrap();
                Ok(data.get(resource).cloned())
            }

            async fn set(&self, resource: Topic, value: String) -> Result<(), Error> {
                let mut data = self.0.lock().unwrap();
                data.insert(resource, value);
                Ok(())
            }

            async fn del(&self, resource: Topic) -> Result<(), Error> {
                let mut data = self.0.lock().unwrap();
                data.remove(&resource);
                Ok(())
            }

            async fn push(&self, _resource: Topic, _value: String) -> Result<(), Error> {
                Ok(())
            }
        }

        impl TestResourcesRepo {
            pub fn reset(&self) {
                let mut data = self.0.lock().unwrap();
                data.clear();
            }
        }
    }

    pub mod item {
        use super::super::{KeyPattern, MaybeFromTopic, PatternMatcher, WatchListItem};
        use std::{convert::TryFrom, hash::Hash};
        use wavesexchange_topic::{State, StateMultiPatterns, StateSingle, Topic};

        #[derive(Clone, PartialEq, Eq, Hash, Debug)]
        pub struct TestItem(pub &'static str);

        impl Into<Topic> for TestItem {
            fn into(self) -> Topic {
                Topic::try_from(self.0).unwrap()
            }
        }

        impl MaybeFromTopic for TestItem {
            fn maybe_item(topic: &Topic) -> Option<Self> {
                let s = String::from(topic.clone());
                let s = Box::leak(Box::new(s));
                Some(TestItem(s.as_str()))
            }
        }

        impl Into<String> for TestItem {
            fn into(self) -> String {
                unimplemented!()
            }
        }

        impl KeyPattern for TestItem {
            const PATTERNS_SUPPORTED: bool = true;
            type PatternMatcher = TestMatcher;

            fn new_matcher(&self) -> Self::PatternMatcher {
                TestMatcher(self.matched_subtopic_prefix())
            }
        }

        impl WatchListItem for TestItem {}

        impl TestItem {
            pub fn matched_subtopic_prefix(&self) -> String {
                if self.0.ends_with('*') {
                    let topic = Topic::try_from(self.0).unwrap();
                    let (address, key_pattern) = match topic {
                        Topic::State(State::MultiPatterns(StateMultiPatterns {
                            addresses,
                            key_patterns,
                        })) => (addresses[0].clone(), key_patterns[0].clone()),
                        _ => panic!("test is broken"),
                    };
                    let key = key_pattern[0..key_pattern.len() - 1].to_string(); // Strip '*' at the end
                    let topic = Topic::State(State::Single(StateSingle { address, key }));
                    topic.into()
                } else {
                    self.0.into()
                }
            }
        }

        pub struct TestMatcher(String);

        impl PatternMatcher<TestItem> for TestMatcher {
            fn is_match(&self, value: &TestItem) -> bool {
                let value = value.0;
                let prefix = self.0.as_str();
                value.starts_with(prefix)
            }
        }
    }

    macro_rules! assert_not_watched {
        ($watchlist:expr, $key:literal) => {
            assert!(
                matches!(
                    $watchlist.key_watch_status(&TestItem($key)),
                    KeyWatchStatus::NotWatched,
                ),
                "Key '{}' expected NOT to be watched; actual: {:?}",
                $key,
                $watchlist.key_watch_status(&TestItem($key)),
            );
        };
    }

    macro_rules! assert_watched {
        ($watchlist:expr, $key:literal) => {
            assert!(
                matches!(
                    $watchlist.key_watch_status(&TestItem($key)),
                    KeyWatchStatus::Watched,
                ),
                "Key '{}' expected to be watched; actual: {:?}",
                $key,
                $watchlist.key_watch_status(&TestItem($key)),
            );
        };
    }

    macro_rules! assert_matched {
        ($watchlist:expr, $key:literal) => {
            assert!(
                matches!(
                    $watchlist.key_watch_status(&TestItem($key)),
                    KeyWatchStatus::MatchesPattern(_),
                ),
                "Key '{}' expected to be matched by pattern; actual: {:?}",
                $key,
                $watchlist.key_watch_status(&TestItem($key)),
            );
        };
    }

    macro_rules! assert_value {
        ($watchlist:expr, $key:literal, $value:expr) => {
            assert_eq!(
                $watchlist.get_value(&TestItem($key)),
                Option::<&str>::clone(&$value)
                    .map(ToString::to_string)
                    .as_ref(),
                "Key '{}' expected to have {:?}",
                $key,
                Option::<&str>::clone(&$value),
            );
        };
    }

    #[tokio::test]
    async fn test_watchlist_simple() {
        // Setup
        let repo = Arc::new(TestResourcesRepo::default());
        let keep_alive = Duration::from_nanos(1);
        let ensure_dead = Duration::from_nanos(2);
        let mut wl = WatchList::<TestItem, TestResourcesRepo>::new(repo, keep_alive);

        // Default state
        assert_not_watched!(wl, "topic://state/address/foo");
        assert_value!(wl, "topic://state/address/foo", None);

        // Add some item
        let res = wl.on_update(&WatchListUpdate::Updated {
            item: TestItem("topic://state/address/foo"),
        });
        assert!(res.is_ok());
        wl.insert_value(
            &TestItem("topic://state/address/foo"),
            "foo_value".to_string(),
        );

        assert_watched!(wl, "topic://state/address/foo");
        assert_value!(wl, "topic://state/address/foo", Some("foo_value"));

        // Remove that item
        let res = wl.on_update(&WatchListUpdate::Removed {
            item: TestItem("topic://state/address/foo"),
        });
        assert!(res.is_ok());

        tokio::time::sleep(ensure_dead).await;
        wl.delete_old().await;

        assert_not_watched!(wl, "topic://state/address/foo");
        assert_value!(wl, "topic://state/address/foo", None);
    }

    #[tokio::test]
    async fn test_watchlist_patterns() {
        use std::iter::FromIterator;
        let set = |items: Vec<&str>| HashSet::from_iter(items.into_iter().map(ToString::to_string));

        // Setup
        let repo = Arc::new(TestResourcesRepo::default());
        let keep_alive = Duration::from_nanos(1);
        let ensure_dead = Duration::from_nanos(2);
        let mut wl = WatchList::<TestItem, TestResourcesRepo>::new(repo, keep_alive);

        // Initial state
        assert_not_watched!(wl, "topic://state/address/foo1");
        assert_not_watched!(wl, "topic://state/address/foo2");
        assert_not_watched!(wl, "topic://state/address/foo3");

        // Add pattern item
        let res = wl.on_update(&WatchListUpdate::Updated {
            item: TestItem("topic://state?address__in[]=address&key__match_any[]=foo*"),
        });
        assert!(res.is_ok());

        assert_watched!(
            wl,
            "topic://state?address__in[]=address&key__match_any[]=foo*"
        );
        assert_not_watched!(wl, "topic://state/address/foo1");
        assert_not_watched!(wl, "topic://state/address/foo2");
        assert_not_watched!(wl, "topic://state/address/foo3");

        wl.update_multitopic(
            TestItem("topic://state?address__in[]=address&key__match_any[]=foo*"),
            set(vec![
                "topic://state/address/foo1",
                "topic://state/address/foo2",
            ]),
        );
        assert_watched!(wl, "topic://state/address/foo1");
        assert_watched!(wl, "topic://state/address/foo2");
        assert_matched!(wl, "topic://state/address/foo3");

        // Change subtopics
        wl.update_multitopic(
            TestItem("topic://state?address__in[]=address&key__match_any[]=foo*"),
            set(vec![
                "topic://state/address/foo1",
                "topic://state/address/foo3",
            ]),
        );
        tokio::time::sleep(ensure_dead).await;
        wl.delete_old().await;
        assert_watched!(wl, "topic://state/address/foo1");
        assert_watched!(wl, "topic://state/address/foo3");
        assert_matched!(wl, "topic://state/address/foo2");

        // Remove pattern item
        let res = wl.on_update(&WatchListUpdate::Removed {
            item: TestItem("topic://state?address__in[]=address&key__match_any[]=foo*"),
        });
        assert!(res.is_ok());

        tokio::time::sleep(ensure_dead).await;
        wl.delete_old().await;

        assert_not_watched!(
            wl,
            "topic://state?address__in[]=address&key__match_any[]=foo*"
        );
        assert_not_watched!(wl, "topic://state/address/foo1");
        assert_not_watched!(wl, "topic://state/address/foo2");
        assert_not_watched!(wl, "topic://state/address/foo3");
    }
}
