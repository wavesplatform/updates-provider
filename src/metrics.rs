use lazy_static::lazy_static;
use prometheus::{IntGaugeVec, Opts, Registry};
lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
    pub static ref WATCHLISTS_TOPICS: IntGaugeVec = IntGaugeVec::new(
        Opts::new("WatchlistsTopics", "topics count per resource type"),
        &["resource_type"]
    )
    .expect("can't create watchlists_topics metrics");
    pub static ref WATCHLISTS_SUBSCRIPTIONS: IntGaugeVec = IntGaugeVec::new(
        Opts::new(
            "WatchlistsSubscriptions",
            "subscriptions count per resource type"
        ),
        &["resource_type"]
    )
    .expect("can't create watchlists_subscriptions metrics");
}

pub fn register_metrics() {
    REGISTRY
        .register(Box::new(WATCHLISTS_TOPICS.clone()))
        .expect("can't register watchlists_topics metrics");

    REGISTRY
        .register(Box::new(WATCHLISTS_SUBSCRIPTIONS.clone()))
        .expect("can't register watchlists_subscriptions metrics");
}
