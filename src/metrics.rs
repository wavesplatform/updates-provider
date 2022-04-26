use lazy_static::lazy_static;
use prometheus::{IntGauge, IntGaugeVec, Opts, Registry};

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
    pub static ref REDIS_INPUT_QUEUE_SIZE: IntGauge = IntGauge::new(
        "WatchlistsQueueSize",
        "Size of incoming Redis messages queue"
    )
    .expect("can't create message_queue metrics");
    pub static ref REDIS_CONNECTIONS_AVAILABLE: IntGauge = IntGauge::new(
        "RedisConnectionsAvailable",
        "Number of available connections in the pool, negative if there are blocked tasks waiting for connection"
    )
    .expect("can't create message_queue metrics");
    pub static ref DB_WRITE_TIME: IntGauge = IntGauge::new(
        "WatchlistsDatabaseWriteTime",
        "Time (in ms) of DB writes"
    )
    .expect("can't create db_write_time metrics");
}

pub fn register_metrics() {
    REGISTRY
        .register(Box::new(WATCHLISTS_TOPICS.clone()))
        .expect("can't register watchlists_topics metrics");

    REGISTRY
        .register(Box::new(WATCHLISTS_SUBSCRIPTIONS.clone()))
        .expect("can't register watchlists_subscriptions metrics");

    REGISTRY
        .register(Box::new(REDIS_INPUT_QUEUE_SIZE.clone()))
        .expect("can't register watchlists_queue_size metrics");

    REGISTRY
        .register(Box::new(REDIS_CONNECTIONS_AVAILABLE.clone()))
        .expect("can't register redis_connections_available metrics");

    REGISTRY
        .register(Box::new(DB_WRITE_TIME.clone()))
        .expect("can't register db_write_time metrics");
}
