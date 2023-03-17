use lazy_static::lazy_static;
use prometheus::{IntGauge, IntGaugeVec, Opts};

lazy_static! {
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
    .expect("can't create watchlists_queue_size metrics");
    pub static ref REDIS_CONNECTIONS_AVAILABLE: IntGauge = IntGauge::new(
        "RedisConnectionsAvailable",
        "Number of available connections in the pool, negative if there are blocked tasks waiting for connection"
    )
    .expect("can't create redis_connections_available metrics");
    pub static ref POSTGRES_READ_CONNECTIONS_AVAILABLE: IntGauge = IntGauge::new(
        "PostgresReadConnectionsAvailable",
        "Number of available connections in the pool, negative if there are blocked tasks waiting for connection"
    )
    .expect("can't create postgres_read_connections_available metrics");
    pub static ref POSTGRES_WRITE_CONNECTIONS_AVAILABLE: IntGauge = IntGauge::new(
        "PostgresWriteConnectionsAvailable",
        "Number of available connections in the pool, negative if there are blocked tasks waiting for connection"
    )
    .expect("can't create postgres_write_connections_available metrics");
    pub static ref DB_WRITE_TIME: IntGauge = IntGauge::new(
        "WatchlistsDatabaseWriteTime",
        "Time (in ms) of DB writes"
    )
    .expect("can't create db_write_time metrics");
}
