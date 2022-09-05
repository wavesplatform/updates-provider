use crate::metrics::*;
use wavesexchange_log::info;
use wavesexchange_warp::endpoints::StatsWarpBuilder;

pub struct Config {
    pub port: u16,
}

pub async fn start(port: u16) {
    info!("Starting web server at 0.0.0.0:{}", port);
    StatsWarpBuilder::no_main_instance()
        .add_metric(WATCHLISTS_TOPICS.clone())
        .add_metric(WATCHLISTS_SUBSCRIPTIONS.clone())
        .add_metric(REDIS_INPUT_QUEUE_SIZE.clone())
        .add_metric(REDIS_CONNECTIONS_AVAILABLE.clone())
        .add_metric(POSTGRES_READ_CONNECTIONS_AVAILABLE.clone())
        .add_metric(POSTGRES_WRITE_CONNECTIONS_AVAILABLE.clone())
        .add_metric(DB_WRITE_TIME.clone())
        .run(port)
        .await;
}
