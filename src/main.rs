#[macro_use]
extern crate diesel;

mod api;
mod config;
mod db;
mod error;
mod metrics;
mod models;
mod providers;
mod resources;
mod schema;
mod subscriptions;
mod utils;
mod waves;

use crate::db::{repo_consumer::PostgresConsumerRepo, repo_provider::PostgresProviderRepo};
use crate::error::Error;
use crate::providers::{blockchain, UpdatesProvider};
use crate::resources::repo::ResourcesRepoRedis;
use r2d2::Pool;
use r2d2_redis::{r2d2, redis, RedisConnectionManager};
use std::sync::Arc;
use wavesexchange_log::{error, info};

// Tracing
use opentelemetry::global;
use tracing_subscriber::prelude::*;

fn main() -> Result<(), Error> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let result = rt.block_on(tokio_main());
    rt.shutdown_timeout(std::time::Duration::from_millis(1));
    result
}

async fn tokio_main() -> Result<(), Error> {
    metrics::register_metrics();

    let redis_config = config::load_redis()?;
    let postgres_config = config::load_postgres()?;
    let configs_updater_config = config::load_configs_updater()?;
    let test_resources_config = config::load_test_resources_updater()?;
    let blockchain_config = config::load_blockchain()?;
    let server_config = config::load_api()?;
    let tracing_config = config::tracing::load()?;

    let opentelemetry_initialized;
    if let Some(tracing_config) = tracing_config {
        info!(
            "Tracing enabled: {}, {}",
            tracing_config.service_name_prefix, tracing_config.jaeger_agent_endpoint,
        );
        global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());

        let tracer = opentelemetry_jaeger::new_pipeline()
            .with_service_name(format!(
                "{}/updates-provider",
                tracing_config.service_name_prefix
            ))
            .with_agent_endpoint(tracing_config.jaeger_agent_endpoint)
            .install_batch(opentelemetry::runtime::Tokio)?;

        let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);
        let fmt_layer = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .finish();

        fmt_layer.with(opentelemetry).try_init()?;

        opentelemetry_initialized = true;
    } else {
        opentelemetry_initialized = false;
    }

    let redis_connection_url = format!(
        "redis://{}:{}@{}:{}/",
        redis_config.username, redis_config.password, redis_config.host, redis_config.port
    );
    let redis_pool_manager = RedisConnectionManager::new(redis_connection_url.clone())?;
    let redis_pool = Pool::builder().build(redis_pool_manager)?;

    let resources_repo = ResourcesRepoRedis::new(redis_pool.clone());
    let resources_repo = Arc::new(resources_repo);

    let consumer_db_pool = db::pool::new(&postgres_config.postgres_rw)?;
    let provider_db_pool = db::pool::new(&postgres_config.postgres_ro)?;

    let consumer_repo = PostgresConsumerRepo::new(consumer_db_pool);
    let provider_repo = PostgresProviderRepo::new(provider_db_pool);

    // Configs
    let configs_requester = Box::new(providers::polling::configs::ConfigRequester::new(
        configs_updater_config.clone(),
    ));
    let configs_updates_provider = providers::polling::PollProvider::new(
        configs_requester,
        configs_updater_config.polling_delay,
        configs_updater_config.delete_timeout,
        resources_repo.clone(),
    );
    let configs_subscriptions_updates_sender = configs_updates_provider.fetch_updates().await?;

    // Test Resources
    let test_resources_requester = Box::new(
        providers::polling::test_resources::TestResourcesRequester::new(
            test_resources_config.test_resources_base_url,
        ),
    );
    let test_resources_updates_provider = providers::polling::PollProvider::new(
        test_resources_requester,
        test_resources_config.polling_delay,
        test_resources_config.delete_timeout,
        resources_repo.clone(),
    );
    let test_resources_subscriptions_updates_sender =
        test_resources_updates_provider.fetch_updates().await?;

    // Blockchain
    let mut blockchain_puller =
        blockchain::handler::Puller::new(blockchain_config.updates_url).await?;

    let blockchain::height::ProviderWithUpdatesSender { tx, mut provider } =
        blockchain::height::Provider::init(resources_repo.clone()).await?;

    blockchain_puller.subscribe(tx);

    tokio::task::spawn(async move {
        info!("starting blockchain height updater");
        if let Err(error) = provider.run().await {
            error!("blockchain height updater returned an error: {:?}", error);
        }
    });

    let blockchain::updater::UpdaterReturn {
        tx,
        last_height,
        mut updater,
    } = blockchain::updater::Updater::init(
        consumer_repo,
        blockchain_config.updates_buffer_size,
        blockchain_config.transactions_count_threshold,
        blockchain_config.associated_addresses_count_threshold,
        blockchain_config.waiting_blocks_timeout,
    )
    .await?;

    blockchain_puller.subscribe(tx);
    let start_from = if last_height > blockchain_config.start_height {
        last_height
    } else {
        blockchain_config.start_height
    };
    blockchain_puller.set_last_height(start_from);

    // random channel buffer size
    let (tx, rx) = tokio::sync::mpsc::channel(20);
    let provider = blockchain::provider::Provider::<wavesexchange_topic::Transaction, _, _>::new(
        resources_repo.clone(),
        blockchain_config.transaction_delete_timeout,
        provider_repo.clone(),
        rx,
    );

    updater.add_provider(tx);

    let transactions_subscriptions_updates_sender = provider.fetch_updates().await?;

    // random channel buffer size
    let (tx, rx) = tokio::sync::mpsc::channel(20);
    let provider = blockchain::provider::Provider::<wavesexchange_topic::State, _, _>::new(
        resources_repo.clone(),
        blockchain_config.state_delete_timeout,
        provider_repo.clone(),
        rx,
    );

    updater.add_provider(tx);

    let states_subscriptions_updates_sender = provider.fetch_updates().await?;

    let (tx, rx) = tokio::sync::mpsc::channel(20);
    let provider = blockchain::provider::Provider::<wavesexchange_topic::LeasingBalance, _, _>::new(
        resources_repo,
        blockchain_config.leasing_balance_delete_timeout,
        provider_repo,
        rx,
    );

    updater.add_provider(tx);

    let leasing_balances_subscriptions_updates_sender = provider.fetch_updates().await?;

    let blockchain_updater_handle = tokio::spawn(async move {
        if let Err(error) = updater.run().await {
            error!("blockchain updater returned an error: {:?}", error);
        }
    });

    let blockchain_puller_handle = tokio::spawn(async move {
        if let Err(error) = blockchain_puller.run().await {
            error!("blockchain puller returned an error: {:?}", error);
        }
    });

    let redis_client = redis::Client::open(redis_connection_url)?;

    let subscriptions_repo =
        subscriptions::repo::RedisSubscriptionsRepo::new(redis_pool, redis_client);
    let subscriptions_repo = Arc::new(subscriptions_repo);

    let notifications_puller = subscriptions::puller::PullerImpl::new(subscriptions_repo.clone());

    let subscriptions_updates_receiver = notifications_puller.run().await?;

    let mut subscriptions_updates_pusher =
        subscriptions::pusher::PusherImpl::new(subscriptions_updates_receiver);

    subscriptions_updates_pusher.add_observer(configs_subscriptions_updates_sender);
    subscriptions_updates_pusher.add_observer(test_resources_subscriptions_updates_sender);
    subscriptions_updates_pusher.add_observer(transactions_subscriptions_updates_sender);
    subscriptions_updates_pusher.add_observer(states_subscriptions_updates_sender);
    subscriptions_updates_pusher.add_observer(leasing_balances_subscriptions_updates_sender);

    let subscriptions_updates_pusher_handle = tokio::spawn(async move {
        info!("starting subscriptions updates pusher");
        if let Err(error) = subscriptions_updates_pusher.run().await {
            error!(
                "subscriptions updates pusher returned an error: {:?}",
                error
            );
        }
    });

    let api_handle = tokio::spawn(async move { api::start(server_config.port).await });

    tokio::select! {
        _ = blockchain_puller_handle => {}
        _ = blockchain_updater_handle => {}
        _ = subscriptions_updates_pusher_handle => {}
        result = api_handle => {
            result?;
        }
    }

    if opentelemetry_initialized {
        global::shutdown_tracer_provider();
    }

    Ok(())
}
