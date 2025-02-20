#[macro_use]
extern crate diesel;
extern crate wavesexchange_topic as wx_topic;

mod asset_info;
mod config;
mod db;
mod decimal;
mod error;
mod metrics;
mod models;
mod providers;
mod redis;
mod resources;
mod schema;
mod subscriptions;
mod utils;
mod waves;

use crate::asset_info::AssetStorage;
use crate::db::{repo_consumer::PostgresConsumerRepo, repo_provider::PostgresProviderRepo};
use crate::error::Error;
use crate::metrics::*;
use crate::providers::blockchain::provider::exchange_pair::PairsContext;
use crate::providers::{blockchain, UpdatesProvider};
use crate::resources::repo::ResourcesRepoRedis;

use std::sync::Arc;
use std::time::Duration;
use wavesexchange_log::{error, info};
use wavesexchange_warp::MetricsWarpBuilder;

fn main() -> Result<(), Error> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let result = rt.block_on(tokio_main());
    rt.shutdown_timeout(Duration::from_millis(1));
    result
}

async fn tokio_main() -> Result<(), Error> {
    let redis_config = config::load_redis()?;
    let postgres_config = config::load_postgres()?;
    let configs_updater_config = config::load_configs_updater()?;
    let test_resources_config = config::load_test_resources_updater()?;
    let blockchain_config = config::load_blockchain()?;
    let server_config = config::load_api()?;

    let asset_storage = AssetStorage::new(&server_config.assets_service_url);

    let metrics = tokio::spawn(
        MetricsWarpBuilder::new()
            .with_metrics_port(server_config.metrics_port)
            .with_metric(&*WATCHLISTS_TOPICS)
            .with_metric(&*WATCHLISTS_SUBSCRIPTIONS)
            .with_metric(&*REDIS_INPUT_QUEUE_SIZE)
            .with_metric(&*REDIS_CONNECTIONS_AVAILABLE)
            .with_metric(&*POSTGRES_READ_CONNECTIONS_AVAILABLE)
            .with_metric(&*POSTGRES_WRITE_CONNECTIONS_AVAILABLE)
            .with_metric(&*DB_WRITE_TIME)
            .run_async(),
    );

    let redis_connection_url = format!(
        "redis://{}:{}@{}:{}/",
        redis_config.username, redis_config.password, redis_config.host, redis_config.port
    );
    let redis_pool =
        redis::new_redis_pool(redis_connection_url, REDIS_CONNECTIONS_AVAILABLE.clone()).await?;

    let resources_repo = ResourcesRepoRedis::new(redis_pool.clone());

    let consumer_db_pool = db::pool::new(
        &postgres_config.postgres_rw,
        POSTGRES_WRITE_CONNECTIONS_AVAILABLE.clone(),
    )?;
    let provider_db_pool = db::pool::new(
        &postgres_config.postgres_ro,
        POSTGRES_READ_CONNECTIONS_AVAILABLE.clone(),
    )?;

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
        blockchain_config.start_rollback_depth,
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
    let provider = blockchain::provider::Provider::<wx_topic::Transaction, _, _, _>::new(
        resources_repo.clone(),
        blockchain_config.transaction_delete_timeout,
        provider_repo.clone(),
        (),
        rx,
    );

    updater.add_provider(tx);

    let transactions_subscriptions_updates_sender = provider.fetch_updates().await?;

    // random channel buffer size
    let (tx, rx) = tokio::sync::mpsc::channel(20);
    let provider = blockchain::provider::Provider::<wx_topic::State, _, _, _>::new(
        resources_repo.clone(),
        blockchain_config.state_delete_timeout,
        provider_repo.clone(),
        (),
        rx,
    );

    updater.add_provider(tx);

    let states_subscriptions_updates_sender = provider.fetch_updates().await?;

    let (tx, rx) = tokio::sync::mpsc::channel(20);
    let provider = blockchain::provider::Provider::<wx_topic::LeasingBalance, _, _, _>::new(
        resources_repo.clone(),
        blockchain_config.leasing_balance_delete_timeout,
        provider_repo.clone(),
        (),
        rx,
    );

    updater.add_provider(tx);

    let leasing_balances_subscriptions_updates_sender = provider.fetch_updates().await?;

    // random channel buffer size
    let (tx, rx) = tokio::sync::mpsc::channel(20);
    let provider = blockchain::provider::Provider::<wx_topic::ExchangePair, _, _, _>::new(
        resources_repo.clone(),
        Duration::from_secs(600),
        provider_repo.clone(),
        PairsContext {
            asset_storage,
            pairs_storage: Default::default(),
        },
        rx,
    );

    updater.add_provider(tx);

    let exchange_pair_updates_sender = provider.fetch_updates().await?;

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

    let subscriptions_repo = subscriptions::repo::SubscriptionsRepoImpl::new(redis_pool.clone());
    let subscriptions_repo = Arc::new(subscriptions_repo);
    let notifications_puller =
        subscriptions::puller::PullerImpl::new(subscriptions_repo.clone(), redis_pool);

    let subscriptions_updates_receiver = notifications_puller.run().await?;

    let mut subscriptions_updates_pusher =
        subscriptions::pusher::PusherImpl::new(subscriptions_updates_receiver);

    subscriptions_updates_pusher.add_observer(configs_subscriptions_updates_sender);
    subscriptions_updates_pusher.add_observer(test_resources_subscriptions_updates_sender);
    subscriptions_updates_pusher.add_observer(transactions_subscriptions_updates_sender);
    subscriptions_updates_pusher.add_observer(states_subscriptions_updates_sender);
    subscriptions_updates_pusher.add_observer(leasing_balances_subscriptions_updates_sender);
    subscriptions_updates_pusher.add_observer(exchange_pair_updates_sender);

    let subscriptions_updates_pusher_handle = tokio::spawn(async move {
        info!("starting subscriptions updates pusher");
        if let Err(error) = subscriptions_updates_pusher.run().await {
            error!(
                "subscriptions updates pusher returned an error: {:?}",
                error
            );
        }
    });

    tokio::select! {
        _ = blockchain_puller_handle => {}
        _ = blockchain_updater_handle => {}
        _ = subscriptions_updates_pusher_handle => {}
        result = metrics => {
            result?;
        }
    }

    Ok(())
}
