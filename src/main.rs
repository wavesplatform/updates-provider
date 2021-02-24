mod config;
mod error;
mod models;
mod providers;
mod resources;
mod subscriptions;

use error::Error;
use providers::UpdatesProvider;
use r2d2::Pool;
use r2d2_redis::{r2d2, redis, RedisConnectionManager};
use std::sync::Arc;
use subscriptions::SubscriptionsUpdatesObservers;
use wavesexchange_log::{error, info};

fn main() -> Result<(), Error> {
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    let _ = rt.block_on(tokio_main());
    Ok(rt.shutdown_timeout(std::time::Duration::from_millis(1)))
}

async fn tokio_main() -> Result<(), Error> {
    let redis_config = config::load_redis()?;
    let subscriptions_config = config::load_subscriptions()?;
    let configs_updater_config = config::load_configs_updater()?;
    let states_updater_config = config::load_states_updater()?;
    let test_resources_config = config::load_test_resources_updater()?;
    let blockchain_height_config = config::load_blockchain_height()?;

    let subscriptions_updates_observers: SubscriptionsUpdatesObservers =
        SubscriptionsUpdatesObservers::default();

    let redis_connection_url = format!(
        "redis://{}:{}@{}:{}/",
        redis_config.username, redis_config.password, redis_config.host, redis_config.port
    );
    let redis_pool_manager = RedisConnectionManager::new(redis_connection_url.clone())?;
    let redis_pool = Pool::builder().build(redis_pool_manager)?;

    let resources_repo = resources::repo::ResourcesRepoImpl::new(redis_pool.clone());
    let resources_repo = Arc::new(resources_repo);

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
    subscriptions_updates_observers
        .write()
        .await
        .push(configs_subscriptions_updates_sender);

    // States
    let states_requester = Box::new(providers::polling::states::StateRequester::new(
        states_updater_config.base_url,
    ));
    let states_updates_provider = providers::polling::PollProvider::new(
        states_requester,
        states_updater_config.polling_delay,
        states_updater_config.delete_timeout,
        resources_repo.clone(),
    );
    let states_subscriptions_updates_sender = states_updates_provider.fetch_updates().await?;

    subscriptions_updates_observers
        .write()
        .await
        .push(states_subscriptions_updates_sender);

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

    subscriptions_updates_observers
        .write()
        .await
        .push(test_resources_subscriptions_updates_sender);

    // Blockchain height
    let blockchain_height_updates_provider = providers::blockchain_height::Provider::new(
        blockchain_height_config,
        resources_repo.clone(),
    )
    .await?;
    let blockchain_height_handle = tokio::task::spawn(async move {
        info!("starting blockchain height updater");
        blockchain_height_updates_provider.run().await
    });

    let subscriptions_repo = subscriptions::repo::SubscriptionsRepoImpl::new(
        redis_pool.clone(),
        subscriptions_config.subscriptions_key.clone(),
    );
    let subscriptions_repo = Arc::new(subscriptions_repo);
    // r2d2 cannot extract dedicated connection for using for redis pubsub
    // therefore its need to use a separated redis client
    let redis_client = redis::Client::open(redis_connection_url)?;
    let notifications_puller = subscriptions::puller::PullerImpl::new(
        subscriptions_repo.clone(),
        redis_client,
        subscriptions_config.subscriptions_key.clone(),
    );

    let subscriptions_updates_receiver = notifications_puller.run().await?;

    let mut subscriptions_updates_pusher = subscriptions::pusher::PusherImpl::new(
        subscriptions_updates_observers,
        subscriptions_updates_receiver,
    );

    let subscriptions_updates_pusher_handle = tokio::task::spawn(async move {
        info!("starting subscriptions updates pusher");
        subscriptions_updates_pusher.run().await
    });

    tokio::select! {
        result = blockchain_height_handle => {
            if let Err(e) = result? {
                let error = Error::from(e);
                error!("blockchain height return error: {:?}", error);
                return Err(error);
            }
        }
        result = subscriptions_updates_pusher_handle => {
            if let Err(e) = result? {
                let error = Error::from(e);
                error!("subscriptions updates pusher error: {}", error);
                return Err(error);
            }
        }
    }

    Ok(())
}
