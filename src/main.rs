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

#[tokio::main]
async fn main() -> Result<(), Error> {
    let redis_config = config::load_redis()?;
    let subscriptions_config = config::load_subscriptions()?;
    let configs_updater_config = config::load_configs_updater()?;
    let states_updater_config = config::load_states_updater()?;
    let test_resources_config = config::load_test_resources_updater()?;

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
    let configs_requester = Box::new(providers::configs::ConfigRequester::new(
        configs_updater_config.clone(),
    ));
    let configs_updates_provider = providers::Provider::new(
        configs_requester,
        configs_updater_config.polling_delay,
        resources_repo.clone(),
    );
    let configs_subscriptions_updates_sender = configs_updates_provider.fetch_updates().await?;
    subscriptions_updates_observers
        .write()
        .await
        .push(configs_subscriptions_updates_sender);

    // States
    let states_requester = Box::new(providers::states::StateRequester::new(
        states_updater_config.base_url,
    ));
    let states_updates_provider = providers::Provider::new(
        states_requester,
        states_updater_config.polling_delay,
        resources_repo.clone(),
    );
    let states_subscriptions_updates_sender = states_updates_provider.fetch_updates().await?;

    subscriptions_updates_observers
        .write()
        .await
        .push(states_subscriptions_updates_sender);

    // Test Resources
    let test_resources_requester =
        Box::new(providers::test_resources::TestResourcesRequester::new(
            test_resources_config.test_resources_base_url,
        ));
    let test_resources_updates_provider = providers::Provider::new(
        test_resources_requester,
        test_resources_config.polling_delay,
        resources_repo,
    );
    let test_resources_subscriptions_updates_sender =
        test_resources_updates_provider.fetch_updates().await?;

    subscriptions_updates_observers
        .write()
        .await
        .push(test_resources_subscriptions_updates_sender);

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

    if let Err(e) = tokio::try_join!(subscriptions_updates_pusher_handle) {
        let err = Error::from(e);
        error!("subscriptions updates pusher error: {}", err);
        return Err(err);
    }

    Ok(())
}
