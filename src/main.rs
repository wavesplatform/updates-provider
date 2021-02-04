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
    let configs_repo = providers::configs::repo::ConfigsRepoImpl::new(
        configs_updater_config.configs_base_url,
        configs_updater_config.gitlab_private_token,
        configs_updater_config.gitlab_configs_branch,
    );

    let config_updates_provider = providers::configs::configs_updater::ConfigsUpdaterImpl::new(
        configs_repo,
        resources_repo.clone(),
        configs_updater_config.polling_delay,
    );

    let configs_subscriptions_updates_sender = config_updates_provider.fetch_updates().await?;

    subscriptions_updates_observers
        .write()
        .await
        .push(configs_subscriptions_updates_sender);

    // Test Resources
    let test_resources_repo = providers::test_resources::repo::TestResourcesRepoImpl::new(
        test_resources_config.test_resources_base_url,
    );

    let test_resources_updates_provider =
        providers::test_resources::updater::TestResourcesUpdaterImpl::new(
            test_resources_repo,
            resources_repo.clone(),
            test_resources_config.polling_delay,
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
