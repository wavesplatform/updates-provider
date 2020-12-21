mod config;
mod error;
mod models;
mod providers;
mod resources;
mod subscriptions;

use bb8_redis::RedisConnectionManager;
use error::Error;
use providers::UpdatesProvider;
use subscriptions::SubscriptionsUpdatesObservers;
use wavesexchange_log::{error, info};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let redis_config = config::load_redis()?;
    let subscriptions_config = config::load_subscriptions()?;
    let configs_updater_config = config::load_configs_updater()?;

    let redis_pool_manager = RedisConnectionManager::new(format!(
        "redis://{}@{}:{}/",
        redis_config.password, redis_config.host, redis_config.port
    ))?;
    let redis_pool = bb8::Pool::builder().build(redis_pool_manager).await?;

    let resources_repo = resources::repo::ResourcesRepoImpl::new(redis_pool.clone());

    let configs_repo =
        providers::configs::repo::ConfigsRepoImpl::new(configs_updater_config.configs_base_url);
    let config_updates_provider = providers::configs::configs_updater::ConfigsUpdaterImpl::new(
        configs_repo,
        resources_repo,
        configs_updater_config.polling_delay,
    );

    let subscriptions_updates_sender = config_updates_provider.fetch_updates().await?;

    let subscriptions_updates_observers: SubscriptionsUpdatesObservers =
        SubscriptionsUpdatesObservers::default();
    subscriptions_updates_observers
        .write()
        .await
        .push(subscriptions_updates_sender);

    let notifications_puller = subscriptions::puller::PullerImpl::new(
        redis_pool.clone(),
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
