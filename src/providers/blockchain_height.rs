use super::TSResourcesRepoImpl;
use crate::{error::Error, models::Topic, resources::ResourcesRepo};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use waves_protobuf_schemas::waves::events::{
    grpc::{
        blockchain_updates_api_client::BlockchainUpdatesApiClient, SubscribeEvent, SubscribeRequest,
    },
    BlockchainUpdated,
};
use waves_protobuf_schemas::waves::node::grpc::blocks_api_client::BlocksApiClient;

#[derive(Debug)]
pub struct Config {
    pub updates_url: String,
    pub node_url: String,
}

pub struct Provider {
    channel: tonic::transport::Channel,
    resources_repo: TSResourcesRepoImpl,
    node_url: String,
}

impl Provider {
    pub async fn new(config: Config, resources_repo: TSResourcesRepoImpl) -> Result<Self, Error> {
        let channel = tonic::transport::Channel::from_shared(config.updates_url)
            .map_err(|e| Error::GRPCUriError(e.to_string()))?
            .connect()
            .await?;
        Ok(Self {
            channel,
            resources_repo,
            node_url: config.node_url,
        })
    }

    pub async fn run(self) -> Result<(), Error> {
        let redis_height = get_last_height(self.resources_repo.clone())?;
        let response = BlocksApiClient::connect(self.node_url)
            .await?
            .get_current_height(())
            .await?;
        let current_height = response.into_inner() as i32;
        let raw_last_value = if current_height != redis_height {
            LastValue::new(current_height, true)
        } else {
            LastValue::new(redis_height, false)
        };
        let last_value = Arc::new(Mutex::new(raw_last_value));
        let request = tonic::Request::new(SubscribeRequest {
            from_height: current_height,
            to_height: 0,
        });

        let mut stream: tonic::Streaming<SubscribeEvent> =
            BlockchainUpdatesApiClient::new(self.channel.clone())
                .subscribe(request)
                .await?
                .into_inner();

        let mut interval = tokio::time::interval(Duration::from_secs(5));

        loop {
            tokio::select! {
                msg = stream.message() => {
                    if let Some(SubscribeEvent { update }) = msg? {
                        if let Some(BlockchainUpdated { height, .. }) = update {
                            last_value.clone().lock().await.update_value(height);
                        }
                    } else {
                        break
                    }
                }
                _ = interval.tick() => {
                    let last_value = last_value.clone();
                    let mut v = last_value.lock().await;
                    if v.changed {
                        self.resources_repo.set(Topic::BlockchainHeight, v.value.to_string())?;
                        v.not_changed();
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
struct LastValue {
    value: i32,
    changed: bool,
}

impl LastValue {
    fn new(value: i32, changed: bool) -> Self {
        Self { value, changed }
    }

    fn update_value(&mut self, value: i32) {
        if value != self.value {
            self.value = value;
            self.changed = true;
        }
    }

    fn not_changed(&mut self) {
        self.changed = false;
    }
}

fn get_last_height(resources_repo: TSResourcesRepoImpl) -> Result<i32, Error> {
    let topic = Topic::BlockchainHeight;
    match resources_repo.get(&topic)? {
        Some(height) => {
            if let Ok(x) = height.parse() {
                Ok(x)
            } else {
                Ok(1)
            }
        }
        None => Ok(1),
    }
}
