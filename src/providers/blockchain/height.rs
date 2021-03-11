use super::super::TSResourcesRepoImpl;
use crate::{error::Error, models::Topic, resources::ResourcesRepo};
use std::sync::Arc;
use tokio::sync::mpsc;
use waves_protobuf_schemas::waves::events::BlockchainUpdated;
use waves_protobuf_schemas::waves::node::grpc::blocks_api_client::BlocksApiClient;

pub struct Provider {
    resources_repo: TSResourcesRepoImpl,
    last_height: i32,
    rx: mpsc::Receiver<Arc<BlockchainUpdated>>,
}

pub struct ProviderReturn {
    pub tx: mpsc::Sender<Arc<BlockchainUpdated>>,
    pub provider: Provider,
}

impl Provider {
    pub async fn new(
        node_url: String,
        resources_repo: TSResourcesRepoImpl,
    ) -> Result<ProviderReturn, Error> {
        let redis_height = get_last_height(resources_repo.clone())?;
        let response = BlocksApiClient::connect(node_url)
            .await?
            .get_current_height(())
            .await?;
        let current_height = response.into_inner() as i32;
        let last_height = if current_height != redis_height {
            resources_repo.set(Topic::BlockchainHeight, current_height.to_string())?;
            current_height
        } else {
            redis_height
        };
        let (tx, rx) = mpsc::channel(20);

        Ok(ProviderReturn {
            tx,
            provider: Self {
                resources_repo,
                last_height,
                rx,
            },
        })
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        while let Some(blockchain_updated) = self.rx.recv().await {
            let height = blockchain_updated.height;
            if self.last_height != height {
                self.resources_repo
                    .set(Topic::BlockchainHeight, height.to_string())?;
                self.last_height = height;
            }
        }

        Ok(())
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
