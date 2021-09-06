use super::super::TSResourcesRepoImpl;
use crate::{error::Error, resources::ResourcesRepo};
use std::sync::Arc;
use tokio::sync::mpsc;
use waves_protobuf_schemas::waves::events::BlockchainUpdated;
use wavesexchange_topic::Topic;

pub struct Provider {
    resources_repo: TSResourcesRepoImpl,
    last_height: i32,
    rx: mpsc::Receiver<Arc<BlockchainUpdated>>,
}

pub struct ProviderWithUpdatesSender {
    pub tx: mpsc::Sender<Arc<BlockchainUpdated>>,
    pub provider: Provider,
}

impl Provider {
    pub async fn init(
        resources_repo: TSResourcesRepoImpl,
    ) -> Result<ProviderWithUpdatesSender, Error> {
        let last_height = get_last_height(resources_repo.clone())?;
        // random channel buffer size
        let (tx, rx) = mpsc::channel(20);

        Ok(ProviderWithUpdatesSender {
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
                    .set_and_push(Topic::BlockchainHeight, height.to_string())?;
                self.last_height = height;
            }
        }

        Ok(())
    }
}

fn get_last_height(resources_repo: TSResourcesRepoImpl) -> Result<i32, Error> {
    let topic = Topic::BlockchainHeight;
    if let Some(height) = resources_repo.get(&topic)? {
        if let Ok(height) = height.parse() {
            return Ok(height);
        }
    }
    Ok(1)
}
