use crate::error::Error;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tonic::transport::Channel;
use waves_protobuf_schemas::waves::events::{
    grpc::{
        blockchain_updates_api_client::BlockchainUpdatesApiClient, SubscribeEvent, SubscribeRequest,
    },
    BlockchainUpdated,
};

pub struct Puller {
    channel: Channel,
    last_height: i32,
    subscribers: Vec<Sender<Arc<BlockchainUpdated>>>,
}

impl Puller {
    pub async fn new(updates_url: String) -> Result<Self, Error> {
        let channel = tonic::transport::Channel::from_shared(updates_url)
            .map_err(|e| Error::GRPCUriError(e.to_string()))?
            .connect()
            .await?;
        Ok(Self {
            channel,
            last_height: 1,
            subscribers: vec![],
        })
    }

    pub fn set_last_height(&mut self, height: i32) {
        self.last_height = height;
    }

    pub fn subscribe(&mut self, tx: Sender<Arc<BlockchainUpdated>>) {
        self.subscribers.push(tx);
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        let request = tonic::Request::new(SubscribeRequest {
            from_height: self.last_height,
            to_height: 0,
        });

        let mut stream: tonic::Streaming<SubscribeEvent> =
            BlockchainUpdatesApiClient::new(self.channel.clone())
                .subscribe(request)
                .await?
                .into_inner();

        while let Some(SubscribeEvent { update }) = stream.message().await? {
            if let Some(message) = update {
                let value = Arc::new(message);
                for ref mut tx in self.subscribers.iter_mut() {
                    tx.send(value.clone()).await?
                }
            }
        }

        Ok(())
    }
}
