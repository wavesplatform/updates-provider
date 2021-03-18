use super::super::watchlist::WatchList;
use super::super::{TSResourcesRepoImpl, TSUpdatesProviderLastValues, UpdatesProvider};
use crate::providers::watchlist::MaybeFromTopic;
use crate::subscriptions::SubscriptionUpdate;
use crate::transactions::repo::TransactionsRepoImpl;
use crate::transactions::{BlockMicroblockAppend, BlockchainUpdate, Transaction, TransactionType};
use crate::{
    error::Result,
    transactions::{Address, TransactionUpdate, TransactionsRepo},
};
use crate::{
    models::{TransactionByAddress, Type},
    resources::ResourcesRepo,
};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{collections::HashMap, convert::TryFrom};
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use waves_protobuf_schemas::waves::events::BlockchainUpdated;
use wavesexchange_log::{debug, error, info};

const UPDATES_BUFFER_SIZE: usize = 10;
const TX_CHUNK_SIZE: usize = 65535 / 3;
const ADDRESSES_CHUNK_SIZE: usize = 65535 / 2;

pub struct Provider {
    watchlist: Arc<RwLock<WatchList<TransactionByAddress>>>,
    rx: mpsc::Receiver<Arc<BlockchainUpdated>>,
    resources_repo: TSResourcesRepoImpl,
    last_values: TSUpdatesProviderLastValues,
    transactions_repo: Arc<Mutex<TransactionsRepoImpl>>,
}

pub struct ProviderReturn {
    pub last_height: i32,
    pub tx: mpsc::Sender<Arc<BlockchainUpdated>>,
    pub provider: Provider,
}

impl Provider {
    pub async fn new(
        resources_repo: TSResourcesRepoImpl,
        delete_timeout: Duration,
        transactions_repo: Arc<Mutex<TransactionsRepoImpl>>,
    ) -> Result<ProviderReturn> {
        let last_values = Arc::new(RwLock::new(HashMap::new()));
        let watchlist = Arc::new(RwLock::new(WatchList::new(
            resources_repo.clone(),
            last_values.clone(),
            delete_timeout,
        )));
        let (tx, rx) = mpsc::channel(UPDATES_BUFFER_SIZE);
        let last_height = {
            let conn = &*transactions_repo.lock().await;
            match conn.get_prev_handled_height()? {
                Some(prev_handled_height) => {
                    rollback(conn, prev_handled_height.uid)?;
                    prev_handled_height.height as i32 + 1
                }
                None => 1i32,
            }
        };
        let provider = Self {
            watchlist,
            rx,
            resources_repo,
            last_values,
            transactions_repo,
        };
        Ok(ProviderReturn {
            last_height,
            tx,
            provider,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        'a: loop {
            self.watchlist.write().await.delete_old().await;
            let mut buffer = Vec::with_capacity(UPDATES_BUFFER_SIZE);
            if let Some(event) = self.rx.recv().await {
                let update = BlockchainUpdate::try_from(event)?;
                buffer.push(update);
                if let Some(BlockchainUpdate::Rollback(_)) = buffer.last() {
                    self.process_updates(buffer).await?;
                    continue;
                }
                let mut delay = tokio::time::delay_for(Duration::from_secs(10));
                loop {
                    tokio::select! {
                        _ = &mut delay => {
                            self.process_updates(buffer).await?;
                            break;
                        }
                        maybe_event = self.rx.recv() => {
                            if let Some(event) = maybe_event {
                                let update = BlockchainUpdate::try_from(event)?;
                                buffer.push(update);
                                if let Some(BlockchainUpdate::Rollback(_)) = buffer.last() {
                                    self.process_updates(buffer).await?;
                                    break;
                                }
                                if buffer.len() == UPDATES_BUFFER_SIZE {
                                    self.process_updates(buffer).await?;
                                    break;
                                }
                            } else {
                                self.process_updates(buffer).await?;
                                break 'a;
                            }
                        }
                    }
                }
            } else {
                break;
            }
        }

        Ok(())
    }

    async fn process_updates(&mut self, blockchain_updates: Vec<BlockchainUpdate>) -> Result<()> {
        let start = Instant::now();
        {
            let conn = &*self.transactions_repo.lock().await;
            insert_blockchain_updates(conn, blockchain_updates.iter())?;
        }
        debug!(
            "process updates {:?} elements in {} ms",
            blockchain_updates.len(),
            start.elapsed().as_millis()
        );
        self.announce_updates(blockchain_updates).await?;

        Ok(())
    }

    async fn announce_updates(&mut self, blockchain_updates: Vec<BlockchainUpdate>) -> Result<()> {
        for blockchain_update in blockchain_updates {
            self.announce_update(blockchain_update).await?
        }

        Ok(())
    }

    async fn announce_update(&mut self, blockchain_update: BlockchainUpdate) -> Result<()> {
        match blockchain_update {
            BlockchainUpdate::Block(BlockMicroblockAppend { transactions, .. }) => {
                self.check_transactions(transactions).await?
            }
            BlockchainUpdate::Microblock(BlockMicroblockAppend { transactions, .. }) => {
                self.check_transactions(transactions).await?
            }
            BlockchainUpdate::Rollback(_) => (),
        }

        Ok(())
    }

    async fn check_transactions(&mut self, tx_updates: Vec<TransactionUpdate>) -> Result<()> {
        for tx_update in tx_updates.into_iter() {
            let tx = tx_update.into();
            self.check_transaction(tx).await?
        }

        Ok(())
    }

    async fn check_transaction(&mut self, tx: Tx) -> Result<()> {
        for address in tx.addresses {
            let data = TransactionByAddress {
                address: address.0.clone(),
                tx_type: tx.tx_type.into(),
            };
            self.inner_check_transaction(data, tx.id.clone()).await?;
            let data = TransactionByAddress {
                address: address.0,
                tx_type: Type::All,
            };
            self.inner_check_transaction(data, tx.id.clone()).await?;
        }

        Ok(())
    }

    async fn inner_check_transaction(
        &mut self,
        data: TransactionByAddress,
        current_value: String,
    ) -> Result<()> {
        if self.watchlist.read().await.items.contains_key(&data) {
            super::super::watchlist_process(
                &data,
                current_value,
                &self.resources_repo,
                &self.last_values,
            )
            .await
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl UpdatesProvider for Provider {
    async fn fetch_updates(mut self) -> Result<mpsc::UnboundedSender<SubscriptionUpdate>> {
        let (subscriptions_updates_sender, mut subscriptions_updates_receiver) =
            mpsc::unbounded_channel::<SubscriptionUpdate>();

        let watchlist = self.watchlist.clone();
        let resources_repo = self.resources_repo.clone();
        let transactions_repo = self.transactions_repo.clone();
        tokio::task::spawn(async move {
            info!("starting transactions subscriptions updates handler");
            while let Some(upd) = subscriptions_updates_receiver.recv().await {
                if let Err(err) = watchlist.write().await.on_update(upd.clone()).await {
                    error!("error while updating watchlist: {:?}", err);
                }
                if let SubscriptionUpdate::New { topic } = upd {
                    if let Some(value) = TransactionByAddress::maybe_item(topic) {
                        if let Err(err) = check_and_maybe_insert(
                            resources_repo.clone(),
                            transactions_repo.clone(),
                            value,
                        )
                        .await
                        {
                            error!("error while updating value: {:?}", err);
                        }
                    }
                }
            }
        });

        tokio::task::spawn(async move {
            info!("starting transactions updater");
            if let Err(error) = self.run().await {
                error!("transaction updater return error: {:?}", error);
            }
        });

        Ok(subscriptions_updates_sender)
    }
}

async fn check_and_maybe_insert(
    resources_repo: TSResourcesRepoImpl,
    transactions_repo: Arc<Mutex<TransactionsRepoImpl>>,
    value: TransactionByAddress,
) -> Result<()> {
    let topic = value.clone().into();
    if let None = resources_repo.get(&topic)? {
        let new_value = match value {
            TransactionByAddress {
                tx_type: Type::All,
                address,
            } => {
                if let Some(Transaction { id, .. }) = transactions_repo
                    .lock()
                    .await
                    .last_transaction_by_address(address)?
                {
                    Some(id)
                } else {
                    None
                }
            }
            TransactionByAddress { tx_type, address } => {
                let transaction_type = TransactionType::try_from(tx_type)?;
                if let Some(Transaction { id, .. }) = transactions_repo
                    .lock()
                    .await
                    .last_transaction_by_address_and_type(address, transaction_type)?
                {
                    Some(id)
                } else {
                    None
                }
            }
        };
        resources_repo.set(topic, serde_json::to_string(&new_value)?)?;
    }

    Ok(())
}

fn insert_blockchain_updates<'a>(
    conn: &TransactionsRepoImpl,
    mut blockchain_updates: impl Iterator<Item = &'a BlockchainUpdate>,
) -> Result<()> {
    conn.transaction(|| {
        loop {
            let mut blocks = vec![];
            let mut rollback_block_uid = None;
            while let Some(update) = blockchain_updates.next() {
                match update {
                    BlockchainUpdate::Block(block) => blocks.push(block),
                    BlockchainUpdate::Microblock(block) => blocks.push(block),
                    BlockchainUpdate::Rollback(block_id) => {
                        let block_uid = conn.get_block_uid(&block_id)?;
                        rollback_block_uid = Some(block_uid);
                    }
                }
            }
            if let Some(block_uid) = rollback_block_uid {
                if blocks.len() > 0 {
                    insert_blocks(conn, blocks)?
                };
                rollback(conn, block_uid)?;
            } else {
                if blocks.len() > 0 {
                    insert_blocks(conn, blocks)?
                };
                break;
            }
        }

        Ok(())
    })?;

    Ok(())
}

fn insert_blocks(
    conn: &TransactionsRepoImpl,
    blocks_updates: Vec<&BlockMicroblockAppend>,
) -> Result<()> {
    let h = blocks_updates.last().unwrap().height;
    let blocks = blocks_updates.iter().map(|b| b.clone().into()).collect();
    let start = Instant::now();
    let block_ids = conn.insert_blocks_or_microblocks(&blocks)?;
    info!(
        "insert {} blocks in {} ms",
        blocks.len(),
        start.elapsed().as_millis()
    );
    let transaction_updates = blocks_updates
        .iter()
        .map(|block| block.transactions.iter().map(|tx| tx));
    let transactions_chunks = block_ids
        .into_iter()
        .zip(transaction_updates.clone())
        .flat_map(|(block_uid, txs)| txs.map(move |tx| (block_uid, tx).into()))
        .fold(vec![Vec::with_capacity(TX_CHUNK_SIZE)], |mut acc, tx| {
            let last = acc.last_mut().unwrap();
            if last.len() == TX_CHUNK_SIZE {
                let mut new_last = Vec::with_capacity(TX_CHUNK_SIZE);
                new_last.push(tx);
                acc.push(new_last)
            } else {
                last.push(tx)
            }
            acc
        });
    for transactions in transactions_chunks {
        let start = Instant::now();
        conn.insert_transactions(&transactions)?;
        info!(
            "insert {} txs in {} ms",
            transactions.len(),
            start.elapsed().as_millis()
        );
    }
    let addresses_hashset = transaction_updates
        .flat_map(|txs| {
            txs.flat_map(|tx| {
                tx.addresses
                    .iter()
                    .map(move |address| (tx.id.clone(), address).into())
            })
        })
        .collect::<std::collections::HashSet<_>>();
    let addresses_chunks = addresses_hashset.into_iter().fold(
        vec![Vec::with_capacity(ADDRESSES_CHUNK_SIZE)],
        |mut acc, address| {
            let last = acc.last_mut().unwrap();
            if last.len() == ADDRESSES_CHUNK_SIZE {
                let mut new_last = Vec::with_capacity(ADDRESSES_CHUNK_SIZE);
                new_last.push(address);
                acc.push(new_last)
            } else {
                last.push(address)
            }
            acc
        },
    );
    for addresses in addresses_chunks {
        let start = Instant::now();
        conn.insert_associated_addresses(&addresses)?;
        info!(
            "insert {} addresses in {} ms",
            addresses.len(),
            start.elapsed().as_millis()
        );
    }
    info!("inserted {:?} block", h);
    Ok(())
}

fn rollback(conn: &TransactionsRepoImpl, block_uid: i64) -> Result<()> {
    conn.rollback_blocks_microblocks(&block_uid)?;
    Ok(())
}

struct Tx {
    id: String,
    tx_type: TransactionType,
    addresses: Vec<Address>,
}

impl From<TransactionUpdate> for Tx {
    fn from(value: TransactionUpdate) -> Self {
        Self {
            id: value.id,
            tx_type: value.tx_type,
            addresses: value.addresses,
        }
    }
}
