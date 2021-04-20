use super::super::watchlist::{WatchList, WatchListUpdate};
use super::super::{TSResourcesRepoImpl, TSUpdatesProviderLastValues, UpdatesProvider};
use crate::transactions::repo::TransactionsRepoImpl;
use crate::transactions::{BlockMicroblockAppend, BlockchainUpdate, Transaction, TransactionType};
use crate::{
    error::Result,
    transactions::{exchange::ExchangeData, Address, TransactionUpdate, TransactionsRepo},
};
use crate::{
    models::{self, Topic, TransactionByAddress, TransactionExchange, Type},
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

const TX_CHUNK_SIZE: usize = 65535 / 4;
const ADDRESSES_CHUNK_SIZE: usize = 65535 / 2;

pub struct Provider {
    watchlist: Arc<RwLock<WatchList<models::Transaction>>>,
    rx: mpsc::Receiver<Arc<BlockchainUpdated>>,
    resources_repo: TSResourcesRepoImpl,
    last_values: TSUpdatesProviderLastValues,
    transactions_repo: Arc<Mutex<TransactionsRepoImpl>>,
    updates_buffer_size: usize,
    transactions_count_threshold: usize,
    associated_addresses_count_threshold: usize,
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
        updates_buffer_size: usize,
        transactions_count_threshold: usize,
        associated_addresses_count_threshold: usize,
    ) -> Result<ProviderReturn> {
        let last_values = Arc::new(RwLock::new(HashMap::new()));
        let watchlist = Arc::new(RwLock::new(WatchList::new(
            resources_repo.clone(),
            last_values.clone(),
            delete_timeout,
        )));
        let (tx, rx) = mpsc::channel(updates_buffer_size.clone());
        let last_height = {
            let conn = &*transactions_repo.lock().await;
            match conn.get_prev_handled_height()? {
                Some(prev_handled_height) => prev_handled_height.height as i32 + 1,
                None => 1i32,
            }
        };
        let provider = Self {
            watchlist,
            rx,
            resources_repo,
            last_values,
            transactions_repo,
            updates_buffer_size,
            transactions_count_threshold,
            associated_addresses_count_threshold,
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
            let mut buffer = Vec::with_capacity(self.updates_buffer_size);
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
                                };
                                let (txs_count, addresses_count) = count_txs_addresses(&buffer);
                                if buffer.len() == self.updates_buffer_size
                                || txs_count >= self.transactions_count_threshold
                                || addresses_count >= self.associated_addresses_count_threshold {
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
        self.maybe_insert_in_redis(blockchain_updates).await?;

        Ok(())
    }

    async fn maybe_insert_in_redis(
        &mut self,
        blockchain_updates: Vec<BlockchainUpdate>,
    ) -> Result<()> {
        for blockchain_update in blockchain_updates {
            match blockchain_update {
                BlockchainUpdate::Block(BlockMicroblockAppend { transactions, .. }) => {
                    self.check_transactions(transactions).await?
                }
                BlockchainUpdate::Microblock(BlockMicroblockAppend { transactions, .. }) => {
                    self.check_transactions(transactions).await?
                }
                BlockchainUpdate::Rollback(_) => (),
            }
        }

        Ok(())
    }

    async fn check_transactions(&mut self, tx_updates: Vec<TransactionUpdate>) -> Result<()> {
        for tx_update in tx_updates.into_iter() {
            if let TransactionType::Exchange = tx_update.tx_type {
                let exchange_data = ExchangeData::try_from(&tx_update)?;
                self.check_transaction_exchange(exchange_data).await?
            }
            let tx = tx_update.into();
            self.check_transaction_by_address(tx).await?
        }

        Ok(())
    }

    async fn check_transaction_by_address(&mut self, tx: Tx) -> Result<()> {
        for address in tx.addresses {
            let data = TransactionByAddress {
                address: address.0.clone(),
                tx_type: tx.tx_type.into(),
            };
            self.check_in_watchlist(models::Transaction::ByAddress(data), tx.id.clone())
                .await?;
            let data = TransactionByAddress {
                address: address.0,
                tx_type: Type::All,
            };
            self.check_in_watchlist(models::Transaction::ByAddress(data), tx.id.clone())
                .await?;
        }

        Ok(())
    }

    async fn check_transaction_exchange(&mut self, exchange_data: ExchangeData) -> Result<()> {
        let amount_asset = exchange_data
            .order1
            .asset_pair
            .amount_asset
            .as_ref()
            .map(|x| x.to_owned())
            .unwrap_or("WAVES".to_string());
        let price_asset = exchange_data
            .order1
            .asset_pair
            .price_asset
            .as_ref()
            .map(|x| x.to_owned())
            .unwrap_or("WAVES".to_string());
        let data = models::Transaction::Exchange(TransactionExchange {
            amount_asset,
            price_asset,
        });
        let current_value = serde_json::to_string(&exchange_data)?;
        self.check_in_watchlist(data, current_value).await?;
        Ok(())
    }

    async fn check_in_watchlist(
        &mut self,
        data: models::Transaction,
        current_value: String,
    ) -> Result<()> {
        if self.watchlist.read().await.contains_key(&data) {
            watchlist_process(
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
impl UpdatesProvider<models::Transaction> for Provider {
    async fn fetch_updates(
        mut self,
    ) -> Result<mpsc::UnboundedSender<WatchListUpdate<models::Transaction>>> {
        let (subscriptions_updates_sender, mut subscriptions_updates_receiver) =
            mpsc::unbounded_channel::<WatchListUpdate<models::Transaction>>();

        let watchlist = self.watchlist.clone();
        let resources_repo = self.resources_repo.clone();
        let transactions_repo = self.transactions_repo.clone();
        tokio::task::spawn(async move {
            info!("starting transactions subscriptions updates handler");
            while let Some(upd) = subscriptions_updates_receiver.recv().await {
                if let Err(err) = watchlist.write().await.on_update(&upd) {
                    error!("error while updating watchlist: {:?}", err);
                }
                if let WatchListUpdate::New { item, .. } = upd {
                    if let Err(err) = check_and_maybe_insert(
                        resources_repo.clone(),
                        transactions_repo.clone(),
                        item,
                    )
                    .await
                    {
                        error!("error while updating value: {:?}", err);
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
    value: models::Transaction,
) -> Result<()> {
    let topic = value.clone().into();
    if let None = resources_repo.get(&topic)? {
        let new_value = match value {
            models::Transaction::ByAddress(TransactionByAddress {
                tx_type: Type::All,
                address,
            }) => {
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
            models::Transaction::ByAddress(TransactionByAddress { tx_type, address }) => {
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
            models::Transaction::Exchange(TransactionExchange {
                amount_asset,
                price_asset,
            }) => {
                if let Some(Transaction {
                    body: Some(body_value),
                    ..
                }) = transactions_repo
                    .lock()
                    .await
                    .last_exchange_transaction(amount_asset, price_asset)?
                {
                    Some(serde_json::to_string(&body_value)?)
                } else {
                    None
                }
            }
        };
        let encoded_value = match new_value {
            None => serde_json::to_string(&new_value)?,
            Some(v) => v,
        };
        resources_repo.set(topic.clone(), encoded_value.clone())?;
        resources_repo.push(topic, encoded_value)?;
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
            let mut rollback_block_id = None;
            while let Some(update) = blockchain_updates.next() {
                match update {
                    BlockchainUpdate::Block(block) => blocks.push(block),
                    BlockchainUpdate::Microblock(block) => blocks.push(block),
                    BlockchainUpdate::Rollback(block_id) => rollback_block_id = Some(block_id),
                }
            }
            if let Some(block_id) = rollback_block_id {
                if blocks.len() > 0 {
                    insert_blocks(conn, blocks)?
                };
                let block_uid = conn.get_block_uid(block_id)?;
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
    let transactions_chunks: Vec<Vec<Transaction>> = block_ids
        .into_iter()
        .zip(transaction_updates.clone())
        .flat_map(|(block_uid, txs)| txs.map(move |tx| (block_uid, tx)))
        .try_fold(
            vec![Vec::with_capacity(TX_CHUNK_SIZE)],
            |mut acc, tx| match Transaction::try_from(tx) {
                Ok(tx) => {
                    let last = acc.last_mut().unwrap();
                    if last.len() == TX_CHUNK_SIZE {
                        let mut new_last = Vec::with_capacity(TX_CHUNK_SIZE);
                        new_last.push(tx);
                        acc.push(new_last)
                    } else {
                        last.push(tx)
                    }
                    Ok(acc)
                }
                Err(error) => Err(error),
            },
        )?;
    for transactions in transactions_chunks {
        let start = Instant::now();
        conn.insert_transactions(&transactions)?;
        info!(
            "insert {} txs in {} ms",
            transactions.len(),
            start.elapsed().as_millis()
        );
    }
    let addresses_chunks = transaction_updates
        .clone()
        .flat_map(|txs| {
            txs.flat_map(|tx| {
                tx.addresses
                    .iter()
                    .map(move |address| (tx.id.clone(), address).into())
            })
        })
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .fold(
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

fn count_txs_addresses(buffer: &Vec<BlockchainUpdate>) -> (usize, usize) {
    buffer
        .iter()
        .fold((0, 0), |(txs, addresses), block| match block {
            BlockchainUpdate::Block(b) => {
                let new_txs = txs + b.transactions.len();
                let addresses_count: usize =
                    b.transactions.iter().map(|tx| tx.addresses.len()).sum();
                let new_addresses = addresses + addresses_count;
                (new_txs, new_addresses)
            }
            BlockchainUpdate::Microblock(b) => {
                let new_txs = txs + b.transactions.len();
                let addresses_count: usize =
                    b.transactions.iter().map(|tx| tx.addresses.len()).sum();
                let new_addresses = addresses + addresses_count;
                (new_txs, new_addresses)
            }
            BlockchainUpdate::Rollback(_) => (txs, addresses),
        })
}

pub async fn watchlist_process(
    data: &models::Transaction,
    current_value: String,
    resources_repo: &TSResourcesRepoImpl,
    last_values: &TSUpdatesProviderLastValues,
) -> Result<()> {
    let data_key = data.to_string();
    let resource: Topic = data.clone().into();
    info!("insert new value {:?}", resource);
    last_values
        .write()
        .await
        .insert(data_key, current_value.clone());
    resources_repo.set(resource.clone(), current_value.clone())?;
    resources_repo.push(resource, current_value)?;
    Ok(())
}
