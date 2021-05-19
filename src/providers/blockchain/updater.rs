use crate::transactions::repo::TransactionsRepoPoolImpl;
use crate::transactions::{
    BlockMicroblockAppend, BlockchainUpdate, DataEntryUpdate, InsertableDataEntry, Transaction,
};
use crate::utils::ToChunks;
use crate::{
    error::{Error, Result},
    transactions::{TransactionUpdate, TransactionsRepo, TransactionsRepoPool},
};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use waves_protobuf_schemas::waves::events::BlockchainUpdated;
use wavesexchange_log::{debug, info};

const TX_CHUNK_SIZE: usize = 65535 / 4;
const ADDRESSES_CHUNK_SIZE: usize = 65535 / 2;

pub struct Updater {
    rx: mpsc::Receiver<Arc<BlockchainUpdated>>,
    transactions_repo: Arc<TransactionsRepoPoolImpl>,
    updates_buffer_size: usize,
    transactions_count_threshold: usize,
    associated_addresses_count_threshold: usize,
    providers: Vec<mpsc::Sender<Arc<Vec<BlockchainUpdate>>>>,
}

pub struct UpdaterReturn {
    pub last_height: i32,
    pub tx: mpsc::Sender<Arc<BlockchainUpdated>>,
    pub updater: Updater,
}

#[derive(Debug)]
enum MicroBlockFlag {
    Ok,
    HasMicroBlocks,
    NeedSquash,
}

impl Default for MicroBlockFlag {
    fn default() -> Self {
        Self::Ok
    }
}

impl Updater {
    pub async fn init(
        transactions_repo: Arc<TransactionsRepoPoolImpl>,
        updates_buffer_size: usize,
        transactions_count_threshold: usize,
        associated_addresses_count_threshold: usize,
    ) -> Result<UpdaterReturn> {
        let (tx, rx) = mpsc::channel(updates_buffer_size);
        let last_height = {
            match transactions_repo.get_prev_handled_height()? {
                Some(prev_handled_height) => {
                    transactions_repo.transaction(|conn| {
                        Ok(rollback_by_block_uid(conn, prev_handled_height.uid)?)
                    })?;
                    prev_handled_height.height as i32 + 1
                }
                None => 1i32,
            }
        };
        let updater = Self {
            rx,
            transactions_repo,
            updates_buffer_size,
            transactions_count_threshold,
            associated_addresses_count_threshold,
            providers: vec![],
        };
        Ok(UpdaterReturn {
            last_height,
            tx,
            updater,
        })
    }

    pub fn add_provider(&mut self, subscriber: mpsc::Sender<Arc<Vec<BlockchainUpdate>>>) {
        self.providers.push(subscriber);
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut microblock_flag = MicroBlockFlag::default();
        'a: loop {
            let mut buffer = Vec::with_capacity(self.updates_buffer_size);
            if let Some(event) = self.rx.recv().await {
                let update = BlockchainUpdate::try_from(event)?;
                buffer.push(update);
                match buffer.last().unwrap() {
                    BlockchainUpdate::Microblock(_) => {
                        if let MicroBlockFlag::Ok = microblock_flag {
                            microblock_flag = MicroBlockFlag::HasMicroBlocks
                        }
                    }
                    BlockchainUpdate::Block(_) => {
                        if let MicroBlockFlag::HasMicroBlocks = microblock_flag {
                            microblock_flag = MicroBlockFlag::NeedSquash;
                            self.process_updates(buffer, &mut microblock_flag).await?;
                            continue;
                        }
                    }
                    BlockchainUpdate::Rollback(_) => {
                        self.process_updates(buffer, &mut microblock_flag).await?;
                        continue;
                    }
                }
                let delay = tokio::time::sleep(Duration::from_secs(10));
                tokio::pin!(delay);

                loop {
                    tokio::select! {
                        _ = &mut delay => {
                            self.process_updates(buffer, &mut microblock_flag).await?;
                            break;
                        }
                        maybe_event = self.rx.recv() => {
                            if let Some(event) = maybe_event {
                                let update = BlockchainUpdate::try_from(event)?;
                                buffer.push(update);
                                match buffer.last().unwrap() {
                                    BlockchainUpdate::Microblock(_) => {
                                        if let MicroBlockFlag::Ok = microblock_flag {
                                            microblock_flag = MicroBlockFlag::HasMicroBlocks
                                        }
                                    }
                                    BlockchainUpdate::Block(_) => {
                                        if let MicroBlockFlag::HasMicroBlocks = microblock_flag {
                                            microblock_flag = MicroBlockFlag::NeedSquash;
                                            self.process_updates(buffer, &mut microblock_flag).await?;
                                            break;
                                        }
                                    }
                                    BlockchainUpdate::Rollback(_) => {
                                        self.process_updates(buffer, &mut microblock_flag).await?;
                                        break;
                                    }
                                }
                                let (txs_count, addresses_count) = count_txs_addresses(&buffer);
                                if buffer.len() == self.updates_buffer_size
                                || txs_count >= self.transactions_count_threshold
                                || addresses_count >= self.associated_addresses_count_threshold {
                                    self.process_updates(buffer, &mut microblock_flag).await?;
                                    break;
                                }
                            } else {
                                self.process_updates(buffer, &mut microblock_flag).await?;
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

    async fn process_updates(
        &mut self,
        blockchain_updates: Vec<BlockchainUpdate>,
        microblock_flag: &mut MicroBlockFlag,
    ) -> Result<()> {
        let start = Instant::now();
        {
            if let MicroBlockFlag::NeedSquash = microblock_flag {
                let mut i = 0;
                let mut blockchain_updates_iter = blockchain_updates.iter().enumerate();
                loop {
                    match blockchain_updates_iter.next() {
                        Some((idx, BlockchainUpdate::Block(_))) => {
                            insert_blockchain_updates(
                                &*self.transactions_repo,
                                blockchain_updates[..idx].to_vec().iter(),
                            )?;
                            squash_microblocks(&*self.transactions_repo)?;
                            *microblock_flag = MicroBlockFlag::default();
                            i = idx;
                            break;
                        }
                        Some(_) => (),
                        None => break,
                    }
                }
                insert_blockchain_updates(
                    &*self.transactions_repo,
                    blockchain_updates[i..].to_vec().iter(),
                )?;
            } else {
                insert_blockchain_updates(&*self.transactions_repo, blockchain_updates.iter())?;
            }
        }
        debug!(
            "process updates {:?} elements in {} ms",
            blockchain_updates.len(),
            start.elapsed().as_millis()
        );

        let sending = Arc::new(blockchain_updates);
        for tx in self.providers.iter_mut() {
            tx.send(sending.clone())
                .await
                .map_err(|_| Error::SendErrorVecBlockchainUpdate)?
        }

        Ok(())
    }
}

fn insert_blockchain_updates<'a, P: TransactionsRepoPool>(
    pool: &P,
    blockchain_updates: impl Iterator<Item = &'a BlockchainUpdate>,
) -> Result<()> {
    pool.transaction(|conn| {
        let mut blocks = vec![];
        let mut rollback_block_id = None;
        for update in blockchain_updates {
            match update {
                BlockchainUpdate::Block(block) => blocks.push(block),
                BlockchainUpdate::Microblock(block) => blocks.push(block),
                BlockchainUpdate::Rollback(block_id) => rollback_block_id = Some(block_id),
            }
        }
        inserting(conn, blocks)?;
        if let Some(block_id) = rollback_block_id {
            rollback(conn, block_id)?;
        }

        Ok(())
    })?;

    Ok(())
}

fn inserting<U: TransactionsRepo + ?Sized>(
    conn: &U,
    blocks_updates: Vec<&BlockMicroblockAppend>,
) -> Result<()> {
    if !blocks_updates.is_empty() {
        let h = blocks_updates.last().unwrap().height;
        let block_ids = insert_blocks(conn, &blocks_updates)?;
        let transaction_updates = blocks_updates.iter().map(|block| block.transactions.iter());
        insert_transactions(conn, transaction_updates.clone(), &block_ids)?;
        insert_addresses(conn, transaction_updates)?;
        insert_data_entries(conn, blocks_updates, &block_ids)?;
        info!("inserted {:?} block", h);
    }
    Ok(())
}

fn insert_blocks<U: TransactionsRepo + ?Sized>(
    conn: &U,
    blocks_updates: &[&BlockMicroblockAppend],
) -> Result<Vec<i64>> {
    let blocks = blocks_updates.iter().map(|&b| b.into()).collect::<Vec<_>>();
    let start = Instant::now();
    let block_ids = conn.insert_blocks_or_microblocks(&blocks)?;
    info!(
        "insert {} blocks in {} ms",
        blocks.len(),
        start.elapsed().as_millis()
    );
    Ok(block_ids)
}

fn insert_transactions<'a, U: TransactionsRepo + ?Sized>(
    conn: &U,
    transaction_updates: impl Iterator<Item = impl Iterator<Item = &'a TransactionUpdate>>,
    block_ids: &[i64],
) -> Result<()> {
    let transactions_chunks = block_ids
        .iter()
        .zip(transaction_updates)
        .flat_map(|(&block_uid, txs)| txs.map(move |tx| (block_uid, tx)))
        .chunks_from_iter(TX_CHUNK_SIZE);
    for txs in transactions_chunks {
        let mut transactions = vec![];
        for tx in txs {
            let transaction = Transaction::try_from(tx)?;
            transactions.push(transaction);
        }
        let start = Instant::now();
        if !transactions.is_empty() {
            conn.insert_transactions(&transactions)?;
            info!(
                "insert {} txs in {} ms",
                transactions.len(),
                start.elapsed().as_millis()
            );
        }
    }
    Ok(())
}

fn insert_addresses<'a, U: TransactionsRepo + ?Sized>(
    conn: &U,
    transaction_updates: impl Iterator<Item = impl Iterator<Item = &'a TransactionUpdate>>,
) -> Result<()> {
    let addresses_chunks = transaction_updates
        .flat_map(|txs| {
            txs.flat_map(|tx| {
                tx.addresses
                    .iter()
                    .map(move |address| (tx.id.clone(), address).into())
            })
        })
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .chunks_from_iter(ADDRESSES_CHUNK_SIZE);
    for addresses in addresses_chunks {
        if !addresses.is_empty() {
            let start = Instant::now();
            conn.insert_associated_addresses(&addresses)?;
            info!(
                "insert {} addresses in {} ms",
                addresses.len(),
                start.elapsed().as_millis()
            );
        }
    }
    Ok(())
}

fn insert_data_entries<U: TransactionsRepo + ?Sized>(
    conn: &U,
    blocks_updates: Vec<&BlockMicroblockAppend>,
    block_ids: &[i64],
) -> Result<()> {
    let start = Instant::now();
    let next_uid = conn.get_next_update_uid()?;

    let entries = blocks_updates
        .iter()
        .zip(block_ids)
        .flat_map(|(block, &block_id)| block.data_entries.iter().map(move |de| (de, block_id)))
        .enumerate()
        .map(|(idx, (de, block_id))| (de, idx as i64 + next_uid, block_id).into());

    let updates_count = entries.clone().count() as i64;

    let mut grouped_updates: HashMap<InsertableDataEntry, Vec<InsertableDataEntry>> =
        HashMap::new();

    entries.for_each(|item: InsertableDataEntry| {
        let group = grouped_updates.entry(item.clone()).or_insert_with(Vec::new);
        group.push(item);
    });

    let mut grouped_updates = grouped_updates
        .into_iter()
        .map(|(_k, v)| v)
        .collect::<Vec<_>>();

    for group in grouped_updates.iter_mut() {
        group.sort_by_key(|item| item.uid);
        let mut last_uid = std::i64::MAX - 1;
        for update in group.iter_mut().rev() {
            update.superseded_by = last_uid;
            last_uid = update.uid;
        }
    }

    // First uid for each asset in a new batch. This value closes superseded_by of previous updates.
    let first_uids: Vec<DataEntryUpdate> = grouped_updates
        .iter()
        .map(|group| {
            let first = group.first().cloned().unwrap();
            DataEntryUpdate {
                address: first.address,
                key: first.key,
                superseded_by: first.uid,
            }
        })
        .collect();

    conn.close_superseded_by(&first_uids)?;

    let mut updates_with_uids_superseded_by =
        grouped_updates.into_iter().flatten().collect::<Vec<_>>();

    updates_with_uids_superseded_by.sort_by_key(|de| de.uid);

    conn.insert_data_entries(&updates_with_uids_superseded_by)?;
    conn.set_next_update_uid(next_uid + updates_count)?;
    info!(
        "insert {} data_entries in {} ms",
        updates_count,
        start.elapsed().as_millis()
    );
    Ok(())
}

fn rollback<U: TransactionsRepo + ?Sized>(conn: &U, block_id: &str) -> Result<()> {
    let block_uid = conn.get_block_uid(block_id)?;
    Ok(rollback_by_block_uid(conn, block_uid)?)
}

fn rollback_by_block_uid<U: TransactionsRepo + ?Sized>(conn: &U, block_uid: i64) -> Result<()> {
    let deletes = conn.rollback_data_entries(&block_uid)?;

    let mut grouped_deletes = HashMap::new();

    deletes.into_iter().for_each(|item| {
        let group = grouped_deletes.entry(item.clone()).or_insert_with(Vec::new);
        group.push(item);
    });

    let lowest_deleted_uids: Vec<i64> = grouped_deletes
        .into_iter()
        .filter_map(|(_, group)| group.into_iter().min_by_key(|i| i.uid).map(|i| i.uid))
        .collect();

    conn.reopen_superseded_by(&lowest_deleted_uids)?;
    conn.rollback_blocks_microblocks(&block_uid)?;
    Ok(())
}

fn count_txs_addresses(buffer: &[BlockchainUpdate]) -> (usize, usize) {
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

fn squash_microblocks<P: TransactionsRepoPool>(pool: &P) -> Result<()> {
    pool.transaction(|conn| {
        if let Some(total_block_id) = conn.get_total_block_id()? {
            let key_block_uid = conn.get_key_block_uid()?;

            conn.update_data_entries_block_references(&key_block_uid)?;
            conn.update_transactions_block_references(&key_block_uid)?;
            conn.delete_microblocks()?;
            conn.change_block_id(&key_block_uid, &total_block_id)?;
        }

        Ok(())
    })?;

    Ok(())
}
