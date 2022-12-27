use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use waves_protobuf_schemas::waves::events::BlockchainUpdated;
use wavesexchange_log::{debug, info};

use crate::db::repo_consumer::{ConsumerRepo, ConsumerRepoOperations};
use crate::db::{
    BlockchainUpdate, DataEntry, DataEntryUpdate, LeasingBalance, LeasingBalanceUpdate,
    PrevHandledHeight,
};
use crate::error::{Error, Result};
use crate::metrics::DB_WRITE_TIME;
use crate::utils::chunks::ToChunks;
use crate::waves::transactions::{InsertableTransaction, TransactionUpdate};
use crate::waves::BlockMicroblockAppend;

const TX_CHUNK_SIZE: usize = 65535 / 4;
const ADDRESSES_CHUNK_SIZE: usize = 65535 / 2;

pub struct Updater<R: ConsumerRepo> {
    rx: mpsc::Receiver<Arc<BlockchainUpdated>>,
    repo: R,
    updates_buffer_size: usize,
    transactions_count_threshold: usize,
    associated_addresses_count_threshold: usize,
    providers: Vec<mpsc::Sender<Arc<Vec<BlockchainUpdate>>>>,
    waiting_blocks_timeout: Duration,
}

pub struct UpdaterReturn<R: ConsumerRepo> {
    pub last_height: i32,
    pub tx: mpsc::Sender<Arc<BlockchainUpdated>>,
    pub updater: Updater<R>,
}

#[derive(Debug)]
enum UpdatesSequenceState {
    Ok,
    HasMicroBlocks,
    NeedSquash,
}

impl Default for UpdatesSequenceState {
    fn default() -> Self {
        Self::Ok
    }
}

impl<R: ConsumerRepo> Updater<R> {
    pub async fn init(
        repo: R,
        updates_buffer_size: usize,
        transactions_count_threshold: usize,
        associated_addresses_count_threshold: usize,
        waiting_blocks_timeout: Duration,
    ) -> Result<UpdaterReturn<R>> {
        let (tx, rx) = mpsc::channel(updates_buffer_size);
        let last_height = {
            let prev_handled_height = repo.execute(|ops| ops.get_prev_handled_height()).await?;
            match prev_handled_height {
                Some(PrevHandledHeight { uid, height }) => {
                    repo.transaction(move |ops| rollback_by_block_uid(ops, uid))
                        .await?;
                    height as i32 + 1
                }
                None => 1i32,
            }
        };
        let updater = Self {
            rx,
            repo,
            updates_buffer_size,
            transactions_count_threshold,
            associated_addresses_count_threshold,
            providers: vec![],
            waiting_blocks_timeout,
        };
        Ok(UpdaterReturn {
            last_height,
            tx,
            updater,
        })
    }

    pub fn add_provider(&mut self, provider: mpsc::Sender<Arc<Vec<BlockchainUpdate>>>) {
        self.providers.push(provider);
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut microblock_flag = UpdatesSequenceState::default();
        'a: loop {
            let mut buffer = Vec::with_capacity(self.updates_buffer_size);
            if let Some(event) = self.rx.recv().await {
                let update = BlockchainUpdate::try_from(event)?;
                buffer.push(update);
                match buffer.last().unwrap() {
                    BlockchainUpdate::Microblock(_) => {
                        if let UpdatesSequenceState::Ok = microblock_flag {
                            microblock_flag = UpdatesSequenceState::HasMicroBlocks
                        }
                        // If we've received microblock, flush current batch immediately.
                        // We are currently on the top of blockchain and don't want to delay updates.
                        self.process_updates(buffer, &mut microblock_flag).await?;
                        continue;
                    }
                    BlockchainUpdate::Block(_) => {
                        if let UpdatesSequenceState::HasMicroBlocks = microblock_flag {
                            microblock_flag = UpdatesSequenceState::NeedSquash;
                            self.process_updates(buffer, &mut microblock_flag).await?;
                            continue;
                        }
                    }
                    BlockchainUpdate::Rollback(_) => {
                        self.process_updates(buffer, &mut microblock_flag).await?;
                        continue;
                    }
                }
                let delay = tokio::time::sleep(self.waiting_blocks_timeout);
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
                                        if let UpdatesSequenceState::Ok = microblock_flag {
                                            microblock_flag = UpdatesSequenceState::HasMicroBlocks
                                        }
                                        // If we've received microblock, flush current batch immediately.
                                        // We are currently on the top of blockchain and don't want to delay updates.
                                        self.process_updates(buffer, &mut microblock_flag).await?;
                                        break;
                                    }
                                    BlockchainUpdate::Block(_) => {
                                        if let UpdatesSequenceState::HasMicroBlocks = microblock_flag {
                                            microblock_flag = UpdatesSequenceState::NeedSquash;
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
                                    || addresses_count >= self.associated_addresses_count_threshold
                                {
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
        microblock_flag: &mut UpdatesSequenceState,
    ) -> Result<()> {
        let start = Instant::now();
        info!(
            "start handling updates batch of size {}",
            blockchain_updates.len()
        );

        let blockchain_updates = Arc::new(blockchain_updates);

        // Write updates to the Postgres database
        write_updates(&self.repo, blockchain_updates.clone(), microblock_flag).await?;

        let elapsed = start.elapsed();
        let elapsed_ms = elapsed.as_millis() as i64;
        DB_WRITE_TIME.set(elapsed_ms);
        info!(
            "{} updates were handled in {} ms (~{} updates/s)",
            blockchain_updates.len(),
            elapsed_ms,
            if elapsed.as_secs() > 0 {
                blockchain_updates.len() / elapsed.as_secs() as usize
            } else {
                blockchain_updates.len() * 1000 / elapsed.as_millis() as usize
            }
        );

        // Write updates to Redis
        for tx in self.providers.iter_mut() {
            tx.send(blockchain_updates.clone())
                .await
                .map_err(|_| Error::SendErrorVecBlockchainUpdate)?
        }

        Ok(())
    }
}

async fn write_updates<R: ConsumerRepo>(
    repo: &R,
    blockchain_updates: Arc<Vec<BlockchainUpdate>>,
    microblock_flag: &mut UpdatesSequenceState,
) -> Result<()> {
    if let UpdatesSequenceState::NeedSquash = microblock_flag {
        let mut i = 0;
        let mut blockchain_updates_iter = blockchain_updates.iter().enumerate();
        loop {
            match blockchain_updates_iter.next() {
                Some((idx, BlockchainUpdate::Block(_))) => {
                    insert_blockchain_updates(repo, Arc::new(blockchain_updates[..idx].to_vec()))
                        .await?;
                    squash_microblocks(repo).await?;
                    *microblock_flag = UpdatesSequenceState::default();
                    i = idx;
                    break;
                }
                Some(_) => (),
                None => break,
            }
        }
        insert_blockchain_updates(repo, Arc::new(blockchain_updates[i..].to_vec())).await?;
    } else {
        insert_blockchain_updates(repo, blockchain_updates).await?;
    }

    Ok(())
}

async fn insert_blockchain_updates<'a, R: ConsumerRepo>(
    repo: &R,
    blockchain_updates: Arc<Vec<BlockchainUpdate>>,
) -> Result<()> {
    repo.transaction(move |ops| {
        let mut appends = vec![];
        let mut rollback_block_id = None;
        for update in blockchain_updates.iter() {
            match update {
                BlockchainUpdate::Block(block) => appends.push(block),
                BlockchainUpdate::Microblock(block) => appends.push(block),
                BlockchainUpdate::Rollback(rb) => rollback_block_id = Some(&rb.block_id),
            }
        }
        insert_appends(ops, appends)?;
        if let Some(block_id) = rollback_block_id {
            rollback(ops, block_id)?;
            info!("rolled back to block id {}", block_id);
        }

        Ok(())
    })
    .await?;

    Ok(())
}

fn insert_appends<O: ConsumerRepoOperations>(
    repo_ops: &mut O,
    appends: Vec<&BlockMicroblockAppend>,
) -> Result<()> {
    if !appends.is_empty() {
        let h = appends.last().unwrap().height;
        let block_uids = insert_blocks(repo_ops, &appends)?;
        let transaction_updates = appends.iter().map(|block| block.transactions.iter());

        insert_transactions(repo_ops, transaction_updates.clone(), &block_uids)?;
        insert_addresses(repo_ops, transaction_updates)?;
        insert_data_entries(repo_ops, &appends, &block_uids)?;
        insert_leasing_balances(repo_ops, &appends, &block_uids)?;

        info!("handled updates batch last height {:?}", h);
    }
    Ok(())
}

fn insert_blocks<O: ConsumerRepoOperations>(
    repo_ops: &mut O,
    appends: &[&BlockMicroblockAppend],
) -> Result<Vec<i64>> {
    let blocks = appends.iter().map(|&b| b.into()).collect::<Vec<_>>();
    let start = Instant::now();
    let block_uids = repo_ops.insert_blocks_or_microblocks(&blocks)?;
    debug!(
        "{} blocks were inserted in {} ms",
        blocks.len(),
        start.elapsed().as_millis()
    );
    Ok(block_uids)
}

fn insert_transactions<'a, O: ConsumerRepoOperations>(
    repo_ops: &mut O,
    transaction_updates: impl Iterator<Item = impl Iterator<Item = &'a TransactionUpdate>>,
    block_uids: &[i64],
) -> Result<()> {
    let start = Instant::now();
    let mut inserted_transactions_count = 0;
    let transactions_chunks = block_uids
        .iter()
        .zip(transaction_updates)
        .flat_map(|(&block_uid, txs)| txs.map(move |tx| (block_uid, tx)))
        .chunks_from_iter(TX_CHUNK_SIZE);
    for txs in transactions_chunks {
        let mut transactions = vec![];
        for tx in txs {
            let transaction = InsertableTransaction::try_from(tx)?;
            transactions.push(transaction);
        }
        if !transactions.is_empty() {
            repo_ops.insert_transactions(&transactions)?;
        }
        inserted_transactions_count += transactions.len();
    }
    debug!(
        "{} txs were inserted in {} ms",
        inserted_transactions_count,
        start.elapsed().as_millis()
    );
    Ok(())
}

fn insert_addresses<'a, O: ConsumerRepoOperations>(
    repo_ops: &mut O,
    transaction_updates: impl Iterator<Item = impl Iterator<Item = &'a TransactionUpdate>>,
) -> Result<()> {
    let start = Instant::now();
    let mut inserted_addresses_count = 0;
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
            repo_ops.insert_associated_addresses(&addresses)?;
            inserted_addresses_count += addresses.len();
        }
    }
    debug!(
        "{} addresses were inserted in {} ms",
        inserted_addresses_count,
        start.elapsed().as_millis()
    );
    Ok(())
}

fn insert_data_entries<O: ConsumerRepoOperations>(
    repo_ops: &mut O,
    blocks_updates: &[&BlockMicroblockAppend],
    block_uids: &[i64],
) -> Result<()> {
    let start = Instant::now();
    let next_uid = repo_ops.get_next_update_uid()?;

    let entries = blocks_updates
        .iter()
        .zip(block_uids)
        .flat_map(|(block, &block_uid)| block.data_entries.iter().map(move |de| (de, block_uid)))
        .enumerate()
        .map(|(idx, (de, block_uid))| (de, idx as i64 + next_uid, block_uid).into());

    let data_entries_count = entries.clone().count() as i64;

    let mut grouped_updates: HashMap<DataEntry, Vec<DataEntry>> = HashMap::new();

    entries.for_each(|item: DataEntry| {
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

    repo_ops.close_superseded_by(&first_uids)?;

    let mut updates_with_uids_superseded_by =
        grouped_updates.into_iter().flatten().collect::<Vec<_>>();

    updates_with_uids_superseded_by.sort_by_key(|de| de.uid);

    repo_ops.insert_data_entries(&updates_with_uids_superseded_by)?;
    repo_ops.set_next_update_uid(next_uid + data_entries_count)?;
    debug!(
        "{} data entries were inserted in {} ms",
        data_entries_count,
        start.elapsed().as_millis()
    );
    Ok(())
}

fn insert_leasing_balances<O: ConsumerRepoOperations>(
    repo_ops: &mut O,
    blocks_updates: &[&BlockMicroblockAppend],
    block_uids: &[i64],
) -> Result<()> {
    let start = Instant::now();
    let next_uid = repo_ops.get_next_lease_update_uid()?;

    let leasing_balances = blocks_updates
        .iter()
        .zip(block_uids)
        .flat_map(|(block, &block_uid)| block.leasing_balances.iter().map(move |l| (l, block_uid)))
        .enumerate()
        .map(|(idx, (l, block_uid))| (l, idx as i64 + next_uid, block_uid).into());

    let leasing_balances_count = leasing_balances.clone().count() as i64;

    let mut grouped_updates: HashMap<LeasingBalance, Vec<LeasingBalance>> = HashMap::new();

    leasing_balances.for_each(|item: LeasingBalance| {
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
    let first_uids: Vec<LeasingBalanceUpdate> = grouped_updates
        .iter()
        .map(|group| {
            let first = group.first().cloned().unwrap();
            LeasingBalanceUpdate {
                address: first.address,
                superseded_by: first.uid,
            }
        })
        .collect();

    repo_ops.close_lease_superseded_by(&first_uids)?;

    let mut updates_with_uids_superseded_by =
        grouped_updates.into_iter().flatten().collect::<Vec<_>>();

    updates_with_uids_superseded_by.sort_by_key(|de| de.uid);

    repo_ops.insert_leasing_balances(&updates_with_uids_superseded_by)?;
    repo_ops.set_next_lease_update_uid(next_uid + leasing_balances_count)?;
    debug!(
        "{} leasing balances were inserted in {} ms",
        leasing_balances_count,
        start.elapsed().as_millis()
    );
    Ok(())
}

fn rollback<O: ConsumerRepoOperations>(repo_ops: &mut O, block_id: &str) -> Result<()> {
    let block_uid = repo_ops.get_block_uid(block_id)?;
    rollback_by_block_uid(repo_ops, block_uid)
}

fn rollback_by_block_uid<O: ConsumerRepoOperations>(
    repo_ops: &mut O,
    block_uid: i64,
) -> Result<()> {
    let deletes = repo_ops.rollback_data_entries(&block_uid)?;

    let mut grouped_deletes = HashMap::new();

    deletes.into_iter().for_each(|item| {
        let group = grouped_deletes.entry(item.clone()).or_insert_with(Vec::new);
        group.push(item);
    });

    let lowest_deleted_uids: Vec<i64> = grouped_deletes
        .into_iter()
        .filter_map(|(_, group)| group.into_iter().min_by_key(|i| i.uid).map(|i| i.uid))
        .collect();

    repo_ops.reopen_superseded_by(&lowest_deleted_uids)?;

    let deletes = repo_ops.rollback_leasing_balances(&block_uid)?;

    let mut grouped_deletes = HashMap::new();

    deletes.into_iter().for_each(|item| {
        let group = grouped_deletes.entry(item.clone()).or_insert_with(Vec::new);
        group.push(item);
    });

    let lowest_deleted_uids: Vec<i64> = grouped_deletes
        .into_iter()
        .filter_map(|(_, group)| group.into_iter().min_by_key(|i| i.uid).map(|i| i.uid))
        .collect();

    repo_ops.reopen_lease_superseded_by(&lowest_deleted_uids)?;
    repo_ops.rollback_blocks_microblocks(&block_uid)?;
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

async fn squash_microblocks<R: ConsumerRepo>(repo: &R) -> Result<()> {
    repo.transaction(|ops| {
        if let Some(total_block_id) = ops.get_total_block_id()? {
            let key_block_uid = ops.get_key_block_uid()?;

            ops.update_data_entries_block_references(&key_block_uid)?;
            ops.update_leasing_balances_block_references(&key_block_uid)?;
            ops.update_transactions_block_references(&key_block_uid)?;
            ops.delete_microblocks()?;
            ops.change_block_id(&key_block_uid, &total_block_id)?;
        }

        Ok(())
    })
    .await?;

    Ok(())
}
