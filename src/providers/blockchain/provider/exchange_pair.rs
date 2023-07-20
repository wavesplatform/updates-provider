use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Condvar, Mutex, RwLock},
};

use super::{BlockData, DataFromBlock, LastValue};
use crate::{
    asset_info::AssetStorage,
    db::repo_provider::ProviderRepo,
    decimal::{Decimal, UnknownScale},
    error::Result,
    waves::{
        encode_asset,
        transactions::{TransactionType, TransactionUpdate},
        BlockMicroblockAppend, RollbackData,
    },
};
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use itertools::Itertools;
use scopeguard::defer;
use serde::Serialize;
use tokio::task;
use waves_protobuf_schemas::waves::transaction::Data;
use wavesexchange_apis::chrono::Utc;
use wx_topic::ExchangePair;

const PRICE_ASSET_DECIMALS: u8 = 8;

#[derive(Debug, Clone)]
pub struct ExchangePairTx {
    pub tx_id: String,
    pub time_stamp: i64,
    pub amount_asset_volume: Decimal<UnknownScale>,
    pub price_asset_volume: Decimal<UnknownScale>,
}

#[derive(Default)]
pub struct PairsStorage {
    /// All transactions for the last 24h
    transactions: RwLock<Vec<(ExchangePair, ExchangePairTx)>>,
    /// Pairs that are fully loaded (or being loaded) from database + blockchain
    pairs: Mutex<HashMap<ExchangePair, Arc<PairState>>>,
}

#[derive(Default)]
struct PairState {
    is_loaded: Condvar,
    loaded_lock: Mutex<bool>,
}

pub struct PairsContext {
    pub asset_storage: AssetStorage,
    pub pairs_storage: PairsStorage,
}

#[derive(Debug, Serialize)]
pub struct ExchangePairsDailyStat {
    amount_asset: String,
    price_asset: String,
    first_price: Option<BigDecimal>,
    last_price: Option<BigDecimal>,
    low: Option<BigDecimal>,
    high: Option<BigDecimal>,
    volume: Option<BigDecimal>,
    quote_volume: Option<BigDecimal>,
    txs_count: i64,
}

impl PairsStorage {
    fn add_transactions(&self, txs: Vec<(ExchangePair, ExchangePairTx)>) {
        let mut storage_txs = self.transactions.write().unwrap();
        for (pair, tx) in txs {
            // This needs to be optimized: collection scan
            if storage_txs.iter().all(|&(_, ref pd)| pd.tx_id != tx.tx_id) {
                storage_txs.push((pair, tx));
            }
        }
    }

    fn set_pair_initial_transactions(&self, pair: &ExchangePair, txs: Vec<ExchangePairTx>) {
        let mut storage_txs = self.transactions.write().unwrap();

        // This needs to be optimized: collection scan
        let txs = {
            let mut txs = txs;
            txs.retain(|p| storage_txs.iter().all(|&(_, ref pp)| p.tx_id != pp.tx_id));
            txs
        };

        storage_txs.extend(txs.into_iter().map(|data| (pair.to_owned(), data)));
    }

    fn calc_stat(
        &self,
        pair: &ExchangePair,
        asset_storage: &AssetStorage,
    ) -> Option<ExchangePairsDailyStat> {
        let mut stat = ExchangePairsDailyStat {
            amount_asset: pair.amount_asset.clone(),
            price_asset: pair.price_asset.clone(),
            txs_count: 0,
            first_price: None,
            last_price: None,
            low: None,
            high: None,
            volume: None,
            quote_volume: None,
        };

        let storage_txs = self.transactions.read().unwrap();

        let amount_decimals = asset_storage.decimals_for_asset(&pair.amount_asset)?;

        // This is a hack: we don't need price asset decimals for computations,
        // but this allows to check whether price asset is invalid
        // and therefore exit early returning `None`.
        let _ = asset_storage.decimals_for_asset(&pair.price_asset)?;

        let mut low = i64::MAX;
        let mut high = 0_i64;
        let mut volume = BigDecimal::from(0);
        let mut quote_volume = BigDecimal::from(0);

        let last_day_stamp = Utc::now().timestamp_millis() - 86400000;

        storage_txs
            .iter()
            .filter(|&&(ref p, ref d)| {
                p.amount_asset == pair.amount_asset
                    && p.price_asset == pair.price_asset
                    //all microblocks or last 24h blocks
                    && (d.time_stamp < 1 || d.time_stamp > last_day_stamp)
            })
            .map(|&(_, ref data)| data)
            .sorted_by_key(|pd| pd.time_stamp)
            .rev()
            .for_each(|pair_data| {
                stat.txs_count += 1;

                let price_asset_volume = pair_data
                    .price_asset_volume
                    .with_scale(PRICE_ASSET_DECIMALS)
                    .to_big();
                let amount_asset_volume = pair_data
                    .amount_asset_volume
                    .with_scale(amount_decimals)
                    .to_big();

                // Because transactions are sorted in reverse order, we need to switch first_price/last_price here
                if stat.last_price.is_none() {
                    stat.last_price = Some(price_asset_volume.clone());
                }

                // Because transactions are sorted in reverse order, we need to switch first_price/last_price here
                stat.first_price = Some(price_asset_volume.clone());

                if low > pair_data.price_asset_volume.raw_value() {
                    stat.low = Some(price_asset_volume.clone());
                    low = pair_data.price_asset_volume.raw_value();
                }

                if high < pair_data.price_asset_volume.raw_value() {
                    stat.high = Some(price_asset_volume.clone());
                    high = pair_data.price_asset_volume.raw_value();
                }

                volume += &amount_asset_volume;
                quote_volume += &amount_asset_volume * &price_asset_volume;
            });

        stat.volume = Some(volume.with_scale(amount_decimals as i64));
        stat.quote_volume = Some(quote_volume.with_scale(PRICE_ASSET_DECIMALS as i64));

        Some(stat)
    }

    fn rollback(&self, removed_tx_ids: &HashSet<String>) -> HashSet<ExchangePair> {
        if removed_tx_ids.is_empty() {
            return HashSet::new();
        }

        let mut changed_pairs = HashSet::new();

        let mut storage_txs = self.transactions.write().unwrap();

        storage_txs.retain(|(pair, data)| {
            let delete = removed_tx_ids.contains(&data.tx_id);
            if delete {
                changed_pairs.insert(pair.clone());
            }
            !delete
        });

        changed_pairs
    }

    fn cleanup(&self) -> HashSet<ExchangePair> {
        let trunc_stamp = Utc::now().timestamp_millis() - 86400000; // 1 day ago

        let mut changed_pairs = HashSet::new();

        let mut storage_txs = self.transactions.write().unwrap();

        storage_txs.retain(|(pair, data)| {
            let delete = data.time_stamp < trunc_stamp;
            if delete {
                changed_pairs.insert(pair.clone());
            }
            !delete
        });

        changed_pairs
    }
}

impl DataFromBlock for ExchangePair {
    type Context = PairsContext;

    fn data_from_block(
        block: &BlockMicroblockAppend,
        ctx: &PairsContext,
    ) -> Vec<BlockData<ExchangePair>> {
        let mut pairs_to_recompute = HashSet::new();

        let txs_from_block = block
            .transactions
            .iter()
            .filter(|t| t.tx_type == TransactionType::Exchange)
            .filter_map(extract_pair_data)
            .collect_vec();

        for (pair, _) in &txs_from_block {
            if !pairs_to_recompute.contains(pair) {
                pairs_to_recompute.insert(pair.to_owned());
            }
        }

        ctx.pairs_storage.add_transactions(txs_from_block);

        // Storage cleanup (removal of transactions that became older than 24h)
        // should be a proper background (or timer-based) task, but instead
        // we perform cleanup on every incoming block. This is more or less acceptable
        // as long as the blocks are coming at a steady rate.
        // As the result of cleanup there can be some pairs that needs to be recomputed,
        // in addition to those actually present in the block.
        let changed_pairs = ctx.pairs_storage.cleanup();

        pairs_to_recompute.extend(changed_pairs);

        let loaded_pairs = ctx.pairs_storage.pairs.lock().unwrap();

        pairs_to_recompute
            .into_iter()
            .filter(|pair| loaded_pairs.contains_key(pair))
            .map(|pair| {
                let current_value = ctx.pairs_storage.calc_stat(&pair, &ctx.asset_storage);
                let current_value = serde_json::to_string(&current_value).unwrap();
                BlockData::new(current_value, pair)
            })
            .collect()
    }

    fn data_from_rollback(
        rollback: &RollbackData,
        ctx: &PairsContext,
    ) -> Vec<BlockData<ExchangePair>> {
        let removed_tx_ids = rollback
            .removed_transaction_ids
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let changed_pairs = ctx.pairs_storage.rollback(&removed_tx_ids);

        let loaded_pairs = ctx.pairs_storage.pairs.lock().unwrap();

        changed_pairs
            .into_iter()
            .filter(|pair| loaded_pairs.contains_key(pair))
            .map(|pair| {
                let current_value = ctx.pairs_storage.calc_stat(&pair, &ctx.asset_storage);
                let current_value = serde_json::to_string(&current_value).unwrap();
                BlockData::new(current_value, pair)
            })
            .collect()
    }
}

fn extract_pair_data(update: &TransactionUpdate) -> Option<(ExchangePair, ExchangePairTx)> {
    match &update.data {
        Data::Exchange(exchange_data) => {
            let asset_pair = exchange_data
                .orders
                .get(0)
                .expect("invalid exchange data order")
                .asset_pair
                .as_ref()
                .expect("invalid ExchangeTransactionData::order[0]::AssetPair");

            //TODO It seems that update.timestamp is a Tx timestamp which is inaccurate,
            // probably better to use block timestamp
            let timestamp = if update.timestamp > 0 {
                update.timestamp
            } else {
                Utc::now().timestamp_millis()
            };

            let pair = ExchangePair {
                amount_asset: encode_asset(&asset_pair.amount_asset_id),
                price_asset: encode_asset(&asset_pair.price_asset_id),
            };

            let pair_data = ExchangePairTx {
                tx_id: update.id.clone(),
                time_stamp: timestamp,
                amount_asset_volume: Decimal::new_raw(exchange_data.amount),
                price_asset_volume: Decimal::new_raw(exchange_data.price),
            };

            Some((pair, pair_data))
        }
        _ => None,
    }
}

#[async_trait]
impl<R: ProviderRepo + Sync> LastValue<R> for ExchangePair {
    type Context = PairsContext;

    async fn last_value(self, _repo: &R, ctx: &PairsContext) -> Result<String> {
        let current_value =
            serde_json::to_string(&ctx.pairs_storage.calc_stat(&self, &ctx.asset_storage))?;
        Ok(current_value)
    }

    async fn init_context(&self, repo: &R, ctx: &PairsContext) -> Result<bool> {
        // Get loading status of the pair
        let (pair_state, being_loaded) = ctx.pairs_storage.pair_loading_state(self);

        if being_loaded {
            // This pair is already loaded (or being loaded) by another task
            let waited = pair_state.block_until_fully_loaded();

            // Pair is now loaded, so we are done
            return Ok(waited);
        }

        // This code is executed even if one of the subsequent calls fails
        defer! {
            // Signal other tasks that this pair is finished loading
            pair_state.notify_loaded();
        }

        // Not loaded before - initiate loading

        ctx.asset_storage.preload_decimals_for_pair(self).await?;

        let transactions = repo.last_exchange_pairs_transactions(self).await?;

        ctx.pairs_storage
            .set_pair_initial_transactions(self, transactions);

        Ok(true)
    }
}

impl PairsStorage {
    fn pair_loading_state(&self, pair: &ExchangePair) -> (Arc<PairState>, bool) {
        let mut pairs = self.pairs.lock().unwrap();
        if let Some(existing_state) = pairs.get(pair).cloned() {
            (existing_state, true)
        } else {
            let new_state = Arc::new(PairState::default());
            pairs.insert(pair.to_owned(), Arc::clone(&new_state));
            (new_state, false)
        }
    }
}

impl PairState {
    fn block_until_fully_loaded(&self) -> bool {
        let mut loaded = self.loaded_lock.lock().unwrap();

        if *loaded {
            return false;
        }

        task::block_in_place(|| {
            while !*loaded {
                loaded = self.is_loaded.wait(loaded).unwrap();
            }
        });

        return true;
    }

    fn notify_loaded(&self) {
        let mut loaded = self.loaded_lock.lock().unwrap();
        *loaded = true;
        self.is_loaded.notify_all();
    }
}
