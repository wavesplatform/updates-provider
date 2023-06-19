use std::{collections::HashSet, sync::RwLock};

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
use serde::Serialize;
use tokio::sync::Mutex as TokioMutex;
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
pub struct ExchangePairsStorage {
    pairs_data: RwLock<Vec<(ExchangePair, ExchangePairTx)>>,
    loaded_pairs: RwLock<HashSet<ExchangePair>>,
    load_mutex: TokioMutex<()>,
}

pub struct PairsContext {
    pub asset_storage: AssetStorage,
    pub pairs_storage: ExchangePairsStorage,
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

impl ExchangePairsStorage {
    pub fn add_transaction(&self, pair: ExchangePair, data: ExchangePairTx) {
        let mut storage_guard = self.pairs_data.write().unwrap();
        // This needs to be optimized: collection scan
        if storage_guard
            .iter()
            .all(|&(_, ref pd)| pd.tx_id != data.tx_id)
        {
            storage_guard.push((pair, data));
        }
    }

    fn set_pair_initial_transactions(&self, pair: &ExchangePair, txs: Vec<ExchangePairTx>) {
        let mut storage_guard = self.pairs_data.write().unwrap();

        let mut loaded_guard = self.loaded_pairs.write().unwrap();

        // This needs to be optimized: collection scan
        let txs = {
            let mut txs = txs;
            txs.retain(|p| storage_guard.iter().all(|&(_, ref pp)| p.tx_id != pp.tx_id));
            txs
        };

        storage_guard.extend(txs.into_iter().map(|data| (pair.to_owned(), data)));
        loaded_guard.insert(pair.to_owned());
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

        let ex_transactions = self.pairs_data.read().unwrap();

        let amount_decimals = asset_storage.decimals_for_asset(&pair.amount_asset)?;

        let mut low = i64::MAX;
        let mut high = 0_i64;
        let mut volume = BigDecimal::from(0);
        let mut quote_volume = BigDecimal::from(0);

        let last_day_stamp = Utc::now().timestamp_millis() - 86400000;

        ex_transactions
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

        let mut storage_guard = self.pairs_data.write().unwrap();

        storage_guard.retain(|(pair, data)| {
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

        let mut storage_guard = self.pairs_data.write().unwrap();

        storage_guard.retain(|(pair, data)| {
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
            .filter_map(extract_pair_data);

        for (pair, data) in txs_from_block {
            if !pairs_to_recompute.contains(&pair) {
                pairs_to_recompute.insert(pair.clone());
            }
            ctx.pairs_storage.add_transaction(pair, data);
        }

        // Storage cleanup (removal of transactions that became older than 24h)
        // should be a proper background (or timer-based) task, but instead
        // we perform cleanup on every incoming block. This is more or less acceptable
        // as long as the blocks are coming at a steady rate.
        // As the result of cleanup there can be some pairs that needs to be recomputed,
        // in addition to those actually present in the block.
        let changed_pairs = ctx.pairs_storage.cleanup();

        pairs_to_recompute.extend(changed_pairs);

        let loaded_pairs = ctx.pairs_storage.loaded_pairs.read().unwrap();

        pairs_to_recompute
            .into_iter()
            .filter(|pair| loaded_pairs.contains(pair))
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

        let loaded_pairs = ctx.pairs_storage.loaded_pairs.read().unwrap();

        changed_pairs
            .into_iter()
            .filter(|pair| loaded_pairs.contains(pair))
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

    async fn init_last_value(&self, repo: &R, ctx: &PairsContext) -> Result<bool> {
        if ctx
            .pairs_storage
            .loaded_pairs
            .read()
            .unwrap()
            .contains(self)
        {
            return Ok(false);
        }

        let lock = ctx.pairs_storage.load_mutex.lock().await;

        // check second time after locking to be sure that some other thread do not load same data
        if ctx
            .pairs_storage
            .loaded_pairs
            .read()
            .unwrap()
            .contains(self)
        {
            return Ok(false);
        }

        ctx.asset_storage.preload_decimals_for_pair(self).await?;

        let transactions = repo.last_exchange_pairs_transactions(self).await?;

        ctx.pairs_storage
            .set_pair_initial_transactions(self, transactions);

        drop(lock);

        Ok(true)
    }
}
