use std::{collections::HashSet, sync::RwLock};

use super::{BlockData, DataFromBlock, LastValue};
use crate::{
    asset_info::AssetStorage,
    db::repo_provider::ProviderRepo,
    decimal::{Decimal, UnknownScale},
    error::Result,
    waves::{
        self, encode_asset,
        transactions::{TransactionType, TransactionUpdate},
    },
};
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use itertools::Itertools;
use serde::Serialize;
use tokio::sync::{Mutex as TokioMutex, MutexGuard as TokioMutexGuard};
use waves_protobuf_schemas::waves::transaction::Data;
use wavesexchange_apis::chrono::Utc;
use wx_topic::ExchangePair;

const PRICE_ASSET_DECIMALS: u8 = 8;

#[derive(Debug, Clone)]
pub struct ExchangePairData {
    pub amount_asset: String,
    pub price_asset: String,
    pub amount_asset_volume: Decimal<UnknownScale>,
    pub price_asset_volume: Decimal<UnknownScale>,
    pub tx_id: String,
    pub height: i32,
    pub block_id: String,
    pub time_stamp: i64,
}

#[derive(Default)]
pub struct ExchangePairsStorage {
    pairs_data: RwLock<Vec<ExchangePairData>>,
    loaded_pairs: RwLock<HashSet<ExchangePair>>,
    load_mutex: TokioMutex<bool>,
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
    pub fn add_transaction(&self, pair: ExchangePairData) {
        let mut storage_guard = self.pairs_data.write().unwrap();
        // This needs to be optimized: collection scan
        if storage_guard.iter().all(|p| p.tx_id != pair.tx_id) {
            storage_guard.push(pair);
        }
    }

    fn pair_is_loaded(&self, pair: &ExchangePair) -> bool {
        self.loaded_pairs.read().unwrap().contains(pair)
    }

    fn push_pairs_data(&self, pairs: Vec<ExchangePairData>) {
        let Some(pair) = pairs.first() else { return };

        let pair = ExchangePair {
            amount_asset: pair.amount_asset.clone(),
            price_asset: pair.price_asset.clone(),
        };

        let mut storage_guard = self.pairs_data.write().unwrap();

        let mut loaded_guard = self.loaded_pairs.write().unwrap();

        // This needs to be optimized: collection scan
        let pairs = {
            let mut pairs = pairs;
            pairs.retain(|p| storage_guard.iter().all(|pp| p.tx_id != pp.tx_id));
            pairs
        };

        storage_guard.extend(pairs.into_iter());
        loaded_guard.insert(pair);
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
            .filter(|t| {
                t.amount_asset == pair.amount_asset
                    && t.price_asset == pair.price_asset
                    //all microblocks or last 24h blocks
                    && (t.time_stamp < 1 || t.time_stamp > last_day_stamp)
            })
            .sorted_by(|l, r| r.time_stamp.cmp(&l.time_stamp))
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

        storage_guard.retain(|pair_data| {
            let delete = removed_tx_ids.contains(&pair_data.tx_id);
            if delete {
                let pair = ExchangePair {
                    amount_asset: pair_data.amount_asset.clone(),
                    price_asset: pair_data.price_asset.clone(),
                };
                changed_pairs.insert(pair);
            }
            !delete
        });

        changed_pairs
    }

    fn cleanup(&self) -> HashSet<ExchangePair> {
        let trunc_stamp = Utc::now().timestamp_millis() - 86400000; // 1 day ago

        let mut changed_pairs = HashSet::new();

        let mut storage_guard = self.pairs_data.write().unwrap();

        storage_guard.retain(|pair_data| {
            let delete = pair_data.time_stamp < trunc_stamp;
            if delete {
                let pair = ExchangePair {
                    amount_asset: pair_data.amount_asset.clone(),
                    price_asset: pair_data.price_asset.clone(),
                };
                changed_pairs.insert(pair);
            }
            !delete
        });

        changed_pairs
    }

    async fn lock_load_mutex(&self) -> TokioMutexGuard<bool> {
        self.load_mutex.lock().await
    }
}

impl DataFromBlock for ExchangePair {
    type Context = PairsContext;

    fn data_from_block(
        block: &waves::BlockMicroblockAppend,
        ctx: &PairsContext,
    ) -> Vec<BlockData<ExchangePair>> {
        let pairs_in_block = extract_exchange_pairs(&block, ctx);

        let changed_pairs = ctx.pairs_storage.cleanup();

        let pairs_to_recompute = pairs_in_block
            .into_iter()
            .chain(changed_pairs.into_iter())
            .unique()
            .collect_vec();

        pairs_to_recompute
            .into_iter()
            .filter(|pair| ctx.pairs_storage.pair_is_loaded(pair))
            .map(|pair| {
                let current_value = ctx.pairs_storage.calc_stat(&pair, &ctx.asset_storage);
                let current_value = serde_json::to_string(&current_value).unwrap();
                BlockData::new(current_value, pair)
            })
            .collect()
    }

    fn data_from_rollback(
        rollback: &waves::RollbackData,
        ctx: &PairsContext,
    ) -> Vec<BlockData<ExchangePair>> {
        let removed_tx_ids = rollback
            .removed_transaction_ids
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let changed_pairs = ctx.pairs_storage.rollback(&removed_tx_ids);

        changed_pairs
            .into_iter()
            .filter(|pair| ctx.pairs_storage.pair_is_loaded(pair))
            .map(|pair| {
                let current_value = ctx.pairs_storage.calc_stat(&pair, &ctx.asset_storage);
                let current_value = serde_json::to_string(&current_value).unwrap();
                BlockData::new(current_value, pair)
            })
            .collect()
    }
}

fn extract_exchange_pairs(
    block: &waves::BlockMicroblockAppend,
    ctx: &PairsContext,
) -> Vec<ExchangePair> {
    let unique_pairs_in_block = block
        .transactions
        .iter()
        .filter(|t| t.tx_type == TransactionType::Exchange)
        .filter_map(|t| extract_pairs_data(&block, t))
        .map(|exchange_data| {
            let pair = ExchangePair {
                amount_asset: exchange_data.amount_asset.clone(),
                price_asset: exchange_data.price_asset.clone(),
            };
            ctx.pairs_storage.add_transaction(exchange_data);
            pair
        })
        .unique()
        .collect_vec();

    unique_pairs_in_block
}

fn extract_pairs_data(
    block: &waves::BlockMicroblockAppend,
    update: &TransactionUpdate,
) -> Option<ExchangePairData> {
    let pair_data = match &update.data {
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

            Some(ExchangePairData {
                amount_asset: encode_asset(&asset_pair.amount_asset_id),
                price_asset: encode_asset(&asset_pair.price_asset_id),
                amount_asset_volume: Decimal::new_raw(exchange_data.amount),
                price_asset_volume: Decimal::new_raw(exchange_data.price),
                tx_id: update.id.clone(),
                height: block.height,
                block_id: block.id.clone(),
                time_stamp: timestamp,
            })
        }
        _ => None,
    };

    pair_data
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
        if ctx.pairs_storage.pair_is_loaded(self) {
            return Ok(false);
        }

        let lock = ctx.pairs_storage.lock_load_mutex().await;

        // check second time after locking to be sure that some other thread do not load same data
        if ctx.pairs_storage.pair_is_loaded(self) {
            return Ok(false);
        }

        ctx.asset_storage.preload_decimals_for_pair(self).await?;

        let pairs = repo.last_exchange_pairs_transactions(self.clone()).await?;

        ctx.pairs_storage.push_pairs_data(pairs);

        drop(lock);

        Ok(true)
    }
}
