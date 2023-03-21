use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use super::{BlockData, DataFromBlock, Item, LastValue};
use crate::{
    db::repo_provider::ProviderRepo,
    error::{Error, Result},
    waves::{
        self, encode_asset,
        transactions::{TransactionType, TransactionUpdate},
    },
};
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use diesel::sql_types::{Bigint, Integer, VarChar};
use itertools::Itertools;
use serde::Serialize;
use tokio::sync::{Mutex as TokioMutex, MutexGuard as TokioMutexGuard, RwLock as TokioRwLock};
use waves_protobuf_schemas::waves::transaction::Data;
use wavesexchange_apis::{
    assets::dto::{AssetInfo, OutputFormat},
    chrono::Utc,
    AssetsService, HttpClient,
};
use wavesexchange_log::{debug, info, warn};
use wavesexchange_topic::ExchangePair;

const PRICE_ASSET_DECIMALS: i32 = 8;

#[derive(Debug, Clone, QueryableByName)]
pub struct ExchangePairsData {
    #[diesel(sql_type = VarChar)]
    pub amount_asset: String,

    #[diesel(sql_type = VarChar)]
    pub price_asset: String,

    #[diesel(sql_type = Bigint)]
    pub amount_asset_volume: i64,

    #[diesel(sql_type = Bigint)]
    pub price_asset_volume: i64,

    #[diesel(sql_type = Integer)]
    pub height: i32,

    #[diesel(sql_type = VarChar)]
    pub block_id: String,

    #[diesel(sql_type = Bigint)]
    pub block_time_stamp: i64,
}

pub struct ExchangePairsStorage {
    pairs_data: Arc<RwLock<Vec<ExchangePairsData>>>,
    blocks_rowlog: Arc<RwLock<Vec<(String, i64)>>>,
    asset_decimals: Arc<RwLock<HashMap<String, i32>>>,
    asset_service_client: Arc<TokioRwLock<Option<HttpClient<AssetsService>>>>,
    last_block_timestamp: Arc<RwLock<i64>>,
    loaded_pairs: Arc<RwLock<HashSet<(String, String)>>>,
    load_mutex: TokioMutex<bool>,
}

#[derive(Debug, Serialize)]
pub struct ExchangePairsDailyStat {
    amount_assset: String,
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
    pub fn new() -> Self {
        Self {
            pairs_data: Arc::new(RwLock::new(vec![])),
            blocks_rowlog: Arc::new(RwLock::new(vec![])),
            asset_decimals: Arc::new(RwLock::new(HashMap::new())),
            last_block_timestamp: Arc::new(RwLock::new(-1)),
            loaded_pairs: Arc::new(RwLock::new(HashSet::new())),
            asset_service_client: Arc::new(TokioRwLock::new(None)),
            load_mutex: TokioMutex::new(true),
        }
    }

    pub async fn setup_asset_service_client(&self, assets_url: &str) {
        let assets_srv: HttpClient<AssetsService> =
            HttpClient::<AssetsService>::from_base_url(&*assets_url);

        let mut cl = self.asset_service_client.write().await;

        *cl = Some(assets_srv);
    }

    pub fn add(&self, pair: ExchangePairsData) {
        let mut strorage_guard = (*self.pairs_data)
            .write()
            .expect("write lock pairs_data error");
        strorage_guard.push(pair);
    }

    pub fn pair_is_loaded(&self, amount_asset: &str, price_asset: &str) -> bool {
        (*self.loaded_pairs)
            .read()
            .expect("read lock loaded_pairs error")
            .contains(&(amount_asset.into(), price_asset.into()))
    }

    pub fn push_pairs_data(&self, pairs: Vec<ExchangePairsData>) {
        let Some(pair) = pairs.first() else { return };

        debug!(
            "loaded exchange pairs data for amount_asset: {}; price_asset: {}; records: {}",
            &pair.amount_asset,
            &pair.price_asset,
            pairs.len()
        );

        let key = (pair.amount_asset.clone(), pair.price_asset.clone());

        let mut strorage_guard = (*self.pairs_data)
            .write()
            .expect("write lock pairs_data error");

        let mut loaded_guard = (*self.loaded_pairs)
            .write()
            .expect("write lock loaded_pairs error");

        strorage_guard.extend(pairs.into_iter());
        loaded_guard.insert(key);
    }

    pub fn calc_stat(&self, amount_asset: &str, price_asset: &str) -> ExchangePairsDailyStat {
        debug!(
            "calculating stat for amount_asset: {}; price_asset:{} ",
            &amount_asset, &price_asset
        );

        let mut stat = ExchangePairsDailyStat {
            amount_assset: amount_asset.into(),
            price_asset: price_asset.into(),
            txs_count: 0,
            first_price: None,
            last_price: None,
            low: None,
            high: None,
            volume: None,
            quote_volume: None,
        };

        let ex_transactions = self.pairs_data.read().expect("write lock pairs_data error");

        let asset_decimals = self
            .asset_decimals
            .read()
            .expect("write lock asset_decimals error");

        let amount_dec = asset_decimals.get(amount_asset).expect(
            format!(
                "amount asset decimals not found. asset_id: {}",
                &amount_asset
            )
            .as_str(),
        );

        let mut low: i64 = i64::MAX;
        let mut high: i64 = 0;
        let mut volume: i64 = 0;
        let mut quote_volume = BigDecimal::from(0);

        let last_day_stamp = Utc::now().timestamp_millis() - 86400000;

        ex_transactions
            .iter()
            .filter(|t| {
                t.amount_asset == *amount_asset
                    && t.price_asset == *price_asset
                    //all microblocks or last 24h blocks
                    && (t.block_time_stamp < 1 || t.block_time_stamp > last_day_stamp)
            })
            .sorted_by(|l, r| r.block_time_stamp.cmp(&l.block_time_stamp))
            .for_each(|i| {
                stat.txs_count += 1;

                if stat.first_price.is_none() {
                    stat.first_price =
                        Some(apply_decimals(&i.price_asset_volume, &PRICE_ASSET_DECIMALS));
                }

                stat.last_price =
                    Some(apply_decimals(&i.price_asset_volume, &PRICE_ASSET_DECIMALS));

                if low > i.price_asset_volume {
                    stat.low = Some(apply_decimals(&i.price_asset_volume, &PRICE_ASSET_DECIMALS));
                    low = i.price_asset_volume;
                }

                if high < i.price_asset_volume {
                    stat.high = Some(apply_decimals(&i.price_asset_volume, &PRICE_ASSET_DECIMALS));
                    high = i.price_asset_volume;
                }

                volume += i.amount_asset_volume;

                quote_volume += apply_decimals(&i.amount_asset_volume, amount_dec)
                    * apply_decimals(&i.price_asset_volume, &PRICE_ASSET_DECIMALS);
            });

        stat.volume = Some(apply_decimals(&volume, amount_dec));
        stat.quote_volume = Some(quote_volume.with_scale(*&PRICE_ASSET_DECIMALS as i64));

        stat
    }

    fn solidify_microblocks(&self, ref_block_id: &str) {
        let mut rowlog_guard = (*self.blocks_rowlog)
            .write()
            .expect("write lock blocks_rowlog error");

        let mut solid_timestamp = None;

        let len = rowlog_guard.len();

        let solidify_ids: Vec<String> = rowlog_guard
            .iter()
            .rev()
            .enumerate()
            .take_while(|rb| {
                if rb.1 .1 == 0 {
                    return true;
                }
                solid_timestamp = Some(rb.1 .1);
                false
            })
            .map(|i| i.1 .0.clone())
            .collect();

        rowlog_guard.drain(len - solidify_ids.len() - 1..);

        rowlog_guard.push((
            ref_block_id.into(),
            solid_timestamp.expect("solidify error timestamp not found in blocks_microblocks"),
        ));

        self.update_pairs_block_ids(ref_block_id, solidify_ids);
    }

    fn update_pairs_block_ids(&self, ref_block_id: &str, solidify_ids: Vec<String>) {
        let mut storage_guard = (*self.pairs_data)
            .write()
            .expect("write lock pairs_data error");

        storage_guard.iter_mut().for_each(|b| {
            if solidify_ids.iter().find(|id| **id == b.block_id).is_some() {
                b.block_id = ref_block_id.into();
            }
        });
    }

    pub fn push_block_rowlog(&self, block_id: &str, ref_block_id: &str, block_timestamp: &i64) {
        let mut timestamp_guard = (*self.last_block_timestamp)
            .write()
            .expect("write lock last_block_timestamp error");

        if *timestamp_guard == 0 && *block_timestamp != 0 {
            self.solidify_microblocks(&ref_block_id);
        }

        let mut rowlog_guard = (*self.blocks_rowlog)
            .write()
            .expect("write lock blocks_rowlog error");

        rowlog_guard.push((block_id.into(), *block_timestamp));
        *timestamp_guard = *block_timestamp;
    }

    pub fn rollback(&self, block_id: &str) -> Vec<ExchangePair> {
        let mut rowlog_guard = (*self.blocks_rowlog)
            .write()
            .expect("write lock blocks_rowlog error");

        let mut del_idx: usize = 0;

        let len = rowlog_guard.len();

        let blocks_to_del: Vec<String> = rowlog_guard
            .iter()
            .rev()
            .enumerate()
            .take_while(|rb| {
                if rb.1 .0 == *block_id {
                    del_idx = rb.0;
                    return false;
                }
                true
            })
            .map(|i| (i.1).0.clone())
            .collect();

        if del_idx < 1 {
            warn!("rollback block id:{} not found; skipping", block_id);
            return vec![];
        }

        let changed_pairs = self.delete_pairs_by_block_ids(&blocks_to_del);

        rowlog_guard.drain(len - del_idx..);
        assert!(!rowlog_guard.is_empty());
        rowlog_guard.shrink_to_fit();

        changed_pairs
    }

    fn delete_pairs_by_block_ids(&self, ids: &[String]) -> Vec<ExchangePair> {
        let mut storage_guard = (*self.pairs_data)
            .write()
            .expect("write pairs_data lock error");

        let mut changed_pairs = vec![];

        storage_guard.retain(|b| {
            if ids.iter().find(|id| *id == &b.block_id).is_some() {
                changed_pairs.push(ExchangePair {
                    amount_asset: b.amount_asset.clone(),
                    price_asset: b.price_asset.clone(),
                });
                return false;
            }
            true
        });

        storage_guard.shrink_to_fit();
        changed_pairs.into_iter().unique().collect()
    }

    pub fn cleanup(&self) -> Vec<ExchangePair> {
        let mut rowlog_guard = (*self.blocks_rowlog)
            .write()
            .expect("write lock blocks_rowlog error");

        let timestamp_guard = (*self.last_block_timestamp)
            .read()
            .expect("read lock last_block_timestamp error");

        let trunc_stamp = *timestamp_guard - 86400000; // 1 day ago

        let blocks_to_del: Vec<String> = {
            rowlog_guard
                .iter()
                .take_while(|rb| trunc_stamp > rb.1)
                .map(|i| i.0.clone())
                .collect()
        };

        rowlog_guard.drain(..blocks_to_del.len());
        rowlog_guard.shrink_to_fit();

        self.delete_pairs_by_block_ids(&blocks_to_del)
    }

    pub fn debug_heap_size(&self) {
        //need to find a way to calc size of storage size get-size crate cause Poison Errors
        info!("ExchangePairsStorage heap size: {}", "unknown");
    }

    #[cfg(test)]
    pub(crate) fn rowlog(&self) -> Vec<(String, i64)> {
        (*self
            .blocks_rowlog
            .read()
            .expect("read blocks_rowlog lock error"))
        .to_vec()
    }

    #[cfg(test)]
    pub(crate) fn pairs_data(&self) -> Vec<ExchangePairsData> {
        (*self.pairs_data.read().unwrap()).to_vec()
    }

    #[cfg(test)]
    pub(crate) fn solidify_microblocks_test(&self, ref_block_id: &str) {
        self.solidify_microblocks(ref_block_id)
    }

    #[cfg(test)]
    pub(crate) fn delete_pairs_by_block_ids_test(&self, ids: &[String]) -> Vec<ExchangePair> {
        self.delete_pairs_by_block_ids(ids)
    }

    #[cfg(test)]
    pub(crate) fn update_pairs_block_ids_test(
        &self,
        ref_block_id: &str,
        solidify_ids: Vec<String>,
    ) {
        self.update_pairs_block_ids(ref_block_id, solidify_ids)
    }

    #[cfg(test)]
    pub(crate) fn set_last_block_timestamp(&mut self, t: i64) {
        self.last_block_timestamp = Arc::new(RwLock::new(t))
    }
}

#[async_trait]
trait ExchangePairsStorageAsyncTrait {
    async fn push_asset_decimals(&self, amount_asset: &str, price_asset: &str) -> Result<()>;
    async fn lock_for_init_last_value(&self) -> TokioMutexGuard<bool>;
}

#[async_trait]
impl ExchangePairsStorageAsyncTrait for ExchangePairsStorage {
    async fn push_asset_decimals(&self, amount_asset: &str, price_asset: &str) -> Result<()> {
        {
            let r = self
                .asset_decimals
                .read()
                .expect("read lock asset_decimals error");

            if r.contains_key(amount_asset) && r.contains_key(price_asset) {
                return Ok(());
            }
        }

        let assets = [amount_asset, price_asset];

        let assets_info = self
            .asset_service_client
            .read()
            .await
            .as_ref()
            .expect("asset service client not configured")
            .get(assets, None, OutputFormat::Full, false)
            .await
            .map_err(|e| Error::AssetServiceClientError(e.to_string()))?;

        let mut asset_decimals = self
            .asset_decimals
            .write()
            .expect("write lock asset_decimals error");

        for a in assets_info.data {
            match a.data {
                Some(AssetInfo::Full(f)) => {
                    asset_decimals.insert(f.id, f.precision);
                }
                _ => unreachable!(),
            }
        }

        Ok(())
    }

    async fn lock_for_init_last_value(&self) -> TokioMutexGuard<bool> {
        self.load_mutex.lock().await
    }
}

#[async_trait]
pub trait ExchangePairsStorageProviderRepoTrait<R: ProviderRepo + Sync> {
    async fn load_blocks_rowlog(&self, repo: &R) -> Result<()>;
}

#[async_trait]
impl<R: ProviderRepo + Sync> ExchangePairsStorageProviderRepoTrait<R> for ExchangePairsStorage {
    async fn load_blocks_rowlog(&self, repo: &R) -> Result<()> {
        let blocks = repo.last_blocks_microblocks().await?;

        let mut rowlog_guard = (*self.blocks_rowlog)
            .write()
            .expect("write lock blocks_rowlog error");

        rowlog_guard.clear();

        blocks.iter().for_each(|b| {
            rowlog_guard.push((
                b.id.clone(),
                b.time_stamp.expect("block time_stamp is None"),
            ))
        });

        Ok(())
    }
}

impl DataFromBlock for ExchangePair {
    fn data_from_block(block: &waves::BlockMicroblockAppend) -> Vec<BlockData<ExchangePair>> {
        let mut pairs_in_block = extract_exchange_pairs(&block);

        pairs_in_block.append(&mut crate::EXCHANGE_PAIRS_STORAGE.cleanup());

        crate::EXCHANGE_PAIRS_STORAGE.debug_heap_size();

        let pairs_in_block: Vec<ExchangePair> = pairs_in_block.into_iter().unique().collect();

        pairs_in_block
            .iter()
            .filter(|i| {
                crate::EXCHANGE_PAIRS_STORAGE.pair_is_loaded(&i.amount_asset, &i.price_asset)
            })
            .map(|p| {
                let current_value =
                    crate::EXCHANGE_PAIRS_STORAGE.calc_stat(&p.amount_asset, &p.price_asset);
                BlockData::new(
                    serde_json::to_string(&current_value).unwrap(),
                    ExchangePair {
                        amount_asset: p.amount_asset.clone(),
                        price_asset: p.price_asset.clone(),
                    },
                )
            })
            .collect()
    }

    fn data_from_rollback(rollback: &waves::RollbackData) -> Vec<BlockData<ExchangePair>> {
        let changed_pairs = crate::EXCHANGE_PAIRS_STORAGE.rollback(&rollback.block_id);

        changed_pairs
            .into_iter()
            .filter(|i| {
                crate::EXCHANGE_PAIRS_STORAGE.pair_is_loaded(&i.amount_asset, &i.price_asset)
            })
            .map(|i| {
                let current_value = serde_json::to_string(
                    &crate::EXCHANGE_PAIRS_STORAGE.calc_stat(&i.amount_asset, &i.price_asset),
                )
                .unwrap();

                BlockData::new(current_value, i)
            })
            .collect()
    }
}

fn extract_exchange_pairs(block: &waves::BlockMicroblockAppend) -> Vec<ExchangePair> {
    let pairs_in_block: Vec<(String, String)> = block
        .transactions
        .iter()
        .filter(|t| t.tx_type == TransactionType::Exchange)
        .map(|t| match extract_pairs_data(&block, &t) {
            Some(ed) => {
                let out = (ed.amount_asset.clone(), ed.price_asset.clone());
                crate::EXCHANGE_PAIRS_STORAGE.add(ed);
                Some(out)
            }
            _ => None,
        })
        .filter(|i| i.is_some())
        .map(Option::unwrap)
        .collect();

    crate::EXCHANGE_PAIRS_STORAGE.push_block_rowlog(
        &block.id,
        &block.ref_id,
        block.time_stamp.as_ref().unwrap_or(&0),
    );

    pairs_in_block
        .into_iter()
        .unique()
        .into_iter()
        .map(|p| ExchangePair {
            amount_asset: p.0.clone(),
            price_asset: p.1.clone(),
        })
        .collect()
}

fn extract_pairs_data(
    block: &waves::BlockMicroblockAppend,
    update: &TransactionUpdate,
) -> Option<ExchangePairsData> {
    let pair_data = match &update.data {
        Data::Exchange(exchange_data) => {
            let asset_pair = &exchange_data
                .orders
                .get(0)
                .expect("invalid exchange data order")
                .asset_pair
                .as_ref()
                .expect("invalid ExchangeTransactionData::order[0]::AssetPair");

            Some(ExchangePairsData {
                amount_asset: encode_asset(&asset_pair.amount_asset_id),
                price_asset: encode_asset(&asset_pair.price_asset_id),
                amount_asset_volume: exchange_data.amount,
                price_asset_volume: exchange_data.price,
                height: block.height,
                block_id: block.id.clone(),
                block_time_stamp: update.timestamp,
            })
        }
        _ => None,
    };

    pair_data
}

#[async_trait]
impl<R: ProviderRepo + Sync> LastValue<R> for ExchangePair {
    async fn last_value(self, _repo: &R) -> Result<String> {
        let curent_value = serde_json::to_string(
            &crate::EXCHANGE_PAIRS_STORAGE.calc_stat(&self.amount_asset, &self.price_asset),
        )?;

        Ok(curent_value)
    }

    async fn init_last_value(&self, repo: &R) -> Result<bool> {
        if crate::EXCHANGE_PAIRS_STORAGE.pair_is_loaded(&self.amount_asset, &self.price_asset) {
            return Ok(false);
        }

        let lock = crate::EXCHANGE_PAIRS_STORAGE
            .lock_for_init_last_value()
            .await;

        if crate::EXCHANGE_PAIRS_STORAGE.pair_is_loaded(&self.amount_asset, &self.price_asset) {
            // check second time after locking to be sure that some other thread do not load same data
            drop(lock);
            return Ok(false);
        }

        crate::EXCHANGE_PAIRS_STORAGE
            .push_asset_decimals(&self.amount_asset, &self.price_asset)
            .await?;

        debug!(
            "ExchangePair loading last transactions for pair: {}/{}",
            &self.amount_asset, &self.price_asset
        );

        let pairs = repo
            .last_exchange_pairs_transactions(self.amount_asset.clone(), self.price_asset.clone())
            .await?;

        debug!(
            "ExchangePair::last_value:{}/{} cnt:{}",
            &self.amount_asset,
            &self.price_asset,
            pairs.len()
        );

        crate::EXCHANGE_PAIRS_STORAGE.push_pairs_data(pairs);

        drop(lock);

        Ok(true)
    }
}

impl<R: ProviderRepo + Sync> Item<R> for ExchangePair {}

pub fn apply_decimals(num: &i64, dec: &i32) -> BigDecimal {
    (BigDecimal::from(*num) / (10i64.pow(*dec as u32))).with_scale(*dec as i64)
}
