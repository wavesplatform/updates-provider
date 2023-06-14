use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
    time::Duration,
};

use super::{BlockData, DataFromBlock, Item, LastValue};
use crate::{
    db::repo_provider::ProviderRepo,
    decimal::{Decimal, UnknownScale},
    error::{Error, Result},
    waves::{
        self, encode_asset,
        transactions::{TransactionType, TransactionUpdate},
    },
};
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use itertools::Itertools;
use serde::Serialize;
use tokio::{
    sync::{Mutex as TokioMutex, MutexGuard as TokioMutexGuard, RwLock as TokioRwLock},
    time::sleep as tokio_sleep,
};
use waves_protobuf_schemas::waves::transaction::Data;
use wavesexchange_apis::{
    assets::dto::{AssetInfo, OutputFormat},
    chrono::Utc,
    AssetsService, HttpClient,
};
use wavesexchange_log::{debug, error};
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
    pub block_time_stamp: i64,
}

pub struct ExchangePairsStorage {
    pairs_data: Arc<RwLock<Vec<ExchangePairData>>>,
    blocks_rowlog: Arc<RwLock<Vec<(String, i64)>>>,
    asset_decimals: Arc<RwLock<HashMap<String, u8>>>,
    asset_service_client: Arc<TokioRwLock<Option<HttpClient<AssetsService>>>>,
    last_block_timestamp: Arc<RwLock<i64>>,
    loaded_pairs: Arc<RwLock<HashSet<ExchangePair>>>,
    load_mutex: TokioMutex<bool>,
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

    fn is_rowlog_loaded(&self) -> bool {
        let rowlog_guard = self.blocks_rowlog.read().unwrap();

        rowlog_guard.len() > 0
    }

    pub async fn setup_asset_service_client(&self, assets_url: &str) {
        let assets_srv: HttpClient<AssetsService> =
            HttpClient::<AssetsService>::from_base_url(&*assets_url);

        let mut cl = self.asset_service_client.write().await;

        *cl = Some(assets_srv);
    }

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

    fn calc_stat(&self, pair: &ExchangePair) -> Option<ExchangePairsDailyStat> {
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

        let asset_decimals = self.asset_decimals.read().unwrap();

        let amount_dec = match asset_decimals.get(&pair.amount_asset) {
            Some(a) => *a,
            None => {
                debug!(
                    "amount asset decimals not found. asset_id: {}",
                    pair.amount_asset
                );
                return None;
            }
        };

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
                    && (t.block_time_stamp < 1 || t.block_time_stamp > last_day_stamp)
            })
            .sorted_by(|l, r| r.block_time_stamp.cmp(&l.block_time_stamp))
            .for_each(|pair_data| {
                stat.txs_count += 1;

                let price_asset_volume = pair_data
                    .price_asset_volume
                    .with_scale(PRICE_ASSET_DECIMALS)
                    .to_big();
                let amount_asset_volume = pair_data
                    .amount_asset_volume
                    .with_scale(amount_dec)
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

        stat.volume = Some(volume.with_scale(amount_dec as i64));
        stat.quote_volume = Some(quote_volume.with_scale(PRICE_ASSET_DECIMALS as i64));

        Some(stat)
    }

    fn solidify_microblocks(&self, ref_block_id: &str) {
        let mut rowlog_guard = self.blocks_rowlog.write().unwrap();

        let mut solid_timestamp = None;

        let len = rowlog_guard.len();

        let solidify_ids = rowlog_guard
            .iter()
            .rev()
            .take_while(|&&(_, ts)| {
                if ts == 0 {
                    return true;
                }
                solid_timestamp = Some(ts);
                false
            })
            .map(|&(ref id, _)| id.to_owned())
            .collect_vec();

        rowlog_guard.drain(len - solidify_ids.len() - 1..);

        rowlog_guard.push((
            ref_block_id.into(),
            solid_timestamp.expect("solidify error timestamp not found in blocks_microblocks"),
        ));

        self.update_pairs_block_ids(ref_block_id, solidify_ids);
    }

    fn update_pairs_block_ids(&self, ref_block_id: &str, solidify_ids: Vec<String>) {
        let mut storage_guard = self.pairs_data.write().unwrap();

        storage_guard.iter_mut().for_each(|b| {
            if solidify_ids.iter().find(|id| **id == b.block_id).is_some() {
                b.block_id = ref_block_id.into();
            }
        });
    }

    fn push_block_rowlog(&self, block_id: &str, ref_block_id: &str, block_timestamp: i64) {
        let mut timestamp_guard = self.last_block_timestamp.write().unwrap();

        if *timestamp_guard == 0 && block_timestamp != 0 {
            self.solidify_microblocks(&ref_block_id);
        }

        let mut rowlog_guard = self.blocks_rowlog.write().unwrap();

        rowlog_guard.push((block_id.into(), block_timestamp));
        *timestamp_guard = block_timestamp;
    }

    fn rollback(&self, block_id: &str) -> Vec<ExchangePair> {
        let mut rowlog_guard = self.blocks_rowlog.write().unwrap();

        let mut del_idx = 0;

        let len = rowlog_guard.len();

        let blocks_to_del: Vec<String> = rowlog_guard
            .iter()
            .rev()
            .enumerate()
            .take_while(|&(idx, &(ref id, _))| {
                if *id == *block_id {
                    del_idx = idx;
                    return false;
                }
                true
            })
            .map(|(_, &(ref id, _))| id.to_owned())
            .collect();

        if del_idx < 1 {
            let mut b_cnt = 0;
            for &(_, ts) in rowlog_guard.iter().rev() {
                if ts > 0 {
                    b_cnt += 1;
                }
                if b_cnt > 2 {
                    break;
                }
            }

            return vec![];
        }

        let changed_pairs = self.delete_pairs_by_block_ids(&blocks_to_del);

        rowlog_guard.drain(len - del_idx..);
        assert!(!rowlog_guard.is_empty());
        rowlog_guard.shrink_to_fit();

        changed_pairs
    }

    fn delete_pairs_by_block_ids(&self, ids: &[String]) -> Vec<ExchangePair> {
        if ids.is_empty() {
            return vec![];
        }

        let mut storage_guard = self.pairs_data.write().unwrap();

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

        //storage_guard.shrink_to_fit();
        changed_pairs.into_iter().unique().collect()
    }

    fn cleanup(&self) -> Vec<ExchangePair> {
        let mut rowlog_guard = self.blocks_rowlog.write().unwrap();

        let timestamp_guard = self.last_block_timestamp.read().unwrap();

        let trunc_stamp = *timestamp_guard - 86400000; // 1 day ago

        let blocks_to_del: Vec<String> = {
            rowlog_guard
                .iter()
                .take_while(|&&(_, ts)| trunc_stamp > ts)
                .map(|&(ref id, _)| id.to_owned())
                .collect()
        };

        rowlog_guard.drain(..blocks_to_del.len());
        //rowlog_guard.shrink_to_fit();

        self.delete_pairs_by_block_ids(&blocks_to_del)
    }

    #[cfg(test)]
    fn rowlog(&self) -> Vec<(String, i64)> {
        self.blocks_rowlog.read().unwrap().to_vec()
    }

    #[cfg(test)]
    fn pairs_data(&self) -> Vec<ExchangePairData> {
        self.pairs_data.read().unwrap().to_vec()
    }

    #[cfg(test)]
    fn solidify_microblocks_test(&self, ref_block_id: &str) {
        self.solidify_microblocks(ref_block_id)
    }

    #[cfg(test)]
    fn delete_pairs_by_block_ids_test(&self, ids: &[String]) -> Vec<ExchangePair> {
        self.delete_pairs_by_block_ids(ids)
    }

    #[cfg(test)]
    fn update_pairs_block_ids_test(&self, ref_block_id: &str, solidify_ids: Vec<String>) {
        self.update_pairs_block_ids(ref_block_id, solidify_ids)
    }

    #[cfg(test)]
    fn set_last_block_timestamp(&mut self, t: i64) {
        self.last_block_timestamp = Arc::new(RwLock::new(t))
    }

    async fn push_asset_decimals(&self, pair: &ExchangePair) -> Result<()> {
        {
            let r = self.asset_decimals.read().unwrap();

            if r.contains_key(&pair.amount_asset) && r.contains_key(&pair.price_asset) {
                return Ok(());
            }
        }

        let assets = [pair.amount_asset.as_str(), pair.price_asset.as_str()];

        let mut sleep_dur = 30;
        while sleep_dur < 600 {
            let resp = self
                .asset_service_client
                .read()
                .await
                .as_ref()
                .expect("asset service client not configured")
                .get(assets, None, OutputFormat::Full, false)
                .await;

            match resp {
                Ok(assets_info) => {
                    let mut asset_decimals = self.asset_decimals.write().unwrap();

                    for a in assets_info.data {
                        match a.data {
                            Some(AssetInfo::Full(f)) => {
                                assert!(
                                    f.precision >= 0 && f.precision <= 30,
                                    "probably bad precision value {} for asset {}",
                                    f.precision,
                                    f.id
                                );
                                asset_decimals.insert(f.id, f.precision as u8);
                            }
                            _ => {
                                error!("bad AssetInfo from asset-service: {:?}", a);
                            }
                        }
                    }
                    return Ok(());
                }
                Err(e) => {
                    debug!(
                        "asset-service returned error, sleeping {}s: {}",
                        sleep_dur, e
                    );
                    tokio_sleep(Duration::from_secs(sleep_dur)).await;
                    sleep_dur *= 2;
                }
            }
        }

        Err(Error::AssetServiceClientError(format!(
            "failed to query decimals from asset-service for assets {} and {}",
            pair.amount_asset, pair.price_asset
        )))
    }

    async fn lock_load_mutex(&self) -> TokioMutexGuard<bool> {
        self.load_mutex.lock().await
    }

    pub async fn load_blocks_rowlog<R: ProviderRepo + Sync>(&self, repo: &R) -> Result<()> {
        if self.is_rowlog_loaded() {
            return Ok(());
        }

        let lock = self.lock_load_mutex().await;

        if self.is_rowlog_loaded() {
            return Ok(());
        }

        let blocks = repo.last_blocks_microblocks().await?;

        let mut rowlog_guard = self.blocks_rowlog.write().unwrap();

        rowlog_guard.clear();

        blocks.iter().for_each(|b| {
            rowlog_guard.push((
                b.id.clone(),
                b.time_stamp.expect("block time_stamp is None"),
            ))
        });

        drop(lock);

        Ok(())
    }
}

impl DataFromBlock for ExchangePair {
    fn data_from_block(block: &waves::BlockMicroblockAppend) -> Vec<BlockData<ExchangePair>> {
        let mut pairs_in_block = extract_exchange_pairs(&block);

        pairs_in_block.append(&mut crate::EXCHANGE_PAIRS_STORAGE.cleanup());

        let pairs_in_block = pairs_in_block.into_iter().unique().collect_vec();

        pairs_in_block
            .into_iter()
            .filter(|pair| crate::EXCHANGE_PAIRS_STORAGE.pair_is_loaded(pair))
            .map(|pair| {
                let current_value = crate::EXCHANGE_PAIRS_STORAGE.calc_stat(&pair);
                let current_value = serde_json::to_string(&current_value).unwrap();
                BlockData::new(current_value, pair)
            })
            .collect()
    }

    fn data_from_rollback(rollback: &waves::RollbackData) -> Vec<BlockData<ExchangePair>> {
        let changed_pairs = crate::EXCHANGE_PAIRS_STORAGE.rollback(&rollback.block_id);

        changed_pairs
            .into_iter()
            .filter(|pair| crate::EXCHANGE_PAIRS_STORAGE.pair_is_loaded(pair))
            .map(|pair| {
                let current_value = crate::EXCHANGE_PAIRS_STORAGE.calc_stat(&pair);
                let current_value = serde_json::to_string(&current_value).unwrap();
                BlockData::new(current_value, pair)
            })
            .collect()
    }
}

fn extract_exchange_pairs(block: &waves::BlockMicroblockAppend) -> Vec<ExchangePair> {
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
            crate::EXCHANGE_PAIRS_STORAGE.add_transaction(exchange_data);
            pair
        })
        .unique()
        .collect_vec();

    crate::EXCHANGE_PAIRS_STORAGE.push_block_rowlog(
        &block.id,
        &block.ref_id,
        block.time_stamp.unwrap_or(0),
    );

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

            Some(ExchangePairData {
                amount_asset: encode_asset(&asset_pair.amount_asset_id),
                price_asset: encode_asset(&asset_pair.price_asset_id),
                amount_asset_volume: Decimal::new_raw(exchange_data.amount),
                price_asset_volume: Decimal::new_raw(exchange_data.price),
                tx_id: update.id.clone(),
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
        let current_value = serde_json::to_string(&crate::EXCHANGE_PAIRS_STORAGE.calc_stat(&self))?;
        Ok(current_value)
    }

    async fn init_last_value(&self, repo: &R) -> Result<bool> {
        if crate::EXCHANGE_PAIRS_STORAGE.pair_is_loaded(self) {
            return Ok(false);
        }

        let lock = crate::EXCHANGE_PAIRS_STORAGE.lock_load_mutex().await;

        // check second time after locking to be sure that some other thread do not load same data
        if crate::EXCHANGE_PAIRS_STORAGE.pair_is_loaded(self) {
            return Ok(false);
        }

        crate::EXCHANGE_PAIRS_STORAGE
            .push_asset_decimals(self)
            .await?;

        let pairs = repo.last_exchange_pairs_transactions(self.clone()).await?;

        crate::EXCHANGE_PAIRS_STORAGE.push_pairs_data(pairs);

        drop(lock);

        Ok(true)
    }
}

impl<R: ProviderRepo + Sync> Item<R> for ExchangePair {}

#[cfg(test)]
mod tests {
    use super::{ExchangePairData, ExchangePairsStorage};
    use crate::decimal::Decimal;
    use itertools::Itertools;

    #[test]
    fn solidify_test() {
        let st = ExchangePairsStorage::new();
        st.push_block_rowlog("id_0", "", 1);
        st.push_block_rowlog("id_1", "", 2);
        st.push_block_rowlog("id_2", "", 3);
        st.push_block_rowlog("id_3", "", 0);
        st.push_block_rowlog("id_4", "", 0);

        st.solidify_microblocks_test("id_new");

        assert_eq!(
            st.rowlog(),
            &[
                ("id_0".to_string(), 1),
                ("id_1".to_string(), 2),
                ("id_new".to_string(), 3)
            ]
        );
    }

    #[test]
    fn rollback_test() {
        let st = ExchangePairsStorage::new();
        st.push_block_rowlog("id_0", "", 1);
        st.push_block_rowlog("id_1", "", 2);
        st.push_block_rowlog("id_2", "", 3);
        st.push_block_rowlog("id_3", "", 0);
        st.push_block_rowlog("id_4", "", 0);

        st.rollback("id_1");
        assert_eq!(
            st.rowlog(),
            &[("id_0".to_string(), 1), ("id_1".to_string(), 2)]
        );
    }

    #[test]
    fn delete_by_ids_test() {
        let st = ExchangePairsStorage::new();
        st.add_transaction(ExchangePairData {
            block_id: "id_1".into(),
            block_time_stamp: 0,
            amount_asset: "WAVES".into(),
            price_asset: "DG2xFkPdDwKUoBkzGAhQtLpSGzfXLiCYPEzeKH2Ad24p".into(),
            amount_asset_volume: Decimal::new_raw(0),
            price_asset_volume: Decimal::new_raw(0),
            tx_id: "tx_1".into(),
            height: 0,
        });
        st.add_transaction(ExchangePairData {
            block_id: "id_1".into(),
            block_time_stamp: 0,
            amount_asset: "WAVES".into(),
            price_asset: "DG2xFkPdDwKUoBkzGAhQtLpSGzfXLiCYPEzeKH2Ad24p".into(),
            amount_asset_volume: Decimal::new_raw(0),
            price_asset_volume: Decimal::new_raw(0),
            tx_id: "tx_2".into(),
            height: 0,
        });
        st.add_transaction(ExchangePairData {
            block_id: "id_3".into(),
            block_time_stamp: 0,
            amount_asset: "WAVES".into(),
            price_asset: "DG2xFkPdDwKUoBkzGAhQtLpSGzfXLiCYPEzeKH2Ad24p".into(),
            amount_asset_volume: Decimal::new_raw(0),
            price_asset_volume: Decimal::new_raw(0),
            tx_id: "tx_3".into(),
            height: 0,
        });
        st.add_transaction(ExchangePairData {
            block_id: "id_4".into(),
            block_time_stamp: 0,
            amount_asset: "WAVES".into(),
            price_asset: "DG2xFkPdDwKUoBkzGAhQtLpSGzfXLiCYPEzeKH2Ad24p".into(),
            amount_asset_volume: Decimal::new_raw(0),
            price_asset_volume: Decimal::new_raw(0),
            tx_id: "tx_4".into(),
            height: 0,
        });
        st.add_transaction(ExchangePairData {
            block_id: "id_1".into(),
            block_time_stamp: 0,
            amount_asset: "WAVES".into(),
            price_asset: "DG2xFkPdDwKUoBkzGAhQtLpSGzfXLiCYPEzeKH2Ad24p".into(),
            amount_asset_volume: Decimal::new_raw(0),
            price_asset_volume: Decimal::new_raw(0),
            tx_id: "tx_5".into(),
            height: 0,
        });

        let id_1 = "id_1".to_string();
        let id_4 = "id_4".to_string();

        let to_del = vec![id_1.clone(), id_4.clone()];

        st.delete_pairs_by_block_ids_test(&to_del);
        let pairs = st.pairs_data();

        pairs.iter().for_each(|i| {
            assert_ne!(i.block_id, *id_1);
            assert_ne!(i.block_id, *id_4);
        });

        assert_eq!(pairs.len(), 1);
    }

    #[test]
    fn update_pairs_block_ids_test() {
        let st = ExchangePairsStorage::new();

        st.add_transaction(ExchangePairData {
            block_id: "id_1".into(),
            block_time_stamp: 0,
            amount_asset: "WAVES".into(),
            price_asset: "DG2xFkPdDwKUoBkzGAhQtLpSGzfXLiCYPEzeKH2Ad24p".into(),
            amount_asset_volume: Decimal::new_raw(0),
            price_asset_volume: Decimal::new_raw(0),
            tx_id: "tx_1".into(),
            height: 0,
        });
        st.add_transaction(ExchangePairData {
            block_id: "id_1".into(),
            block_time_stamp: 0,
            amount_asset: "WAVES".into(),
            price_asset: "DG2xFkPdDwKUoBkzGAhQtLpSGzfXLiCYPEzeKH2Ad24p".into(),
            amount_asset_volume: Decimal::new_raw(0),
            price_asset_volume: Decimal::new_raw(0),
            tx_id: "tx_2".into(),
            height: 0,
        });
        st.add_transaction(ExchangePairData {
            block_id: "id_3".into(),
            block_time_stamp: 0,
            amount_asset: "WAVES".into(),
            price_asset: "DG2xFkPdDwKUoBkzGAhQtLpSGzfXLiCYPEzeKH2Ad24p".into(),
            amount_asset_volume: Decimal::new_raw(0),
            price_asset_volume: Decimal::new_raw(0),
            tx_id: "tx_3".into(),
            height: 0,
        });
        st.add_transaction(ExchangePairData {
            block_id: "id_4".into(),
            block_time_stamp: 0,
            amount_asset: "WAVES".into(),
            price_asset: "DG2xFkPdDwKUoBkzGAhQtLpSGzfXLiCYPEzeKH2Ad24p".into(),
            amount_asset_volume: Decimal::new_raw(0),
            price_asset_volume: Decimal::new_raw(0),
            tx_id: "tx_4".into(),
            height: 0,
        });
        st.add_transaction(ExchangePairData {
            block_id: "id_1".into(),
            block_time_stamp: 0,
            amount_asset: "WAVES".into(),
            price_asset: "DG2xFkPdDwKUoBkzGAhQtLpSGzfXLiCYPEzeKH2Ad24p".into(),
            amount_asset_volume: Decimal::new_raw(0),
            price_asset_volume: Decimal::new_raw(0),
            tx_id: "tx_5".into(),
            height: 0,
        });
        st.add_transaction(ExchangePairData {
            block_id: "id_2".into(),
            block_time_stamp: 0,
            amount_asset: "WAVES".into(),
            price_asset: "DG2xFkPdDwKUoBkzGAhQtLpSGzfXLiCYPEzeKH2Ad24p".into(),
            amount_asset_volume: Decimal::new_raw(0),
            price_asset_volume: Decimal::new_raw(0),
            tx_id: "tx_6".into(),
            height: 0,
        });

        let to_upd = vec!["id_4".into(), "id_3".into()];
        st.update_pairs_block_ids_test("new_id", to_upd);

        let pairs_data = st.pairs_data();

        pairs_data.iter().for_each(|i| {
            assert!(i.block_id != "id_4");
            assert!(i.block_id != "id_3");

            assert!(i.block_id == "new_id" || i.block_id == "id_1" || i.block_id == "id_2");
        });

        assert_eq!(pairs_data.len(), 6);
    }

    #[test]
    fn cleanup_test() {
        let mut st = ExchangePairsStorage::new();

        st.push_block_rowlog("id_0", "", 80000000); // to del
        st.push_block_rowlog("id_1", "", 80000000); // to del
        st.push_block_rowlog("id_2", "", 86500000);
        st.push_block_rowlog("id_3", "", 86500000);
        st.push_block_rowlog("id_4", "", 86500000);
        st.push_block_rowlog("id_5", "", 80000000); //not del

        st.set_last_block_timestamp(86400000 * 2);

        st.cleanup();

        let binding = st.rowlog();
        let rowlog = binding.iter().map(|&(ref id, _)| id.as_str()).collect_vec();

        assert_eq!(rowlog, vec!["id_2", "id_3", "id_4", "id_5"]);
        assert_eq!(st.rowlog().len(), 4);
    }

    #[test]
    fn wrong_rollback_test() {
        let st = ExchangePairsStorage::new();

        st.push_block_rowlog("id_0", "", 80000000);
        st.push_block_rowlog("id_1", "", 80000000);
        st.push_block_rowlog("id_2", "", 86500000);
        st.push_block_rowlog("id_3", "", 86500000);
        st.push_block_rowlog("id_4", "", 86500000);
        st.push_block_rowlog("id_5", "", 80000000);
        st.push_block_rowlog("id_6", "", 80000000);
        st.push_block_rowlog("id_7", "", 0);
        st.push_block_rowlog("id_8", "", 0);
        st.push_block_rowlog("id_9", "", 0);

        st.rollback("invalid_id");

        let binding = st.rowlog();
        let rowlog = binding.iter().map(|&(ref id, _)| id.as_str()).collect_vec();

        assert_eq!(
            rowlog,
            vec!["id_0", "id_1", "id_2", "id_3", "id_4", "id_5", "id_6", "id_7", "id_8", "id_9"]
        );
        assert_eq!(st.rowlog().len(), 10);
    }
}
