//! Asset info loading from asset-service

use crate::error::Error;
use std::{collections::HashMap, sync::RwLock, time::Duration};
use tokio::time::sleep;
use wavesexchange_apis::{
    assets::dto::{AssetInfo, OutputFormat},
    ApiResult, AssetsService, HttpClient,
};
use wavesexchange_log::{debug, error, warn};
use wx_topic::ExchangePair;

pub struct AssetStorage {
    asset_service: HttpClient<AssetsService>,
    asset_decimals: RwLock<HashMap<String, u8>>,
}

impl AssetStorage {
    pub fn new(assets_url: &str) -> Self {
        let asset_service = HttpClient::<AssetsService>::from_base_url(assets_url);

        AssetStorage {
            asset_service,
            asset_decimals: RwLock::new(HashMap::new()),
        }
    }

    pub async fn preload_decimals_for_pair(&self, pair: &ExchangePair) -> Result<(), Error> {
        if self.is_loaded(pair) {
            return Ok(());
        }

        let mut attempt = 0;
        let mut retry_delay = 30;
        while retry_delay < 600 {
            if attempt > 0 {
                warn!("Attempt #{} to load decimals for pair {:?}", attempt, pair);
            }
            let res = self.load(pair).await;
            if res.is_ok() {
                return Ok(());
            }
            if let Err(e) = res {
                debug!("asset-service error, sleeping {}s: {}", retry_delay, e);
                sleep(Duration::from_secs(retry_delay)).await;
                retry_delay *= 2;
                attempt += 1;
            }
        }

        Err(Error::AssetServiceClientError(format!(
            "failed to query decimals from asset-service for assets {} and {}",
            pair.amount_asset, pair.price_asset
        )))
    }

    fn is_loaded(&self, pair: &ExchangePair) -> bool {
        let assets = self.asset_decimals.read().unwrap();
        assets.contains_key(&pair.amount_asset) && assets.contains_key(&pair.price_asset)
    }

    async fn load(&self, pair: &ExchangePair) -> ApiResult<()> {
        let assets = [pair.amount_asset.as_str(), pair.price_asset.as_str()];

        let resp = self
            .asset_service
            .get(assets, None, OutputFormat::Full, false)
            .await?;

        let mut assets = self.asset_decimals.write().unwrap();

        for asset in resp.data {
            let Some(AssetInfo::Full(f)) = asset.data else {
                panic!("bad AssetInfo from asset-service: {:?}", asset);
            };
            assert!(
                f.precision >= 0 && f.precision <= 30,
                "probably bad precision value {} for asset {}",
                f.precision,
                f.id
            );
            assets.insert(f.id, f.precision as u8);
        }

        Ok(())
    }

    pub fn decimals_for_asset(&self, asset: &str) -> Option<u8> {
        let assets = self.asset_decimals.read().unwrap();

        let res = assets.get(asset).copied();
        if res.is_none() {
            error!("asset decimals not found for {}", asset);
        }
        res
    }
}
