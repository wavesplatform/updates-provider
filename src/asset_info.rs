//! Asset info loading from asset-service

use crate::error::Error;
use itertools::Itertools;
use std::{collections::HashMap, sync::RwLock, time::Duration};
use tokio::time::sleep;
use wavesexchange_apis::{
    assets::dto::{AssetInfo, OutputFormat},
    AssetsService, HttpClient,
};
use wavesexchange_log::{debug, error, warn};
use wx_topic::ExchangePair;
use AssetLoadError::*;

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

    pub async fn preload_decimals_for_pair(
        &self,
        pair: &ExchangePair,
    ) -> Result<(), AssetLoadError> {
        if self.is_loaded(pair) {
            return Ok(());
        }

        let mut attempt = 0;
        let mut retry_delay = 30;
        let mut last_error: Option<AssetLoadError> = None;

        while retry_delay < 600 {
            if attempt > 0 {
                warn!("Attempt #{} to load decimals for pair {:?}", attempt, pair);
            }

            match self.load(pair).await {
                Ok(..) => break,
                Err(e) => match &e {
                    AssetsNotFound(..) => {
                        last_error = Some(e);
                        break;
                    }
                    AssetServiceError { api_error, .. } => {
                        debug!(
                            "asset-service error, sleeping {}s: {:?}",
                            retry_delay, api_error
                        );
                        last_error = Some(e);
                        attempt += 1;
                        sleep(Duration::from_secs(retry_delay)).await;
                        retry_delay *= 2;
                    }
                },
            }
        }

        match last_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn is_loaded(&self, pair: &ExchangePair) -> bool {
        let assets = self.asset_decimals.read().unwrap();
        assets.contains_key(&pair.amount_asset) && assets.contains_key(&pair.price_asset)
    }

    async fn load(&self, pair: &ExchangePair) -> Result<(), AssetLoadError> {
        let pair_assets = [pair.amount_asset.as_str(), pair.price_asset.as_str()];

        let resp = self
            .asset_service
            .get(pair_assets, None, OutputFormat::Full, false)
            .await
            .map_err(|api_error| AssetServiceError {
                pair: pair.to_owned(),
                api_error,
            })?;

        let mut assets = self.asset_decimals.write().unwrap();

        let assets_not_found = resp
            .data
            .into_iter()
            .zip(pair_assets)
            .filter_map(|(asset, asset_id)| {
                let Some(AssetInfo::Full(f)) = asset.data else {
                    return Some(asset_id.to_owned());
                };
                assert!(
                    f.precision >= 0 && f.precision <= 30,
                    "probably bad precision value {} for asset {}",
                    f.precision,
                    f.id
                );
                assets.insert(f.id, f.precision as u8);
                None
            })
            .collect_vec();

        if assets_not_found.is_empty() {
            Ok(())
        } else {
            Err(AssetsNotFound(assets_not_found))
        }
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

#[derive(Debug, Clone, thiserror::Error)]
pub enum AssetLoadError {
    #[error("AssetsNotFound {0:?}")]
    AssetsNotFound(Vec<String>),
    #[error("AssetServiceError for pair {pair:?}, reason: {api_error:?}")]
    AssetServiceError {
        pair: ExchangePair,
        api_error: wavesexchange_apis::Error,
    },
}

impl From<AssetLoadError> for Error {
    fn from(err: AssetLoadError) -> Self {
        Error::AssetServiceClientError(match err {
            AssetsNotFound(ids) => {
                format!("requested assets not found in asset-service: {:?}", ids)
            }
            AssetServiceError { pair, api_error } => {
                format!(
                    "asset-service request failed for pair {:?} with {:?}",
                    pair, api_error,
                )
            }
        })
    }
}
