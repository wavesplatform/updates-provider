use async_trait::async_trait;

use super::{DataFromBlock, Item, LastValue};
use crate::{
    db::repo_provider::ProviderRepo, error::Result, providers::watchlist::KeyPattern, waves,
};
use wavesexchange_topic::LeasingBalance;

impl DataFromBlock for LeasingBalance {
    fn data_from_block(block: &waves::BlockMicroblockAppend) -> Vec<(String, Self)> {
        block
            .leasing_balances
            .iter()
            .map(|lb| {
                let data = LeasingBalance {
                    address: lb.address.to_owned(),
                };
                let current_value = serde_json::to_string(lb).unwrap();
                (current_value, data)
            })
            .collect()
    }
}

#[async_trait]
impl<R: ProviderRepo + Sync> LastValue<R> for LeasingBalance {
    async fn last_value(self, repo: &R) -> Result<String> {
        Ok(
            if let Some(lb) = repo.last_leasing_balance(self.address).await? {
                let lb = waves::LeasingBalance::from(lb);
                serde_json::to_string(&lb)?
            } else {
                serde_json::to_string(&None::<waves::LeasingBalance>)?
            },
        )
    }
}

#[allow(clippy::unused_unit)]
impl KeyPattern for LeasingBalance {
    const PATTERNS_SUPPORTED: bool = false;
    type PatternMatcher = ();

    fn new_matcher(&self) -> Self::PatternMatcher {
        ()
    }
}

impl<R: ProviderRepo + Sync> Item<R> for LeasingBalance {}
