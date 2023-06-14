use async_trait::async_trait;

use super::{BlockData, DataFromBlock, LastValue};
use crate::{
    db::repo_provider::ProviderRepo, error::Result, providers::watchlist::KeyPattern, waves,
};
use wx_topic::LeasingBalance;

impl DataFromBlock for LeasingBalance {
    type Context = ();

    fn data_from_block(
        block: &waves::BlockMicroblockAppend,
        _ctx: &(),
    ) -> Vec<BlockData<LeasingBalance>> {
        extract_leasing_balances(&block.leasing_balances)
    }

    fn data_from_rollback(
        rollback: &waves::RollbackData,
        _ctx: &(),
    ) -> Vec<BlockData<LeasingBalance>> {
        extract_leasing_balances(&rollback.leasing_balances)
    }
}

fn extract_leasing_balances(
    leasing_balances: &[waves::LeasingBalance],
) -> Vec<BlockData<LeasingBalance>> {
    leasing_balances
        .iter()
        .map(|lb| {
            let data = LeasingBalance {
                address: lb.address.to_owned(),
            };
            let current_value = serde_json::to_string(lb).unwrap();
            BlockData::new(current_value, data)
        })
        .collect()
}

#[async_trait]
impl<R: ProviderRepo + Sync> LastValue<R> for LeasingBalance {
    type Context = ();

    async fn last_value(self, repo: &R, _ctx: &()) -> Result<String> {
        Ok(
            if let Some(lb) = repo.last_leasing_balance(self.address).await? {
                let lb = waves::LeasingBalance::from(lb);
                serde_json::to_string(&lb)?
            } else {
                serde_json::to_string(&None::<waves::LeasingBalance>)?
            },
        )
    }

    async fn init_last_value(&self, _repo: &R, _ctx: &()) -> Result<bool> {
        Ok(false)
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
