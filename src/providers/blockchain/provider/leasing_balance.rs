use async_trait::async_trait;
use wavesexchange_topic::LeasingBalance;

use super::{DataFromBlock, Item, LastValue};
use crate::{db, error::Result, providers::watchlist::KeyPattern, waves};

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
impl<D: db::Repo + Sync> LastValue<D> for LeasingBalance {
    async fn get_last(self, repo: &D) -> Result<String> {
        Ok(
            if let Some(lb) =
                tokio::task::block_in_place(move || repo.last_leasing_balance(self.address))?
            {
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

impl<D: db::Repo + Sync> Item<D> for LeasingBalance {}
