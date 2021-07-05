use super::{DataFromBlock, Item, LastValue};
use crate::error::Result;
use crate::transactions::repo::TransactionsRepoPoolImpl;
use crate::transactions::LeasingBalance as LeasingBalanceDB;
use crate::transactions::{BlockMicroblockAppend, TransactionsRepo};
use async_trait::async_trait;
use std::sync::Arc;
use wavesexchange_topic::LeasingBalance;

impl DataFromBlock for LeasingBalance {
    fn data_from_block(block: &BlockMicroblockAppend) -> Vec<(String, Self)> {
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
impl LastValue for LeasingBalance {
    async fn get_last(self, repo: &Arc<TransactionsRepoPoolImpl>) -> Result<String> {
        Ok(
            if let Some(ilb) =
                tokio::task::block_in_place(move || repo.last_leasing_balance(self.address))?
            {
                let lb = LeasingBalanceDB::from(ilb);
                serde_json::to_string(&lb)?
            } else {
                serde_json::to_string(&None::<LeasingBalanceDB>)?
            },
        )
    }
}

impl Item for LeasingBalance {}
