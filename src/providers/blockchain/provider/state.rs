use super::{DataFromBlock, Item, LastValue};
use crate::transactions::repo::TransactionsRepoPoolImpl;
use crate::transactions::BlockMicroblockAppend;
use crate::{
    error::Result,
    transactions::{DataEntry, TransactionsRepo},
};
use async_trait::async_trait;
use std::sync::Arc;
use wavesexchange_topic::State;

impl DataFromBlock for State {
    fn data_from_block(block: &BlockMicroblockAppend) -> Vec<(String, Self)> {
        block
            .data_entries
            .iter()
            .map(|de| {
                let data = State {
                    address: de.address.to_owned(),
                    key: de.key.to_owned(),
                };
                let current_value = serde_json::to_string(de).unwrap();
                (current_value, data)
            })
            .collect()
    }
}

#[async_trait]
impl LastValue for State {
    async fn get_last(self, repo: &Arc<TransactionsRepoPoolImpl>) -> Result<String> {
        Ok(
            if let Some(ide) =
                tokio::task::block_in_place(move || repo.last_data_entry(self.address, self.key))?
            {
                let de = DataEntry::from(ide);
                serde_json::to_string(&de)?
            } else {
                serde_json::to_string(&None::<DataEntry>)?
            },
        )
    }
}

impl Item for State {}
