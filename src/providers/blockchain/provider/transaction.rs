use async_trait::async_trait;
use std::convert::TryFrom;
use wx_topic::{Transaction, TransactionByAddress, TransactionExchange, TransactionType};

use super::{BlockData, DataFromBlock, Item, LastValue};
use crate::db::repo_provider::ProviderRepo;
use crate::error::Result;
use crate::providers::watchlist::KeyPattern;
use crate::waves;

impl DataFromBlock for Transaction {
    fn data_from_block(block: &waves::BlockMicroblockAppend) -> Vec<BlockData<Transaction>> {
        block
            .transactions
            .iter()
            .flat_map(|tx_update| {
                let tx_id = tx_update.id.to_owned();
                let tx_type = tx_update.tx_type.into();
                let tx_addresses = tx_update.addresses.to_owned();
                let mut txs = Vec::with_capacity(1 + 2 * tx_addresses.len());
                if let waves::transactions::TransactionType::Exchange = tx_update.tx_type {
                    let exchange_data =
                        waves::transactions::exchange::ExchangeData::try_from(tx_update).unwrap();
                    let amount_asset = exchange_data
                        .order1
                        .asset_pair
                        .amount_asset
                        .as_ref()
                        .map(|x| x.to_owned())
                        .unwrap_or_else(|| "WAVES".to_string());
                    let price_asset = exchange_data
                        .order1
                        .asset_pair
                        .price_asset
                        .as_ref()
                        .map(|x| x.to_owned())
                        .unwrap_or_else(|| "WAVES".to_string());
                    let data = Transaction::Exchange(TransactionExchange {
                        amount_asset,
                        price_asset,
                    });
                    let current_value = serde_json::to_string(&exchange_data).unwrap();
                    txs.push(BlockData::new(current_value, data))
                }
                for address in tx_addresses {
                    let data = Transaction::ByAddress(TransactionByAddress {
                        address: address.0.clone(),
                        tx_type,
                    });
                    let current_value = tx_id.clone();
                    txs.push(BlockData::new(current_value, data));
                    let data = Transaction::ByAddress(TransactionByAddress {
                        address: address.0,
                        tx_type: TransactionType::All,
                    });
                    let current_value = tx_id.clone();
                    txs.push(BlockData::new(current_value, data));
                }
                txs
            })
            .collect()
    }

    fn data_from_rollback(_rollback: &waves::RollbackData) -> Vec<BlockData<Transaction>> {
        //TODO process somehow rollback.removed_transaction_ids (is it possible??)
        vec![]
    }
}

#[async_trait]
impl<R: ProviderRepo + Sync> LastValue<R> for Transaction {
    async fn last_value(self, repo: &R) -> Result<String> {
        Ok(match self {
            Transaction::ByAddress(TransactionByAddress {
                tx_type: TransactionType::All,
                address,
            }) => {
                if let Some(waves::transactions::Transaction { id, .. }) =
                    repo.last_transaction_by_address(address).await?
                {
                    id
                } else {
                    serde_json::to_string(&None::<String>)?
                }
            }
            Transaction::ByAddress(TransactionByAddress { tx_type, address }) => {
                let transaction_type = waves::transactions::TransactionType::try_from(tx_type)?;
                if let Some(waves::transactions::Transaction { id, .. }) = repo
                    .last_transaction_by_address_and_type(address, transaction_type)
                    .await?
                {
                    id
                } else {
                    serde_json::to_string(&None::<String>)?
                }
            }
            Transaction::Exchange(TransactionExchange {
                amount_asset,
                price_asset,
            }) => {
                if let Some(waves::transactions::Transaction {
                    body: Some(body_value),
                    ..
                }) = repo
                    .last_exchange_transaction(amount_asset, price_asset)
                    .await?
                {
                    serde_json::to_string(&body_value)?
                } else {
                    serde_json::to_string(&None::<String>)?
                }
            }
        })
    }

    async fn init_last_value(&self, _repo: &R) -> Result<bool> {
        Ok(false)
    }
}

#[allow(clippy::unused_unit)]
impl KeyPattern for Transaction {
    const PATTERNS_SUPPORTED: bool = false;
    type PatternMatcher = ();

    fn new_matcher(&self) -> Self::PatternMatcher {
        ()
    }
}

impl<R: ProviderRepo + Sync> Item<R> for Transaction {}
