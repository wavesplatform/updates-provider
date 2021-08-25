use async_trait::async_trait;
use std::convert::TryFrom;
use std::sync::Arc;
use wavesexchange_topic::{TransactionByAddress, TransactionExchange, TransactionType as Type};

use super::{DataFromBlock, Item, LastValue};
use crate::db::repo::RepoImpl;
use crate::db::Repo;
use crate::error::Result;
use crate::providers::watchlist::KeyPattern;
use crate::waves::transactions::exchange::ExchangeData;
use crate::waves::transactions::{Transaction, TransactionType, TransactionUpdate};
use crate::waves::{Address, BlockMicroblockAppend};

impl DataFromBlock for wavesexchange_topic::Transaction {
    fn data_from_block(block: &BlockMicroblockAppend) -> Vec<(String, Self)> {
        block
            .transactions
            .iter()
            .flat_map(|tx_update| {
                let mut txs = vec![];
                if let TransactionType::Exchange = tx_update.tx_type {
                    let exchange_data = ExchangeData::try_from(tx_update).unwrap();
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
                    let data = wavesexchange_topic::Transaction::Exchange(TransactionExchange {
                        amount_asset,
                        price_asset,
                    });
                    let current_value = serde_json::to_string(&exchange_data).unwrap();
                    txs.push((current_value, data))
                }
                let tx: Tx = tx_update.into();
                for address in tx.addresses {
                    let data = wavesexchange_topic::Transaction::ByAddress(TransactionByAddress {
                        address: address.0.clone(),
                        tx_type: tx.tx_type.into(),
                    });
                    let current_value = tx.id.clone();
                    txs.push((current_value, data));
                    let data = wavesexchange_topic::Transaction::ByAddress(TransactionByAddress {
                        address: address.0,
                        tx_type: Type::All,
                    });
                    let current_value = tx.id.clone();
                    txs.push((current_value, data));
                }
                txs
            })
            .collect()
    }
}

#[async_trait]
impl LastValue for wavesexchange_topic::Transaction {
    async fn get_last(self, repo: &Arc<RepoImpl>) -> Result<String> {
        Ok(match self {
            wavesexchange_topic::Transaction::ByAddress(TransactionByAddress {
                tx_type: Type::All,
                address,
            }) => {
                if let Some(Transaction { id, .. }) =
                    tokio::task::block_in_place(move || repo.last_transaction_by_address(address))?
                {
                    id
                } else {
                    serde_json::to_string(&None::<String>)?
                }
            }
            wavesexchange_topic::Transaction::ByAddress(TransactionByAddress {
                tx_type,
                address,
            }) => {
                let transaction_type = TransactionType::try_from(tx_type)?;
                if let Some(Transaction { id, .. }) =
                    repo.last_transaction_by_address_and_type(address, transaction_type)?
                {
                    id
                } else {
                    serde_json::to_string(&None::<String>)?
                }
            }
            wavesexchange_topic::Transaction::Exchange(TransactionExchange {
                amount_asset,
                price_asset,
            }) => {
                if let Some(Transaction {
                    body: Some(body_value),
                    ..
                }) = tokio::task::block_in_place(move || {
                    repo.last_exchange_transaction(amount_asset, price_asset)
                })? {
                    serde_json::to_string(&body_value)?
                } else {
                    serde_json::to_string(&None::<String>)?
                }
            }
        })
    }
}

struct Tx {
    id: String,
    tx_type: TransactionType,
    addresses: Vec<Address>,
}

impl From<&TransactionUpdate> for Tx {
    fn from(value: &TransactionUpdate) -> Self {
        Self {
            id: value.id.to_owned(),
            tx_type: value.tx_type,
            addresses: value.addresses.to_owned(),
        }
    }
}

impl KeyPattern for wavesexchange_topic::Transaction {
    const PATTERNS_SUPPORTED: bool = false;
    type PatternMatcher = ();

    fn new_matcher(&self) -> Self::PatternMatcher {
        ()
    }
}

impl Item for wavesexchange_topic::Transaction {}
