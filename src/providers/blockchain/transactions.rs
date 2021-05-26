use super::super::watchlist::{WatchList, WatchListUpdate};
use super::super::{TSResourcesRepoImpl, TSUpdatesProviderLastValues, UpdatesProvider};
use crate::transactions::repo::TransactionsRepoPoolImpl;
use crate::transactions::{BlockMicroblockAppend, BlockchainUpdate, Transaction, TransactionType};
use crate::utils::clean_timeout;
use crate::{
    error::Result,
    transactions::{exchange::ExchangeData, Address, TransactionUpdate, TransactionsRepo},
};
use crate::{
    models::{self, Topic, TransactionByAddress, TransactionExchange, Type},
    resources::ResourcesRepo,
};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, convert::TryFrom};
use tokio::sync::{mpsc, RwLock};
use wavesexchange_log::{error, info};

pub struct Provider {
    watchlist: Arc<RwLock<WatchList<models::Transaction>>>,
    resources_repo: TSResourcesRepoImpl,
    last_values: TSUpdatesProviderLastValues<models::Transaction>,
    rx: mpsc::Receiver<Arc<Vec<BlockchainUpdate>>>,
    transactions_repo: Arc<TransactionsRepoPoolImpl>,
    clean_timeout: Duration,
}

impl Provider {
    pub fn new(
        resources_repo: TSResourcesRepoImpl,
        delete_timeout: Duration,
        transactions_repo: Arc<TransactionsRepoPoolImpl>,
        rx: mpsc::Receiver<Arc<Vec<BlockchainUpdate>>>,
    ) -> Self {
        let last_values = Arc::new(RwLock::new(HashMap::new()));
        let watchlist = Arc::new(RwLock::new(WatchList::new(
            resources_repo.clone(),
            last_values.clone(),
            delete_timeout,
        )));
        let clean_timeout = clean_timeout(delete_timeout);
        Self {
            watchlist,
            resources_repo,
            last_values,
            rx,
            transactions_repo,
            clean_timeout,
        }
    }

    async fn run(&mut self) -> Result<()> {
        let mut interval = tokio::time::interval(self.clean_timeout);
        loop {
            tokio::select! {
                msg = self.rx.recv() => {
                    if let Some(blockchain_updates) = msg {
                        self.process_updates(blockchain_updates).await?;
                        continue;
                    }
                    break;
                }
                _ = interval.tick() => {
                    let mut watchlist_lock = self.watchlist.write().await;
                    watchlist_lock.delete_old().await;
                }
            }
        }
        Ok(())
    }

    async fn process_updates(
        &mut self,
        blockchain_updates: Arc<Vec<BlockchainUpdate>>,
    ) -> Result<()> {
        for blockchain_update in blockchain_updates.iter() {
            match blockchain_update {
                BlockchainUpdate::Block(BlockMicroblockAppend { transactions, .. }) => {
                    self.check_transactions(transactions).await?
                }
                BlockchainUpdate::Microblock(BlockMicroblockAppend { transactions, .. }) => {
                    self.check_transactions(transactions).await?
                }
                BlockchainUpdate::Rollback(_) => (),
            }
        }

        Ok(())
    }

    async fn check_transactions(&mut self, tx_updates: &[TransactionUpdate]) -> Result<()> {
        for tx_update in tx_updates.iter() {
            if let TransactionType::Exchange = tx_update.tx_type {
                let exchange_data = ExchangeData::try_from(tx_update)?;
                self.check_transaction_exchange(exchange_data).await?
            }
            let tx = tx_update.into();
            self.check_transaction_by_address(tx).await?
        }

        Ok(())
    }

    async fn check_transaction_by_address(&mut self, tx: Tx) -> Result<()> {
        for address in tx.addresses {
            let data = TransactionByAddress {
                address: address.0.clone(),
                tx_type: tx.tx_type.into(),
            };
            self.check_in_watchlist(models::Transaction::ByAddress(data), tx.id.clone())
                .await?;
            let data = TransactionByAddress {
                address: address.0,
                tx_type: Type::All,
            };
            self.check_in_watchlist(models::Transaction::ByAddress(data), tx.id.clone())
                .await?;
        }

        Ok(())
    }

    async fn check_transaction_exchange(&mut self, exchange_data: ExchangeData) -> Result<()> {
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
        let data = models::Transaction::Exchange(TransactionExchange {
            amount_asset,
            price_asset,
        });
        let current_value = serde_json::to_string(&exchange_data)?;
        self.check_in_watchlist(data, current_value).await?;
        Ok(())
    }

    async fn check_in_watchlist(
        &mut self,
        data: models::Transaction,
        current_value: String,
    ) -> Result<()> {
        if self.watchlist.read().await.contains_key(&data) {
            Self::watchlist_process(
                &data,
                current_value,
                &self.resources_repo,
                &self.last_values,
            )
            .await
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl UpdatesProvider<models::Transaction> for Provider {
    async fn fetch_updates(
        mut self,
    ) -> Result<mpsc::UnboundedSender<WatchListUpdate<models::Transaction>>> {
        let (subscriptions_updates_sender, mut subscriptions_updates_receiver) =
            mpsc::unbounded_channel::<WatchListUpdate<models::Transaction>>();

        let watchlist = self.watchlist.clone();
        let resources_repo = self.resources_repo.clone();
        let transactions_repo = self.transactions_repo.clone();
        tokio::task::spawn(async move {
            info!("starting transactions subscriptions updates handler");
            while let Some(upd) = subscriptions_updates_receiver.recv().await {
                if let Err(err) = watchlist.write().await.on_update(&upd) {
                    error!("error while updating watchlist: {:?}", err);
                }
                if let WatchListUpdate::New { item, .. } = upd {
                    if let Err(err) =
                        check_and_maybe_insert(&resources_repo, &transactions_repo, item).await
                    {
                        error!("error while updating value: {:?}", err);
                    }
                }
            }
        });

        tokio::task::spawn(async move {
            info!("starting transactions provider");
            if let Err(error) = self.run().await {
                error!("transaction provider return error: {:?}", error);
            }
        });

        Ok(subscriptions_updates_sender)
    }

    async fn watchlist_process(
        data: &models::Transaction,
        current_value: String,
        resources_repo: &TSResourcesRepoImpl,
        last_values: &TSUpdatesProviderLastValues<models::Transaction>,
    ) -> Result<()> {
        let resource: Topic = data.clone().into();
        info!("insert new value {:?}", resource);
        last_values
            .write()
            .await
            .insert(data.to_owned(), current_value.clone());
        resources_repo.set(resource.clone(), current_value.clone())?;
        resources_repo.push(resource, current_value)?;
        Ok(())
    }
}

async fn check_and_maybe_insert(
    resources_repo: &TSResourcesRepoImpl,
    transactions_repo: &Arc<TransactionsRepoPoolImpl>,
    value: models::Transaction,
) -> Result<()> {
    let topic = value.clone().into();
    if resources_repo.get(&topic)?.is_none() {
        let new_value = match value {
            models::Transaction::ByAddress(TransactionByAddress {
                tx_type: Type::All,
                address,
            }) => {
                if let Some(Transaction { id, .. }) =
                    transactions_repo.last_transaction_by_address(address)?
                {
                    Some(id)
                } else {
                    None
                }
            }
            models::Transaction::ByAddress(TransactionByAddress { tx_type, address }) => {
                let transaction_type = TransactionType::try_from(tx_type)?;
                if let Some(Transaction { id, .. }) = transactions_repo
                    .last_transaction_by_address_and_type(address, transaction_type)?
                {
                    Some(id)
                } else {
                    None
                }
            }
            models::Transaction::Exchange(TransactionExchange {
                amount_asset,
                price_asset,
            }) => {
                if let Some(Transaction {
                    body: Some(body_value),
                    ..
                }) = transactions_repo.last_exchange_transaction(amount_asset, price_asset)?
                {
                    Some(serde_json::to_string(&body_value)?)
                } else {
                    None
                }
            }
        };
        let encoded_value = match new_value {
            None => serde_json::to_string(&new_value)?,
            Some(v) => v,
        };
        resources_repo.set(topic.clone(), encoded_value.clone())?;
        resources_repo.push(topic, encoded_value)?;
    }

    Ok(())
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
