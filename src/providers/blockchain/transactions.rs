use super::super::watchlist::{WatchList, WatchListItem};
use super::super::{TSResourcesRepoImpl, TSUpdatesProviderLastValues};
use crate::error::Error;
use crate::models::{Topic, TransactionByAddress, TransactionType};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use waves_protobuf_schemas::waves::events::blockchain_updated::{
    append::Body, Append, Rollback, Update,
};
use waves_protobuf_schemas::waves::events::BlockchainUpdated;
// use waves_protobuf_schemas::waves::node::grpc::blocks_api_client::BlocksApiClient;
use waves_protobuf_schemas::waves::transaction;
use waves_protobuf_schemas::waves::{
    self, Block, MicroBlock, Recipient, SignedMicroBlock, SignedTransaction, Transaction,
};

pub struct Provider {
    watchlist: WatchList<TransactionByAddress>,
    rx: mpsc::Receiver<Arc<BlockchainUpdated>>,
    resources_repo: TSResourcesRepoImpl,
    last_values: TSUpdatesProviderLastValues,
}

impl Provider {
    pub fn new(
        resources_repo: TSResourcesRepoImpl,
        delete_timeout: std::time::Duration,
    ) -> (mpsc::Sender<Arc<BlockchainUpdated>>, Self) {
        let last_values = Arc::new(RwLock::new(HashMap::new()));
        let watchlist = WatchList::new(resources_repo.clone(), last_values.clone(), delete_timeout);
        let (tx, rx) = mpsc::channel(20);
        let provider = Self {
            watchlist,
            rx,
            resources_repo,
            last_values,
        };
        (tx, provider)
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        while let Some(update) = self.rx.recv().await {
            match &update.update {
                Some(Update::Append(Append {
                    body: Some(body),
                    transaction_ids,
                    ..
                })) => match body {
                    Body::Block(block) => {
                        if let Some(Block { transactions, .. }) = block.block.as_ref() {
                            self.parse_transactions(transactions, transaction_ids)
                                .await?
                        }
                    }
                    Body::MicroBlock(micro_block) => {
                        if let Some(SignedMicroBlock {
                            micro_block: Some(MicroBlock { transactions, .. }),
                            ..
                        }) = micro_block.micro_block.as_ref()
                        {
                            self.parse_transactions(transactions, transaction_ids)
                                .await?
                        }
                    }
                },
                Some(Update::Rollback(rollback)) => println!("rollback: {:?}", rollback),
                _ => (),
            }
        }

        Ok(())
    }

    async fn parse_transactions(
        &mut self,
        transactions: &Vec<SignedTransaction>,
        transaction_ids: &Vec<Vec<u8>>,
    ) -> Result<(), Error> {
        for (tx_id, tx) in transaction_ids.iter().zip(transactions.iter()) {
            if let Some(ref transaction) = tx.transaction {
                let sender_public_key = &transaction.sender_public_key;
                let sender = address_from_public_key(sender_public_key, transaction.chain_id as u8);
                if let Some(data) = &transaction.data {
                    let tx_type = TransactionType::from(data);
                    let mut tx = Tx {
                        sender,
                        tx_type,
                        id: bs58::encode(tx_id).into_string(),
                        recipients: vec![],
                    };
                    maybe_add_recipients(data, transaction.chain_id, &mut tx.recipients);
                    self.check_transaction(tx).await?;
                }
            }
        }

        Ok(())
    }

    async fn check_transaction(&mut self, tx: Tx) -> Result<(), Error> {
        let data = TransactionByAddress {
            address: tx.sender.0.clone(),
            tx_type: tx.tx_type.clone(),
        };
        self.inner_check_transaction(data, tx.id.clone()).await?;
        let data = TransactionByAddress {
            address: tx.sender.0,
            tx_type: TransactionType::All,
        };
        self.inner_check_transaction(data, tx.id.clone()).await?;
        for address in tx.recipients {
            let data = TransactionByAddress {
                address: address.0.clone(),
                tx_type: tx.tx_type.clone(),
            };
            self.inner_check_transaction(data, tx.id.clone()).await?;
            let data = TransactionByAddress {
                address: address.0,
                tx_type: TransactionType::All,
            };
            self.inner_check_transaction(data, tx.id.clone()).await?;
        }

        Ok(())
    }

    async fn inner_check_transaction(
        &mut self,
        data: TransactionByAddress,
        current_value: String,
    ) -> Result<(), Error> {
        if self.watchlist.items.contains_key(&data) {
            super::super::watchlist_process(
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

struct Tx {
    id: String,
    sender: Address,
    tx_type: TransactionType,
    recipients: Vec<Address>,
}

struct Address(String);

fn address_from_public_key(pk: &Vec<u8>, chain_id: u8) -> Address {
    let pkh = &keccak256(&blake2b256(&pk))[..20];
    address_from_public_key_hash(&pkh.to_vec(), chain_id)
}

fn address_from_public_key_hash(pkh: &Vec<u8>, chain_id: u8) -> Address {
    let mut addr = [0u8, 26];
    addr[0] = 1;
    addr[1] = chain_id;
    for i in 0..20 {
        addr[i + 2] = pkh[i]
    }
    let chks = &keccak256(&blake2b256(&addr[..22]))[..4];
    for (i, v) in chks.into_iter().enumerate() {
        addr[i + 23] = *v
    }
    Address(bs58::encode(addr).into_string())
}

use sha3::Digest;

fn keccak256(message: &[u8]) -> [u8; 32] {
    let mut hasher = sha3::Keccak256::new();
    hasher.input(message);
    hasher.result().into()
}

fn blake2b256(message: &[u8]) -> [u8; 32] {
    use blake2::digest::{Input, VariableOutput};
    use std::convert::TryInto;
    let mut hasher = blake2::VarBlake2b::new(32).unwrap();
    hasher.input(message);
    let mut arr = [0u8; 32];
    hasher.variable_result(|res| arr = res.try_into().unwrap());
    arr
}

fn maybe_add_recipients(data: &transaction::Data, chain_id: i32, recipients: &mut Vec<Address>) {
    match data {
        transaction::Data::Genesis(data) => {
            let address = Address(bs58::encode(data.recipient_address.clone()).into_string());
            recipients.push(address);
        }
        transaction::Data::Payment(data) => {
            let address = Address(bs58::encode(data.recipient_address.clone()).into_string());
            recipients.push(address);
        }
        transaction::Data::Transfer(data) => {
            maybe_address_from_recipient(&data.recipient, chain_id, recipients);
        }
        transaction::Data::Lease(data) => {
            maybe_address_from_recipient(&data.recipient, chain_id, recipients);
        }
        transaction::Data::InvokeScript(data) => {
            maybe_address_from_recipient(&data.d_app, chain_id, recipients);
        }
        transaction::Data::MassTransfer(data) => {
            for transfer in data.transfers.iter() {
                maybe_address_from_recipient(&transfer.recipient, chain_id, recipients);
            }
        }
        // TODO:
        transaction::Data::Exchange(data) => {}

        transaction::Data::Issue(_data) => (),
        transaction::Data::Reissue(_data) => (),
        transaction::Data::Burn(_data) => (),
        transaction::Data::LeaseCancel(_data) => (),
        transaction::Data::CreateAlias(_data) => (),
        transaction::Data::DataTransaction(_data) => (),
        transaction::Data::SetScript(_data) => (),
        transaction::Data::SponsorFee(_data) => (),
        transaction::Data::SetAssetScript(_data) => (),
        transaction::Data::UpdateAssetInfo(_data) => (),
    }
}

fn maybe_address_from_recipient(
    recipient: &Option<Recipient>,
    chain_id: i32,
    recipients: &mut Vec<Address>,
) {
    if let Some(Recipient {
        recipient: Some(waves::recipient::Recipient::PublicKeyHash(pkh)),
    }) = recipient
    {
        let address = address_from_public_key_hash(pkh, chain_id as u8);
        recipients.push(address)
    }
}
