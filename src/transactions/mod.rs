use crate::error::{Error, Result};
use crate::schema::{associated_addresses, blocks_microblocks, transactions};
use diesel::{
    deserialize::FromSql,
    serialize::{Output, ToSql},
    sql_types::*,
    Insertable, Queryable,
};
use std::convert::{TryFrom, TryInto};
use waves_protobuf_schemas::waves;
use waves_protobuf_schemas::waves::events::blockchain_updated::append::{
    BlockAppend, Body, MicroBlockAppend,
};
use waves_protobuf_schemas::waves::events::blockchain_updated::{Append, Update};
use waves_protobuf_schemas::waves::events::BlockchainUpdated;
use waves_protobuf_schemas::waves::transaction::Data;

pub mod exchange;
pub mod repo;

#[derive(Debug, Clone)]
pub struct Config {
    pub blockchain_updates_url: String,
    pub updates_per_request: usize,
    pub max_wait_time_in_secs: u64,
}

#[derive(Clone, Debug, Insertable, QueryableByName)]
#[table_name = "blocks_microblocks"]
pub struct BlockMicroblock {
    pub id: String,
    pub time_stamp: Option<i64>,
    pub height: i32,
}

#[derive(Clone, Debug, Insertable, QueryableByName, Queryable)]
#[table_name = "transactions"]
pub struct Transaction {
    pub id: String,
    pub block_uid: i64,
    pub tx_type: TransactionType,
    pub body: Option<serde_json::Value>,
    pub exchange_amount_asset: Option<String>,
    pub exchange_price_asset: Option<String>,
}

#[repr(i16)]
#[derive(Clone, Debug, Copy, AsExpression, FromSqlRow)]
#[sql_type = "SmallInt"]
pub enum TransactionType {
    Genesis = 1,
    Payment = 2,
    Issue = 3,
    Transfer = 4,
    Reissue = 5,
    Burn = 6,
    Exchange = 7,
    Lease = 8,
    LeaseCancel = 9,
    Alias = 10,
    MassTransfer = 11,
    Data = 12,
    SetScript = 13,
    Sponsorship = 14,
    SetAssetScript = 15,
    InvokeScript = 16,
    UpdateAssetInfo = 17,
}

impl TryFrom<i16> for TransactionType {
    type Error = Error;

    fn try_from(value: i16) -> core::result::Result<Self, Self::Error> {
        match value {
            1 => Ok(TransactionType::Genesis),
            2 => Ok(TransactionType::Payment),
            3 => Ok(TransactionType::Issue),
            4 => Ok(TransactionType::Transfer),
            5 => Ok(TransactionType::Reissue),
            6 => Ok(TransactionType::Burn),
            7 => Ok(TransactionType::Exchange),
            8 => Ok(TransactionType::Lease),
            9 => Ok(TransactionType::LeaseCancel),
            10 => Ok(TransactionType::Alias),
            11 => Ok(TransactionType::MassTransfer),
            12 => Ok(TransactionType::Data),
            13 => Ok(TransactionType::SetScript),
            14 => Ok(TransactionType::Sponsorship),
            15 => Ok(TransactionType::SetAssetScript),
            16 => Ok(TransactionType::InvokeScript),
            17 => Ok(TransactionType::UpdateAssetInfo),
            _ => Err(Error::InvalidTransactionType(
                "unknown transaction type".into(),
            )),
        }
    }
}

impl<DB> ToSql<SmallInt, DB> for TransactionType
where
    DB: diesel::backend::Backend,
    i16: ToSql<SmallInt, DB>,
{
    fn to_sql<W: std::io::Write>(&self, out: &mut Output<W, DB>) -> diesel::serialize::Result {
        (*self as i16).to_sql(out)
    }
}

impl<DB> FromSql<SmallInt, DB> for TransactionType
where
    DB: diesel::backend::Backend,
    i16: FromSql<SmallInt, DB>,
{
    fn from_sql(bytes: Option<&DB::RawValue>) -> diesel::deserialize::Result<Self> {
        Ok(i16::from_sql(bytes)?.try_into()?)
    }
}

impl From<&Data> for TransactionType {
    fn from(value: &Data) -> Self {
        match value {
            Data::Genesis(_) => TransactionType::Genesis,
            Data::Payment(_) => TransactionType::Payment,
            Data::Transfer(_) => TransactionType::Transfer,
            Data::Exchange(_) => TransactionType::Exchange,
            Data::Lease(_) => TransactionType::Lease,
            Data::MassTransfer(_) => TransactionType::MassTransfer,
            Data::InvokeScript(_) => TransactionType::InvokeScript,
            Data::Issue(_) => TransactionType::Issue,
            Data::Reissue(_) => TransactionType::Reissue,
            Data::Burn(_) => TransactionType::Burn,
            Data::LeaseCancel(_) => TransactionType::LeaseCancel,
            Data::CreateAlias(_) => TransactionType::Alias,
            Data::DataTransaction(_) => TransactionType::Data,
            Data::SetScript(_) => TransactionType::SetScript,
            Data::SponsorFee(_) => TransactionType::Sponsorship,
            Data::SetAssetScript(_) => TransactionType::SetAssetScript,
            Data::UpdateAssetInfo(_) => TransactionType::UpdateAssetInfo,
        }
    }
}

impl TryFrom<crate::models::Type> for TransactionType {
    type Error = Error;

    fn try_from(value: crate::models::Type) -> core::result::Result<Self, Self::Error> {
        match value {
            crate::models::Type::All => Err(Error::InvalidDBTransactionType(value.to_string())),
            crate::models::Type::Genesis => Ok(Self::Genesis),
            crate::models::Type::Payment => Ok(Self::Payment),
            crate::models::Type::Issue => Ok(Self::Issue),
            crate::models::Type::Transfer => Ok(Self::Transfer),
            crate::models::Type::Reissue => Ok(Self::Reissue),
            crate::models::Type::Burn => Ok(Self::Burn),
            crate::models::Type::Exchange => Ok(Self::Exchange),
            crate::models::Type::Lease => Ok(Self::Lease),
            crate::models::Type::LeaseCancel => Ok(Self::LeaseCancel),
            crate::models::Type::Alias => Ok(Self::Alias),
            crate::models::Type::MassTransfer => Ok(Self::MassTransfer),
            crate::models::Type::Data => Ok(Self::Data),
            crate::models::Type::SetScript => Ok(Self::SetScript),
            crate::models::Type::Sponsorship => Ok(Self::Sponsorship),
            crate::models::Type::SetAssetScript => Ok(Self::SetAssetScript),
            crate::models::Type::InvokeScript => Ok(Self::InvokeScript),
            crate::models::Type::UpdateAssetInfo => Ok(Self::UpdateAssetInfo),
        }
    }
}

#[derive(Clone, Debug, Insertable, QueryableByName, PartialEq, Eq, Hash)]
#[table_name = "associated_addresses"]
pub struct AssociatedAddress {
    pub address: String,
    pub transaction_id: String,
}

#[derive(Clone, Debug)]
pub struct BlockMicroblockAppend {
    id: String,
    time_stamp: Option<i64>,
    pub height: i32,
    pub transactions: Vec<TransactionUpdate>,
}

#[derive(Debug, Clone)]
pub struct TransactionUpdate {
    pub id: String,
    pub tx_type: TransactionType,
    pub block_uid: String,
    pub addresses: Vec<Address>,
    pub data: Data,
    pub sender: Address,
    pub sender_public_key: String,
    pub fee: Option<Amount>,
    pub timestamp: i64,
    pub version: i32,
    pub proofs: Vec<String>,
    pub height: i32,
}

#[derive(Debug, Clone)]
pub struct Amount {
    pub asset_id: String,
    pub amount: i64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Address(pub String);

#[derive(Clone, Debug)]
pub enum BlockchainUpdate {
    Block(BlockMicroblockAppend),
    Microblock(BlockMicroblockAppend),
    Rollback(String),
}

#[derive(Debug)]
pub struct BlockchainUpdatesWithLastHeight {
    pub last_height: u32,
    pub updates: Vec<BlockchainUpdate>,
}

#[derive(Debug, Queryable)]
pub struct PrevHandledHeight {
    pub uid: i64,
    pub height: i32,
}

pub trait TransactionsRepo {
    fn transaction(&self, f: impl FnOnce() -> Result<()>) -> Result<()>;

    fn get_prev_handled_height(&self) -> Result<Option<PrevHandledHeight>>;

    fn get_block_uid(&self, block_id: &str) -> Result<i64>;

    fn get_key_block_uid(&self) -> Result<i64>;

    fn get_total_block_id(&self) -> Result<Option<String>>;

    // fn get_next_update_uid(&self) -> Result<i64>;

    fn insert_blocks_or_microblocks(&self, blocks: &Vec<BlockMicroblock>) -> Result<Vec<i64>>;

    fn insert_transactions(&self, transactions: &Vec<Transaction>) -> Result<()>;

    fn insert_associated_addresses(
        &self,
        associated_addresses: &Vec<AssociatedAddress>,
    ) -> Result<()>;

    // fn insert_data_entries(&self, entries: &Vec<InsertableDataEntry>) -> Result<()>;

    // fn close_superseded_by(&self, updates: &Vec<DataEntryUpdate>) -> Result<()>;

    // fn reopen_superseded_by(&self, current_superseded_by: &Vec<i64>) -> Result<()>;

    // fn set_next_update_uid(&self, uid: i64) -> Result<()>;

    fn change_block_id(&self, block_uid: &i64, new_block_id: &str) -> Result<()>;

    fn update_transactions_block_references(&self, block_uid: &i64) -> Result<()>;

    fn delete_microblocks(&self) -> Result<()>;

    fn rollback_blocks_microblocks(&self, block_uid: &i64) -> Result<()>;

    // fn rollback_data_entries(&self, block_uid: &i64) -> Result<Vec<DeletedDataEntry>>;

    fn last_transaction_by_address(&self, address: String) -> Result<Option<Transaction>>;

    fn last_transaction_by_address_and_type(
        &self,
        address: String,
        transaction_type: TransactionType,
    ) -> Result<Option<Transaction>>;

    fn last_exchange_transaction(
        &self,
        amount_asset: String,
        price_asset: String,
    ) -> Result<Option<Transaction>>;
}

impl TryFrom<std::sync::Arc<BlockchainUpdated>> for BlockchainUpdate {
    type Error = Error;

    fn try_from(value: std::sync::Arc<BlockchainUpdated>) -> Result<Self> {
        match value.update.as_ref() {
            Some(Update::Append(Append {
                body,
                transaction_ids,
                ..
            })) => {
                let height = value.height;

                match body {
                    Some(Body::Block(BlockAppend { block, .. })) => {
                        let block_uid = bs58::encode(&value.id).into_string();
                        let raw_transactions = &block.as_ref().unwrap().transactions;
                        let transactions = parse_transactions(
                            block_uid.clone(),
                            height,
                            raw_transactions,
                            &transaction_ids,
                        );
                        Ok(BlockchainUpdate::Block(BlockMicroblockAppend {
                            id: block_uid,
                            time_stamp: block
                                .as_ref()
                                .map(|b| {
                                    b.header.as_ref().map(|h| Some(h.timestamp)).unwrap_or(None)
                                })
                                .unwrap_or(None),
                            height,
                            transactions,
                        }))
                    }
                    Some(Body::MicroBlock(MicroBlockAppend { micro_block, .. })) => {
                        let block_uid = bs58::encode(&micro_block.as_ref().unwrap().total_block_id)
                            .into_string();
                        let raw_transactions = &micro_block
                            .as_ref()
                            .unwrap()
                            .micro_block
                            .as_ref()
                            .unwrap()
                            .transactions;
                        let transactions = parse_transactions(
                            block_uid.clone(),
                            height,
                            raw_transactions,
                            &transaction_ids,
                        );
                        Ok(BlockchainUpdate::Microblock(BlockMicroblockAppend {
                            id: block_uid,
                            time_stamp: None,
                            height,
                            transactions,
                        }))
                    }
                    _ => Err(Error::GRPCBodyError("Append body is empty.".to_string())),
                }
            }
            Some(Update::Rollback(_)) => Ok(BlockchainUpdate::Rollback(
                bs58::encode(&value.id).into_string(),
            )),
            _ => Err(Error::GRPCBodyError(
                "Unknown blockchain update.".to_string(),
            )),
        }
    }
}

fn encode_asset(asset: &Vec<u8>) -> String {
    if asset.len() > 0 {
        bs58::encode(asset).into_string()
    } else {
        "WAVES".to_string()
    }
}

impl From<&BlockMicroblockAppend> for BlockMicroblock {
    fn from(value: &BlockMicroblockAppend) -> Self {
        Self {
            id: value.id.clone(),
            height: value.height,
            time_stamp: value.time_stamp,
        }
    }
}

impl TryFrom<(i64, &TransactionUpdate)> for Transaction {
    type Error = Error;

    fn try_from(value: (i64, &TransactionUpdate)) -> Result<Self> {
        Ok(Self {
            id: value.1.id.clone(),
            tx_type: value.1.tx_type,
            block_uid: value.0,
            exchange_amount_asset: exchange_amount_asset_from_update(value.1),
            exchange_price_asset: exchange_price_asset_from_update(value.1),
            body: body_from_transaction_update(value.1)?,
        })
    }
}

fn body_from_transaction_update(update: &TransactionUpdate) -> Result<Option<serde_json::Value>> {
    let body = match update.tx_type {
        TransactionType::Exchange => {
            let data = exchange::ExchangeData::try_from(update)?;
            Some(serde_json::to_value(data)?)
        }
        _ => None,
    };
    Ok(body)
}

fn exchange_amount_asset_from_update(update: &TransactionUpdate) -> Option<String> {
    match &update.data {
        Data::Exchange(exchange_data) => {
            let amount_asset = &exchange_data
                .orders
                .get(0)
                .unwrap()
                .asset_pair
                .as_ref()
                .unwrap()
                .amount_asset_id;
            Some(encode_asset(amount_asset))
        }
        _ => None,
    }
}

fn exchange_price_asset_from_update(update: &TransactionUpdate) -> Option<String> {
    match &update.data {
        Data::Exchange(exchange_data) => {
            let price_asset = &exchange_data
                .orders
                .get(0)
                .unwrap()
                .asset_pair
                .as_ref()
                .unwrap()
                .price_asset_id;
            Some(encode_asset(price_asset))
        }
        _ => None,
    }
}

impl From<(String, &Address)> for AssociatedAddress {
    fn from(value: (String, &Address)) -> Self {
        Self {
            address: value.1 .0.clone(),
            transaction_id: value.0,
        }
    }
}

fn parse_transactions(
    block_uid: String,
    height: i32,
    raw_transactions: &Vec<waves::SignedTransaction>,
    transaction_ids: &Vec<Vec<u8>>,
) -> Vec<TransactionUpdate> {
    transaction_ids
        .iter()
        .zip(raw_transactions.iter())
        .map(|(tx_id, tx)| {
            let transaction = tx.transaction.as_ref().unwrap();
            let sender_public_key = &transaction.sender_public_key;
            let mut addresses = if sender_public_key.len() == 0 {
                vec![]
            } else {
                let sender = address_from_public_key(sender_public_key, transaction.chain_id as u8);
                vec![sender]
            };
            let data = transaction.data.clone().unwrap();
            maybe_add_addresses(&data, transaction.chain_id as u8, &mut addresses);
            let tx_type = TransactionType::from(&data);
            let tx = TransactionUpdate {
                tx_type,
                data,
                height,
                block_uid: block_uid.clone(),
                id: bs58::encode(tx_id).into_string(),
                addresses,
                sender: address_from_public_key(
                    &transaction.sender_public_key,
                    transaction.chain_id as u8,
                ),
                sender_public_key: bs58::encode(&transaction.sender_public_key).into_string(),
                fee: transaction.fee.as_ref().map(Into::into),
                timestamp: transaction.timestamp,
                version: transaction.version,
                proofs: tx
                    .proofs
                    .iter()
                    .map(|proof| bs58::encode(proof).into_string())
                    .collect(),
            };
            tx
        })
        .collect()
}

impl From<&waves_protobuf_schemas::waves::Amount> for Amount {
    fn from(value: &waves_protobuf_schemas::waves::Amount) -> Self {
        Self {
            asset_id: bs58::encode(&value.asset_id).into_string(),
            amount: value.amount,
        }
    }
}

pub fn address_from_public_key(pk: &Vec<u8>, chain_id: u8) -> Address {
    let pkh = &keccak256(&blake2b256(&pk))[..20];
    address_from_public_key_hash(&pkh.to_vec(), chain_id)
}

fn address_from_public_key_hash(pkh: &Vec<u8>, chain_id: u8) -> Address {
    let mut addr = [0u8; 26];
    addr[0] = 1;
    addr[1] = chain_id;
    for i in 0..20 {
        addr[i + 2] = pkh[i];
    }
    let chks = &keccak256(&blake2b256(&addr[..22]))[..4];
    for (i, v) in chks.into_iter().enumerate() {
        addr[i + 22] = *v
    }
    Address(bs58::encode(addr).into_string())
}

#[test]
fn address_from_public_key_test() {
    let pk = vec![
        169, 213, 159, 238, 197, 81, 67, 140, 199, 67, 126, 57, 205, 117, 50, 139, 192, 195, 69,
        191, 200, 252, 145, 136, 67, 194, 84, 135, 114, 186, 38, 64,
    ];
    let address = address_from_public_key(&pk, 84);
    assert_eq!(address.0, "3NBVqYXrapgJP9atQccdBPAgJPwHDKkh6A8".to_string());
}

use sha3::Digest;

fn keccak256(message: &[u8]) -> [u8; 32] {
    let mut hasher = sha3::Keccak256::new();
    hasher.input(message);
    hasher.result().into()
}

pub fn blake2b256(message: &[u8]) -> [u8; 32] {
    use blake2::digest::{Input, VariableOutput};
    let mut hasher = blake2::VarBlake2b::new(32).unwrap();
    hasher.input(message);
    let mut arr = [0u8; 32];
    hasher.variable_result(|res| arr = res.try_into().unwrap());
    arr
}

fn maybe_add_addresses(data: &Data, chain_id: u8, addresses: &mut Vec<Address>) {
    match data {
        Data::Genesis(data) => {
            let address = Address(bs58::encode(data.recipient_address.clone()).into_string());
            addresses.push(address);
        }
        Data::Payment(data) => {
            let address = Address(bs58::encode(data.recipient_address.clone()).into_string());
            addresses.push(address);
        }
        Data::Transfer(data) => {
            maybe_address_from_recipient(&data.recipient, chain_id, addresses);
        }
        Data::Lease(data) => {
            maybe_address_from_recipient(&data.recipient, chain_id, addresses);
        }
        Data::InvokeScript(data) => {
            maybe_address_from_recipient(&data.d_app, chain_id, addresses);
        }
        Data::MassTransfer(data) => {
            for transfer in data.transfers.iter() {
                maybe_address_from_recipient(&transfer.recipient, chain_id, addresses);
            }
        }
        Data::Exchange(data) => {
            for waves::Order {
                sender_public_key, ..
            } in data.orders.iter()
            {
                let address = address_from_public_key(sender_public_key, chain_id);
                addresses.push(address);
            }
        }
        Data::Issue(_data) => (),
        Data::Reissue(_data) => (),
        Data::Burn(_data) => (),
        Data::LeaseCancel(_data) => (),
        Data::CreateAlias(_data) => (),
        Data::DataTransaction(_data) => (),
        Data::SetScript(_data) => (),
        Data::SponsorFee(_data) => (),
        Data::SetAssetScript(_data) => (),
        Data::UpdateAssetInfo(_data) => (),
    }
}

fn maybe_address_from_recipient(
    recipient: &Option<waves::Recipient>,
    chain_id: u8,
    addresses: &mut Vec<Address>,
) {
    if let Some(waves::Recipient {
        recipient: Some(waves::recipient::Recipient::PublicKeyHash(pkh)),
    }) = recipient
    {
        let address = address_from_public_key_hash(pkh, chain_id);
        addresses.push(address)
    }
}
