use crate::error::{Error, Result};
use crate::schema::{associated_addresses, blocks_microblocks, data_entries, transactions};
use diesel::{
    deserialize::FromSql,
    serialize::{Output, ToSql},
    sql_types::*,
    Insertable, Queryable,
};
use serde::Serialize;
use std::convert::{TryFrom, TryInto};
use std::hash::{Hash, Hasher};
use waves_protobuf_schemas::waves;
use waves_protobuf_schemas::waves::data_transaction_data::data_entry::Value;
use waves_protobuf_schemas::waves::events::blockchain_updated::append::{
    BlockAppend, Body, MicroBlockAppend,
};
use waves_protobuf_schemas::waves::events::blockchain_updated::{Append, Update};
use waves_protobuf_schemas::waves::events::BlockchainUpdated;
use waves_protobuf_schemas::waves::transaction::Data;

pub const FRAGMENT_SEPARATOR: &str = "__";
pub const STRING_DESCRIPTOR: &str = "s";
pub const INTEGER_DESCRIPTOR: &str = "d";

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
    pub data_entries: Vec<DataEntry>,
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

#[derive(Debug, Clone, Serialize)]
pub struct Address(pub String);

#[derive(Debug, Clone, Serialize)]
pub struct DataEntry {
    pub address: String,
    pub key: String,
    pub transaction_id: String,
    pub value: ValueDataEntry,
    pub fragments: Fragments,
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum ValueDataEntry {
    Binary(Vec<u8>),
    Bool(bool),
    Integer(i64),
    String(String),
}

#[derive(Debug, Clone, Serialize)]
pub struct Fragments {
    pub key: Vec<DataEntryFragment>,
    pub value: Vec<DataEntryFragment>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DataEntryFragment {
    String { value: String },
    Integer { value: i64 },
}

#[derive(Clone, Debug, Insertable)]
#[table_name = "data_entries"]
pub struct DataEntryUpdate {
    pub superseded_by: i64,
    pub address: String,
    pub key: String,
}

#[derive(Clone, Debug)]
pub struct DeletedDataEntry {
    pub uid: i64,
    pub address: String,
    pub key: String,
}

impl PartialEq for DeletedDataEntry {
    fn eq(&self, other: &DeletedDataEntry) -> bool {
        (&self.address, &self.key) == (&other.address, &other.key)
    }
}

impl Eq for DeletedDataEntry {}

impl Hash for DeletedDataEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.address.hash(state);
        self.key.hash(state);
    }
}

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

#[derive(Clone, Debug, Insertable, QueryableByName, Queryable)]
#[table_name = "data_entries"]
pub struct InsertableDataEntry {
    pub block_uid: i64,
    pub transaction_id: String,
    pub uid: i64,
    pub superseded_by: i64,
    pub address: String,
    pub key: String,
    pub value_binary: Option<Vec<u8>>,
    pub value_bool: Option<bool>,
    pub value_integer: Option<i64>,
    pub value_string: Option<String>,
}

impl PartialEq for InsertableDataEntry {
    fn eq(&self, other: &InsertableDataEntry) -> bool {
        (&self.address, &self.key) == (&other.address, &other.key)
    }
}

impl Eq for InsertableDataEntry {}

impl Hash for InsertableDataEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.address.hash(state);
        self.key.hash(state);
    }
}

pub trait TransactionsRepoPool {
    fn transaction(&self, f: impl FnOnce(&dyn TransactionsRepo) -> Result<()>) -> Result<()>;
}

pub trait TransactionsRepo {
    fn get_prev_handled_height(&self) -> Result<Option<PrevHandledHeight>>;

    fn get_block_uid(&self, block_id: &str) -> Result<i64>;

    fn get_key_block_uid(&self) -> Result<i64>;

    fn get_total_block_id(&self) -> Result<Option<String>>;

    fn get_next_update_uid(&self) -> Result<i64>;

    fn insert_blocks_or_microblocks(&self, blocks: &[BlockMicroblock]) -> Result<Vec<i64>>;

    fn insert_transactions(&self, transactions: &[Transaction]) -> Result<()>;

    fn insert_associated_addresses(&self, associated_addresses: &[AssociatedAddress])
        -> Result<()>;

    fn insert_data_entries(&self, entries: &[InsertableDataEntry]) -> Result<()>;

    fn close_superseded_by(&self, updates: &[DataEntryUpdate]) -> Result<()>;

    fn reopen_superseded_by(&self, current_superseded_by: &[i64]) -> Result<()>;

    fn set_next_update_uid(&self, uid: i64) -> Result<()>;

    fn change_block_id(&self, block_uid: &i64, new_block_id: &str) -> Result<()>;

    fn update_transactions_block_references(&self, block_uid: &i64) -> Result<()>;

    fn delete_microblocks(&self) -> Result<()>;

    fn rollback_blocks_microblocks(&self, block_uid: &i64) -> Result<()>;

    fn rollback_data_entries(&self, block_uid: &i64) -> Result<Vec<DeletedDataEntry>>;

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

    fn last_data_entry(&self, address: String, key: String) -> Result<Option<InsertableDataEntry>>;

    fn update_data_entries_block_references(&self, block_uid: &i64) -> Result<()>;
}

impl TryFrom<std::sync::Arc<BlockchainUpdated>> for BlockchainUpdate {
    type Error = Error;

    fn try_from(value: std::sync::Arc<BlockchainUpdated>) -> Result<Self> {
        match value.update.as_ref() {
            Some(Update::Append(Append {
                body,
                transaction_ids,
                transaction_state_updates,
                ..
            })) => {
                let height = value.height;

                let data_entries = transaction_state_updates
                    .iter()
                    .enumerate()
                    .flat_map::<Vec<DataEntry>, _>(|(idx, su)| {
                        let transaction_id =
                            bs58::encode(&transaction_ids.get(idx).unwrap()).into_string();
                        su.data_entries
                            .iter()
                            .map(|de| DataEntry::from((de, &transaction_id)))
                            .collect()
                    })
                    .collect();

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
                            data_entries,
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
                            data_entries,
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

// from data_entry with transaction_id
impl From<(&waves::events::state_update::DataEntryUpdate, &String)> for DataEntry {
    fn from(de: (&waves::events::state_update::DataEntryUpdate, &String)) -> Self {
        let transaction_id = de.1.to_owned();
        let deu = de.0.data_entry.as_ref().unwrap();

        let value = match deu.value.as_ref() {
            Some(Value::IntValue(v)) => ValueDataEntry::Integer(v.to_owned()),
            Some(Value::BoolValue(v)) => ValueDataEntry::Bool(v.to_owned()),
            Some(Value::BinaryValue(v)) => ValueDataEntry::Binary(v.to_owned()),
            Some(Value::StringValue(v)) => ValueDataEntry::String(v.replace("\0", "\\0")),
            None => ValueDataEntry::String("".to_string()),
        };
        // nul symbol is badly processed at least by PostgreSQL
        // so escape this for safety
        let key =
            de.0.data_entry
                .as_ref()
                .unwrap()
                .key
                .clone()
                .replace("\0", "\\0");

        let fragments = Fragments::from((&key, &value));

        Self {
            address: bs58::encode(&de.0.address).into_string(),
            key,
            transaction_id,
            value,
            fragments,
        }
    }
}

impl From<(&String, &ValueDataEntry)> for Fragments {
    fn from(v: (&String, &ValueDataEntry)) -> Self {
        let key = split_fragments(v.0);
        let value = if let ValueDataEntry::String(s) = v.1 {
            split_fragments(s)
        } else {
            vec![]
        };
        Self { key, value }
    }
}

fn split_fragments(value: &str) -> Vec<DataEntryFragment> {
    let mut frs = value.split(FRAGMENT_SEPARATOR);

    let types = frs
        .next()
        .map(|fragment| {
            fragment
                .split('%')
                .into_iter()
                .skip(1) // first item is empty
                .collect()
        })
        .unwrap_or_else(Vec::new);

    let mut result = vec![];
    for (t, v) in types.into_iter().zip(frs) {
        match t {
            STRING_DESCRIPTOR => {
                let fragment = DataEntryFragment::String {
                    value: v.to_string(),
                };
                result.push(fragment)
            }
            INTEGER_DESCRIPTOR => {
                if let Ok(value) = v.parse() {
                    let fragment = DataEntryFragment::Integer { value };
                    result.push(fragment)
                } else {
                    break;
                }
            }
            _ => break,
        }
    }
    result
}

impl From<InsertableDataEntry> for DataEntry {
    fn from(ide: InsertableDataEntry) -> Self {
        let value = match ide {
            InsertableDataEntry {
                value_binary: Some(v),
                ..
            } => ValueDataEntry::Binary(v),
            InsertableDataEntry {
                value_bool: Some(v),
                ..
            } => ValueDataEntry::Bool(v),
            InsertableDataEntry {
                value_integer: Some(v),
                ..
            } => ValueDataEntry::Integer(v),
            InsertableDataEntry {
                value_string: Some(v),
                ..
            } => ValueDataEntry::String(v),
            _ => panic!("InsertableDataEntry without value: {:?}", ide),
        };
        let fragments = Fragments::from((&ide.key, &value));
        Self {
            address: ide.address,
            key: ide.key,
            transaction_id: ide.transaction_id,
            value,
            fragments,
        }
    }
}

// from data_entry, index and block_id
impl From<(&DataEntry, i64, i64)> for InsertableDataEntry {
    fn from(value: (&DataEntry, i64, i64)) -> Self {
        let value_binary = if let ValueDataEntry::Binary(v) = &value.0.value {
            Some(v.to_owned())
        } else {
            None
        };
        let value_bool = if let ValueDataEntry::Bool(v) = &value.0.value {
            Some(v.to_owned())
        } else {
            None
        };
        let value_integer = if let ValueDataEntry::Integer(v) = &value.0.value {
            Some(v.to_owned())
        } else {
            None
        };
        let value_string = if let ValueDataEntry::String(v) = &value.0.value {
            Some(v.to_owned())
        } else {
            None
        };
        Self {
            address: value.0.address.to_owned(),
            key: value.0.key.to_owned(),
            transaction_id: value.0.transaction_id.to_owned(),
            value_binary,
            value_bool,
            value_integer,
            value_string,
            block_uid: value.2,
            uid: value.1,
            superseded_by: -1,
        }
    }
}

fn encode_asset(asset: &[u8]) -> String {
    if asset.is_empty() {
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
    raw_transactions: &[waves::SignedTransaction],
    transaction_ids: &[Vec<u8>],
) -> Vec<TransactionUpdate> {
    transaction_ids
        .iter()
        .zip(raw_transactions.iter())
        .map(|(tx_id, tx)| {
            let transaction = tx.transaction.as_ref().unwrap();
            let sender_public_key = &transaction.sender_public_key;
            let mut addresses = if sender_public_key.is_empty() {
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

pub fn address_from_public_key(pk: &[u8], chain_id: u8) -> Address {
    let pkh = &keccak256(&blake2b256(&pk))[..20];
    address_from_public_key_hash(&pkh.to_vec(), chain_id)
}

fn address_from_public_key_hash(pkh: &[u8], chain_id: u8) -> Address {
    let mut addr = [0u8; 26];
    addr[0] = 1;
    addr[1] = chain_id;
    addr[2..22].clone_from_slice(&pkh[..20]);
    let chks = &keccak256(&blake2b256(&addr[..22]))[..4];
    for (i, v) in chks.iter().enumerate() {
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
