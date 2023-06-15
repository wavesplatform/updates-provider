use diesel::Insertable;
use std::convert::TryFrom;
use std::hash::{Hash, Hasher};

use waves_protobuf_schemas::waves;
use waves_protobuf_schemas::waves::data_transaction_data::data_entry::Value;
use waves_protobuf_schemas::waves::events::blockchain_updated::append::{
    BlockAppend, Body, MicroBlockAppend,
};
use waves_protobuf_schemas::waves::events::blockchain_updated::{Append, Update};
use waves_protobuf_schemas::waves::events::BlockchainUpdated;

use crate::error::{Error, Result};
use crate::schema::{associated_addresses, blocks_microblocks, data_entries, leasing_balances};
use crate::waves::transactions::{parse_transactions, InsertableTransaction};
use crate::waves::{
    Address, BlockMicroblockAppend, DataEntry as DataEntryDTO, DataEntryFragment, Fragments,
    LeasingBalance as LeasingBalanceDTO, RollbackData, ValueDataEntry,
};

pub const FRAGMENT_SEPARATOR: &str = "__";
pub const STRING_DESCRIPTOR: &str = "s";
pub const INTEGER_DESCRIPTOR: &str = "d";

pub const MAX_UID: i64 = i64::MAX - 1;

pub mod pool;
pub mod repo_consumer;
pub mod repo_provider;

#[derive(Debug, Clone)]
pub struct Config {
    pub blockchain_updates_url: String,
    pub updates_per_request: usize,
    pub max_wait_time_in_secs: u64,
}

#[derive(Clone, Debug, Insertable, QueryableByName)]
#[diesel(table_name = blocks_microblocks)]
pub struct BlockMicroblock {
    pub id: String,
    pub time_stamp: Option<i64>,
    pub height: i32,
}

#[derive(Clone, Debug, Insertable, QueryableByName, PartialEq, Eq, Hash)]
#[diesel(table_name = associated_addresses)]
pub struct AssociatedAddress {
    pub address: String,
    pub transaction_id: String,
}

impl From<(String, &Address)> for AssociatedAddress {
    fn from(value: (String, &Address)) -> Self {
        Self {
            address: value.1 .0.clone(),
            transaction_id: value.0,
        }
    }
}

#[derive(Clone, Debug)]
pub enum BlockchainUpdate {
    Block(BlockMicroblockAppend),
    Microblock(BlockMicroblockAppend),
    Rollback(RollbackData),
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

#[derive(Clone, Debug, Insertable)]
#[diesel(table_name = data_entries)]
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

#[derive(Clone, Debug, Insertable, QueryableByName, Queryable)]
#[diesel(table_name = data_entries)]
pub struct DataEntry {
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
    pub key_frag_0: Option<String>,
    pub key_frag_1: Option<String>,
    pub key_frag_2: Option<String>,
    pub key_frag_3: Option<String>,
    pub key_frag_4: Option<String>,
    pub key_frag_5: Option<String>,
    pub key_frag_6: Option<String>,
    pub key_frag_7: Option<String>,
}

impl PartialEq for DataEntry {
    fn eq(&self, other: &DataEntry) -> bool {
        (&self.address, &self.key) == (&other.address, &other.key)
    }
}

impl Eq for DataEntry {}

impl Hash for DataEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.address.hash(state);
        self.key.hash(state);
    }
}

#[derive(Clone, Debug, Insertable)]
#[diesel(table_name = leasing_balances)]
pub struct LeasingBalanceUpdate {
    pub superseded_by: i64,
    pub address: String,
}

#[derive(Clone, Debug)]
pub struct DeletedLeasingBalance {
    pub uid: i64,
    pub address: String,
}

impl PartialEq for DeletedLeasingBalance {
    fn eq(&self, other: &DeletedLeasingBalance) -> bool {
        self.address == other.address
    }
}

impl Eq for DeletedLeasingBalance {}

impl Hash for DeletedLeasingBalance {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.address.hash(state);
    }
}

#[derive(Clone, Debug, Insertable, QueryableByName, Queryable)]
#[diesel(table_name = leasing_balances)]
pub struct LeasingBalance {
    pub block_uid: i64,
    pub uid: i64,
    pub superseded_by: i64,
    pub address: String,
    pub balance_in: i64,
    pub balance_out: i64,
}

impl PartialEq for LeasingBalance {
    fn eq(&self, other: &LeasingBalance) -> bool {
        self.address == other.address
    }
}

impl Eq for LeasingBalance {}

impl Hash for LeasingBalance {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.address.hash(state);
    }
}

impl TryFrom<std::sync::Arc<BlockchainUpdated>> for BlockchainUpdate {
    type Error = Error;

    fn try_from(value: std::sync::Arc<BlockchainUpdated>) -> Result<Self> {
        match value.update.as_ref() {
            Some(Update::Append(Append {
                body,
                transaction_ids,
                transactions_metadata,
                transaction_state_updates,
                ..
            })) => {
                let height = value.height;

                let mut data_entries = vec![];
                let mut leasing_balances = vec![];

                for (idx, state_update) in transaction_state_updates.iter().enumerate() {
                    let transaction_id =
                        bs58::encode(&transaction_ids.get(idx).unwrap()).into_string();
                    for data_entry_update in state_update.data_entries.iter() {
                        let data_entry = convert_data_entry(data_entry_update, &transaction_id);
                        data_entries.push(data_entry);
                    }
                    for leasing_update in state_update.leasing_for_address.iter() {
                        let leasing_balance = LeasingBalanceDTO::from(leasing_update);
                        leasing_balances.push(leasing_balance);
                    }
                }

                match body {
                    Some(Body::Block(BlockAppend { block, .. })) => {
                        let block_uid = bs58::encode(&value.id).into_string();
                        let raw_transactions = &block.as_ref().unwrap().transactions;
                        let transactions = parse_transactions(
                            block_uid.clone(),
                            raw_transactions,
                            transactions_metadata,
                            transaction_ids,
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
                            leasing_balances,
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
                            raw_transactions,
                            transactions_metadata,
                            transaction_ids,
                        );

                        Ok(BlockchainUpdate::Microblock(BlockMicroblockAppend {
                            id: block_uid,
                            time_stamp: None,
                            height,
                            transactions,
                            data_entries,
                            leasing_balances,
                        }))
                    }
                    _ => Err(Error::GRPCBodyError("Append body is empty.".to_string())),
                }
            }
            Some(Update::Rollback(rollback)) => {
                let mut removed_transaction_ids = vec![];
                let mut data_entries = vec![];
                let mut leasing_balances = vec![];
                for tx_id in rollback.removed_transaction_ids.iter() {
                    let transaction_id = bs58::encode(tx_id).into_string();
                    removed_transaction_ids.push(transaction_id);
                }
                if let Some(state_update) = rollback.rollback_state_update.as_ref() {
                    for data_entry_update in state_update.data_entries.iter() {
                        let data_entry = convert_data_entry(data_entry_update, "");
                        data_entries.push(data_entry);
                    }
                    for leasing_update in state_update.leasing_for_address.iter() {
                        let leasing_balance = LeasingBalanceDTO::from(leasing_update);
                        leasing_balances.push(leasing_balance);
                    }
                }
                let block_id = bs58::encode(&value.id).into_string();
                Ok(BlockchainUpdate::Rollback(RollbackData {
                    block_id,
                    removed_transaction_ids,
                    data_entries,
                    leasing_balances,
                }))
            }
            _ => Err(Error::GRPCBodyError(
                "Unknown blockchain update.".to_string(),
            )),
        }
    }
}

fn convert_data_entry(
    data_entry_update: &waves::events::state_update::DataEntryUpdate,
    transaction_id: &str,
) -> DataEntryDTO {
    let transaction_id = transaction_id.to_owned();
    let deu = data_entry_update.data_entry.as_ref().unwrap();

    let value = match deu.value.as_ref() {
        Some(Value::IntValue(v)) => ValueDataEntry::Integer(v.to_owned()),
        Some(Value::BoolValue(v)) => ValueDataEntry::Bool(v.to_owned()),
        Some(Value::BinaryValue(v)) => ValueDataEntry::Binary(v.to_owned()),
        Some(Value::StringValue(v)) => ValueDataEntry::String(v.replace("\0", "\\0")),
        None => ValueDataEntry::Null,
    };
    // nul symbol is badly processed at least by PostgreSQL
    // so escape this for safety
    let key = deu.key.clone().replace("\0", "\\0");

    let fragments = Fragments::from((&key, &value));

    DataEntryDTO {
        address: bs58::encode(&data_entry_update.address).into_string(),
        key,
        transaction_id,
        value,
        fragments,
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

impl From<DataEntry> for DataEntryDTO {
    fn from(ide: DataEntry) -> Self {
        let value = match ide {
            DataEntry {
                value_binary: Some(v),
                ..
            } => ValueDataEntry::Binary(v),
            DataEntry {
                value_bool: Some(v),
                ..
            } => ValueDataEntry::Bool(v),
            DataEntry {
                value_integer: Some(v),
                ..
            } => ValueDataEntry::Integer(v),
            DataEntry {
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

// from data_entry, uid, block_id
impl From<(&DataEntryDTO, i64, i64)> for DataEntry {
    fn from(value: (&DataEntryDTO, i64, i64)) -> Self {
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
        let key_frags = extract_key_fragments(value.0.key.as_str());
        let [key_frag_0, key_frag_1, key_frag_2, key_frag_3, key_frag_4, key_frag_5, key_frag_6, key_frag_7] =
            key_frags;
        Self {
            block_uid: value.2,
            uid: value.1,
            superseded_by: -1,
            address: value.0.address.to_owned(),
            key: value.0.key.to_owned(),
            transaction_id: value.0.transaction_id.to_owned(),
            value_binary,
            value_bool,
            value_integer,
            value_string,
            key_frag_0,
            key_frag_1,
            key_frag_2,
            key_frag_3,
            key_frag_4,
            key_frag_5,
            key_frag_6,
            key_frag_7,
        }
    }
}

fn extract_key_fragments(key: &str) -> [Option<String>; 8] {
    let mut res: [Option<String>; 8] = Default::default();
    let frags = key.split("__").collect::<Vec<_>>();
    if frags.len() < 2 {
        return res;
    }
    let frags_descr = frags[0];
    let frags = &frags[1..];
    let valid_descr = {
        use itertools::Itertools;
        let valid_descr = frags_descr
            .chars()
            .tuples::<(char, char)>()
            .all(|(ch1, ch2)| ch1 == '%' && (ch2 == 's' || ch2 == 'd'));
        valid_descr && frags_descr.len() % 2 == 0 && frags_descr.len() / 2 == frags.len()
    };
    if valid_descr && frags.len() <= 8 {
        for i in 0..frags.len() {
            res[i] = Some(frags[i].to_owned());
        }
    }
    res
}

impl From<&waves::events::state_update::LeasingUpdate> for LeasingBalanceDTO {
    fn from(lu: &waves::events::state_update::LeasingUpdate) -> Self {
        let address = bs58::encode(&lu.address).into_string();
        let balance_in = lu.in_after;
        let balance_out = lu.out_after;
        Self {
            address,
            balance_in,
            balance_out,
        }
    }
}

// leasing_balance, uid, block_uid
impl From<(&LeasingBalanceDTO, i64, i64)> for LeasingBalance {
    fn from(value: (&LeasingBalanceDTO, i64, i64)) -> Self {
        Self {
            block_uid: value.2,
            uid: value.1,
            superseded_by: -1,
            address: value.0.address.to_owned(),
            balance_in: value.0.balance_in,
            balance_out: value.0.balance_out,
        }
    }
}

impl From<LeasingBalance> for LeasingBalanceDTO {
    fn from(ilb: LeasingBalance) -> Self {
        Self {
            address: ilb.address,
            balance_in: ilb.balance_in,
            balance_out: ilb.balance_out,
        }
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
