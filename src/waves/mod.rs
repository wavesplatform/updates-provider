use serde::Serialize;
use sha3::Digest;
use std::convert::TryInto;
use waves_protobuf_schemas::waves;
use waves_protobuf_schemas::waves::transaction::Data;

use self::transactions::TransactionUpdate;

pub mod transactions;

#[derive(Debug, Clone)]
pub struct Amount {
    pub asset_id: String,
    pub amount: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Address(pub String);

#[derive(Clone, Debug)]
pub struct BlockMicroblockAppend {
    pub id: String,
    pub time_stamp: Option<i64>,
    pub height: i32,
    pub transactions: Vec<TransactionUpdate>,
    pub data_entries: Vec<DataEntry>,
    pub leasing_balances: Vec<LeasingBalance>,
}

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
    Null,
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

#[derive(Debug, Clone, Serialize)]
pub struct LeasingBalance {
    pub address: String,
    #[serde(rename = "in")]
    pub balance_in: i64,
    #[serde(rename = "out")]
    pub balance_out: i64,
}

fn encode_asset(asset: &[u8]) -> String {
    if !asset.is_empty() {
        bs58::encode(asset).into_string()
    } else {
        "WAVES".to_string()
    }
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
    let pkh = &keccak256(&blake2b256(pk))[..20];
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

fn keccak256(message: &[u8]) -> [u8; 32] {
    let mut hasher = sha3::Keccak256::new();
    hasher.input(message);
    hasher.result().into()
}

pub fn blake2b256(message: &[u8]) -> [u8; 32] {
    use blake2::digest::{Update, VariableOutput};
    let mut hasher = blake2::VarBlake2b::new(32).unwrap();
    hasher.update(message);
    let mut arr = [0u8; 32];
    hasher.finalize_variable(|res| arr = res.try_into().unwrap());
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

#[test]
fn address_from_public_key_test() {
    let pk = vec![
        169, 213, 159, 238, 197, 81, 67, 140, 199, 67, 126, 57, 205, 117, 50, 139, 192, 195, 69,
        191, 200, 252, 145, 136, 67, 194, 84, 135, 114, 186, 38, 64,
    ];
    let address = address_from_public_key(&pk, 84);
    assert_eq!(address.0, "3NBVqYXrapgJP9atQccdBPAgJPwHDKkh6A8".to_string());
}
