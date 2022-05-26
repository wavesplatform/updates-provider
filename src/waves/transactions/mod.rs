use diesel::{
    deserialize::FromSql,
    serialize::{Output, ToSql},
    sql_types::*,
    Insertable, Queryable,
};
use std::convert::{TryFrom, TryInto};
use waves_protobuf_schemas::waves;
use waves_protobuf_schemas::waves::transaction::Data;
use wavesexchange_log as log;

use crate::error::{Error, Result};
use crate::schema::transactions;

use super::{address_from_public_key, encode_asset, maybe_add_addresses, Address, Amount};

pub mod exchange;

#[derive(Clone, Debug, QueryableByName, Queryable)]
#[table_name = "transactions"]
pub struct Transaction {
    pub uid: i64,
    pub block_uid: i64,
    pub id: String,
    pub tx_type: TransactionType,
    pub body: Option<serde_json::Value>,
    pub exchange_amount_asset: Option<String>,
    pub exchange_price_asset: Option<String>,
}

#[derive(Clone, Debug, Insertable)]
#[table_name = "transactions"]
pub struct InsertableTransaction {
    pub block_uid: i64,
    pub id: String,
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

impl TryFrom<wavesexchange_topic::TransactionType> for TransactionType {
    type Error = Error;

    fn try_from(
        value: wavesexchange_topic::TransactionType,
    ) -> core::result::Result<Self, Self::Error> {
        match value {
            wavesexchange_topic::TransactionType::All => {
                Err(Error::InvalidDBTransactionType(value.to_string()))
            }
            wavesexchange_topic::TransactionType::Genesis => Ok(Self::Genesis),
            wavesexchange_topic::TransactionType::Payment => Ok(Self::Payment),
            wavesexchange_topic::TransactionType::Issue => Ok(Self::Issue),
            wavesexchange_topic::TransactionType::Transfer => Ok(Self::Transfer),
            wavesexchange_topic::TransactionType::Reissue => Ok(Self::Reissue),
            wavesexchange_topic::TransactionType::Burn => Ok(Self::Burn),
            wavesexchange_topic::TransactionType::Exchange => Ok(Self::Exchange),
            wavesexchange_topic::TransactionType::Lease => Ok(Self::Lease),
            wavesexchange_topic::TransactionType::LeaseCancel => Ok(Self::LeaseCancel),
            wavesexchange_topic::TransactionType::Alias => Ok(Self::Alias),
            wavesexchange_topic::TransactionType::MassTransfer => Ok(Self::MassTransfer),
            wavesexchange_topic::TransactionType::Data => Ok(Self::Data),
            wavesexchange_topic::TransactionType::SetScript => Ok(Self::SetScript),
            wavesexchange_topic::TransactionType::Sponsorship => Ok(Self::Sponsorship),
            wavesexchange_topic::TransactionType::SetAssetScript => Ok(Self::SetAssetScript),
            wavesexchange_topic::TransactionType::InvokeScript => Ok(Self::InvokeScript),
            wavesexchange_topic::TransactionType::UpdateAssetInfo => Ok(Self::UpdateAssetInfo),
        }
    }
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
}

impl TryFrom<(i64, &TransactionUpdate)> for InsertableTransaction {
    type Error = Error;

    fn try_from(value: (i64, &TransactionUpdate)) -> Result<Self> {
        Ok(Self {
            block_uid: value.0,
            id: value.1.id.clone(),
            tx_type: value.1.tx_type,
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

pub fn parse_transactions(
    block_uid: String,
    raw_transactions: &[waves::SignedTransaction],
    transaction_ids: &[Vec<u8>],
) -> Vec<TransactionUpdate> {
    transaction_ids
        .iter()
        .zip(raw_transactions.iter())
        .inspect(|&(tx_id, tx)| {
            if tx.transaction.is_none() {
                log::debug!(
                    "Skipping Ethereum transaction: {}",
                    bs58::encode(tx_id).into_string(),
                );
            }
        })
        .filter(|&(_, tx)| tx.transaction.is_some()) // Skip Ethereum transactions
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
