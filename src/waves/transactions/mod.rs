use diesel::{
    deserialize::FromSql,
    serialize::{Output, ToSql},
    sql_types::*,
    Insertable, Queryable,
};
use std::convert::{TryFrom, TryInto};
use waves_protobuf_schemas::waves;
use waves_protobuf_schemas::waves::events::transaction_metadata::Metadata;
use waves_protobuf_schemas::waves::transaction::Data;
use wavesexchange_log as log;

use crate::error::{Error, Result};
use crate::schema::transactions;

use super::{
    address_from_public_key, encode_asset, maybe_add_addresses, maybe_add_addresses_from_meta,
    Address, Amount,
};

pub mod exchange;

#[derive(Clone, Debug, QueryableByName, Queryable)]
#[diesel(table_name = transactions)]
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
#[diesel(table_name = transactions)]
pub struct InsertableTransaction {
    pub block_uid: i64,
    pub id: String,
    pub tx_type: TransactionType,
    pub body: Option<serde_json::Value>,
    pub exchange_amount_asset: Option<String>,
    pub exchange_price_asset: Option<String>,
}

#[repr(i16)]
#[derive(Clone, Debug, Copy, AsExpression, FromSqlRow, PartialEq, Eq)]
#[diesel(sql_type = SmallInt)]
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
    // Somehow 18 is missing
    InvokeExpression = 19,
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
            // 18 is missing
            19 => Ok(TransactionType::InvokeExpression),
            _ => Err(Error::InvalidTransactionType(
                "unknown transaction type".into(),
            )),
        }
    }
}

impl ToSql<SmallInt, diesel::pg::Pg> for TransactionType
where
    i16: ToSql<SmallInt, diesel::pg::Pg>,
{
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, diesel::pg::Pg>) -> diesel::serialize::Result {
        let value = *self as i16;
        <i16 as ToSql<SmallInt, diesel::pg::Pg>>::to_sql(&value, &mut out.reborrow())
    }
}

impl<DB> FromSql<SmallInt, DB> for TransactionType
where
    DB: diesel::backend::Backend,
    i16: FromSql<SmallInt, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> diesel::deserialize::Result<Self> {
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
            Data::InvokeExpression(_) => TransactionType::InvokeExpression,
        }
    }
}

impl TryFrom<wx_topic::TransactionType> for TransactionType {
    type Error = Error;

    fn try_from(value: wx_topic::TransactionType) -> core::result::Result<Self, Self::Error> {
        match value {
            wx_topic::TransactionType::All => {
                Err(Error::InvalidDBTransactionType(value.to_string()))
            }
            wx_topic::TransactionType::Genesis => Ok(Self::Genesis),
            wx_topic::TransactionType::Payment => Ok(Self::Payment),
            wx_topic::TransactionType::Issue => Ok(Self::Issue),
            wx_topic::TransactionType::Transfer => Ok(Self::Transfer),
            wx_topic::TransactionType::Reissue => Ok(Self::Reissue),
            wx_topic::TransactionType::Burn => Ok(Self::Burn),
            wx_topic::TransactionType::Exchange => Ok(Self::Exchange),
            wx_topic::TransactionType::Lease => Ok(Self::Lease),
            wx_topic::TransactionType::LeaseCancel => Ok(Self::LeaseCancel),
            wx_topic::TransactionType::Alias => Ok(Self::Alias),
            wx_topic::TransactionType::MassTransfer => Ok(Self::MassTransfer),
            wx_topic::TransactionType::Data => Ok(Self::Data),
            wx_topic::TransactionType::SetScript => Ok(Self::SetScript),
            wx_topic::TransactionType::Sponsorship => Ok(Self::Sponsorship),
            wx_topic::TransactionType::SetAssetScript => Ok(Self::SetAssetScript),
            wx_topic::TransactionType::InvokeScript => Ok(Self::InvokeScript),
            wx_topic::TransactionType::UpdateAssetInfo => Ok(Self::UpdateAssetInfo),
            wx_topic::TransactionType::InvokeExpression => Ok(Self::InvokeExpression),
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
    pub meta: Option<Metadata>,
    pub sender_address: Address,
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
    transactions_metadata: &[waves::events::TransactionMetadata],
    transaction_ids: &[Vec<u8>],
) -> Vec<TransactionUpdate> {
    let base58 = |bytes| bs58::encode(bytes).into_string();
    let tx_ids = transaction_ids.iter().map(|tx_id| base58(tx_id));
    let transactions = raw_transactions.iter();
    let tx_meta = transactions_metadata.iter();
    tx_ids
        .zip(transactions)
        .zip(tx_meta)
        .filter_map(|((tx_id, tx), meta)| {
            match tx.transaction.as_ref() {
                Some(waves::signed_transaction::Transaction::WavesTransaction(transaction)) => {
                    let chain_id = transaction.chain_id as u8;
                    let mut addresses = vec![];
                    let sender_public_key = &transaction.sender_public_key;
                    let sender_address = {
                        // The preferred way is to use new metadata
                        if !meta.sender_address.is_empty() {
                            Some(Address(base58(&meta.sender_address)))
                        } else if !sender_public_key.is_empty() {
                            // Fallback to the old method
                            Some(address_from_public_key(sender_public_key, chain_id))
                        } else {
                            None
                        }
                    };
                    if let Some(ref address) = sender_address {
                        addresses.push(address.clone())
                    }
                    let data = transaction.data.clone().unwrap();
                    let meta = meta.metadata.clone();
                    maybe_add_addresses(&data, chain_id, &mut addresses);
                    maybe_add_addresses_from_meta(&meta, &mut addresses);
                    let tx_type = TransactionType::from(&data);
                    let sender_address = {
                        // It seems that old behavior was to compute this bogus "address" from zero-length input..
                        // Keeping it for compatibility yet, but this is weird.
                        sender_address
                            .unwrap_or_else(|| address_from_public_key(sender_public_key, chain_id))
                    };
                    // Deduplicate address list, because there can be dupes due to metadata processing in addition to tx body processing
                    addresses.sort();
                    addresses.dedup();
                    let update = TransactionUpdate {
                        tx_type,
                        data,
                        meta,
                        block_uid: block_uid.clone(),
                        id: tx_id,
                        addresses,
                        sender_address,
                        sender_public_key: base58(sender_public_key),
                        fee: transaction.fee.as_ref().map(Into::into),
                        timestamp: transaction.timestamp,
                        version: transaction.version,
                        proofs: tx.proofs.iter().map(|proof| base58(proof)).collect(),
                    };
                    Some(update)
                }
                Some(waves::signed_transaction::Transaction::EthereumTransaction(_)) => {
                    // Skip Ethereum transactions
                    log::debug!("Skipping Transaction::EthereumTransaction: {}", tx_id);
                    None
                }
                None => {
                    // Skip Ethereum transactions
                    log::debug!("Skipping Transaction::EthereumTransaction: {}", tx_id);
                    None
                }
            }
        })
        .collect()
}
