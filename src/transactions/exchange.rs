use super::{Address, TransactionUpdate};
use crate::error::Error;
use serde::Serialize;
use std::convert::TryFrom;
use waves_protobuf_schemas::waves::transaction::Data;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExchangeData {
    pub sender_public_key: String,
    pub amount: i64,
    pub fee: Option<i64>,
    pub r#type: i64,
    pub version: i32,
    pub sell_matcher_fee: i64,
    pub sender: Address,
    pub fee_asset_id: Option<String>,
    pub proofs: Vec<String>,
    pub price: i64,
    pub id: String,
    pub order2: Order,
    pub order1: Order,
    pub buy_matcher_fee: i64,
    pub timestamp: i64,
    pub height: i32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Order {
    pub version: i32,
    pub sender: Address,
    pub sender_public_key: String,
    pub matcher_public_key: String,
    pub asset_pair: AssetPair,
    pub order_type: OrderType,
    pub amount: i64,
    pub price: i64,
    pub timestamp: i64,
    pub expiration: i64,
    pub matcher_fee: i64,
    pub matcher_fee_asset_id: Option<String>,
    pub proofs: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AssetPair {
    pub amount_asset: Option<String>,
    pub price_asset: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub enum OrderType {
    Sell,
    Buy,
}

impl TryFrom<&TransactionUpdate> for ExchangeData {
    type Error = Error;

    fn try_from(value: &TransactionUpdate) -> Result<Self, Self::Error> {
        if let Data::Exchange(exchange_data) = &value.data {
            let order1 = Order::try_from(exchange_data.orders.get(0).unwrap())?;
            let order2 = Order::try_from(exchange_data.orders.get(1).unwrap())?;
            Ok(Self {
                sender_public_key: value.sender_public_key.clone(),
                amount: exchange_data.amount,
                fee: value.fee.as_ref().map(|x| x.amount),
                r#type: 7,
                version: value.version,
                sell_matcher_fee: exchange_data.sell_matcher_fee,
                sender: value.sender.to_owned(),
                buy_matcher_fee: exchange_data.buy_matcher_fee,
                fee_asset_id: value.fee.as_ref().and_then(|amount| {
                    if amount.asset_id.len() > 0 {
                        Some(amount.asset_id.to_owned())
                    } else {
                        None
                    }
                }),
                proofs: value.proofs.to_owned(),
                price: exchange_data.price,
                id: value.id.to_owned(),
                order1,
                order2,
                timestamp: value.timestamp,
                height: value.height,
            })
        } else {
            Err(Error::InvalidExchangeData(value.data.clone()))
        }
    }
}

impl TryFrom<&waves_protobuf_schemas::waves::Order> for Order {
    type Error = Error;

    fn try_from(value: &waves_protobuf_schemas::waves::Order) -> Result<Self, Self::Error> {
        let sender = super::address_from_public_key(&value.sender_public_key, value.chain_id as u8);
        Ok(Self {
            version: value.version,
            sender,
            sender_public_key: bs58::encode(&value.sender_public_key).into_string(),
            matcher_public_key: bs58::encode(&value.matcher_public_key).into_string(),
            asset_pair: value.asset_pair.as_ref().into(),
            order_type: OrderType::try_from(value.order_side)?,
            amount: value.amount,
            price: value.price,
            timestamp: value.timestamp,
            expiration: value.expiration,
            matcher_fee: value.matcher_fee.as_ref().map_or(0, |amount| amount.amount),
            matcher_fee_asset_id: value
                .matcher_fee
                .as_ref()
                .map(|amount| encode_asset(&amount.asset_id))
                .flatten(),
            proofs: value
                .proofs
                .iter()
                .map(|proof| bs58::encode(proof).into_string())
                .collect(),
        })
    }
}

impl From<Option<&waves_protobuf_schemas::waves::AssetPair>> for AssetPair {
    fn from(value: Option<&waves_protobuf_schemas::waves::AssetPair>) -> Self {
        if let Some(waves_protobuf_schemas::waves::AssetPair {
            amount_asset_id,
            price_asset_id,
        }) = value
        {
            Self {
                amount_asset: encode_asset(amount_asset_id),
                price_asset: encode_asset(price_asset_id),
            }
        } else {
            Self {
                amount_asset: None,
                price_asset: None,
            }
        }
    }
}

impl TryFrom<i32> for OrderType {
    type Error = Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Buy),
            1 => Ok(Self::Sell),
            _ => Err(Error::InvalidOrderType(value)),
        }
    }
}

fn encode_asset(value: &Vec<u8>) -> Option<String> {
    if value.len() > 0 {
        Some(bs58::encode(value).into_string())
    } else {
        None
    }
}
