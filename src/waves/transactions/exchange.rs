use protofixer::sort_protobuf_message_inplace;
use serde::Serialize;
use std::convert::TryFrom;
use waves_protobuf_schemas::{
    prost::Message,
    waves::{
        self,
        events::transaction_metadata::{ExchangeMetadata, Metadata},
        transaction::Data,
    },
};

use super::{Address, TransactionUpdate};
use crate::error::Error;
use crate::waves::blake2b256;

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
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Order {
    pub id: String,
    #[serde(skip)]
    pub chain_id: u8,
    pub version: OrderVersion,
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
    #[serde(skip_serializing_if = "skip_matcher_fee")]
    pub matcher_fee_asset_id: MatcherFeeAssetId,
    pub proofs: Vec<String>,
    #[serde(skip_serializing_if = "PriceMode::is_default")]
    pub price_mode: PriceMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eip712_signature: Option<String>,
}

#[derive(Copy, Clone, PartialEq, Eq, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum PriceMode {
    Default,
    FixedDecimals,
    AssetDecimals,
}

impl Default for PriceMode {
    fn default() -> Self {
        PriceMode::Default
    }
}

impl PriceMode {
    pub fn is_default(&self) -> bool {
        *self == PriceMode::Default
    }
}

impl From<waves::order::PriceMode> for PriceMode {
    fn from(mode: waves::order::PriceMode) -> Self {
        match mode {
            waves::order::PriceMode::Default => PriceMode::Default,
            waves::order::PriceMode::FixedDecimals => PriceMode::FixedDecimals,
            waves::order::PriceMode::AssetDecimals => PriceMode::AssetDecimals,
        }
    }
}

impl From<PriceMode> for waves::order::PriceMode {
    fn from(mode: PriceMode) -> Self {
        match mode {
            PriceMode::Default => waves::order::PriceMode::Default,
            PriceMode::FixedDecimals => waves::order::PriceMode::FixedDecimals,
            PriceMode::AssetDecimals => waves::order::PriceMode::AssetDecimals,
        }
    }
}

#[derive(Debug)]
pub enum MatcherFeeAssetId {
    NotExist,
    Exist(Option<String>),
}

fn skip_matcher_fee(value: &MatcherFeeAssetId) -> bool {
    matches!(value, MatcherFeeAssetId::NotExist)
}

impl serde::Serialize for MatcherFeeAssetId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if let MatcherFeeAssetId::Exist(v) = self {
            serde::Serialize::serialize(&v, serializer)
        } else {
            serde::Serialize::serialize(&None::<String>, serializer)
        }
    }
}

impl
    From<(
        Option<&waves_protobuf_schemas::waves::Amount>,
        &OrderVersion,
    )> for MatcherFeeAssetId
{
    fn from(
        value: (
            Option<&waves_protobuf_schemas::waves::Amount>,
            &OrderVersion,
        ),
    ) -> Self {
        if let OrderVersion::V1 | OrderVersion::V2 = value.1 {
            MatcherFeeAssetId::NotExist
        } else {
            let v = value
                .0
                .as_ref()
                .map(|amount| encode_asset(&amount.asset_id))
                .flatten();
            MatcherFeeAssetId::Exist(v)
        }
    }
}

impl MatcherFeeAssetId {
    fn to_bytes(&self) -> Vec<u8> {
        if let MatcherFeeAssetId::Exist(v) = self {
            asset_to_bytes(v.as_ref())
        } else {
            vec![]
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AssetPair {
    pub amount_asset: Option<String>,
    pub price_asset: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum OrderType {
    Sell,
    Buy,
}

#[derive(Debug)]
pub enum OrderVersion {
    V1 = 1,
    V2 = 2,
    V3 = 3,
    V4 = 4,
}

#[derive(Debug)]
struct OrderWithOptionalMeta<'a> {
    order: &'a waves::Order,
    order_id: Option<&'a [u8]>,
    sender_address: Option<&'a [u8]>,
    sender_public_key: Option<&'a [u8]>,
}

impl TryFrom<&TransactionUpdate> for ExchangeData {
    type Error = Error;

    fn try_from(value: &TransactionUpdate) -> Result<Self, Self::Error> {
        if let Data::Exchange(exchange_data) = &value.data {
            {
                // Sanity checks
                assert_eq!(
                    exchange_data.orders.len(),
                    2,
                    "Expected to have exactly 2 orders in an ExchangeTransaction"
                );
            }
            let order1 = exchange_data.orders.get(0).expect("order1");
            let order2 = exchange_data.orders.get(1).expect("order2");
            // ExchangeMetadata was added recently, don't fail if it is not present
            let orders = match value.meta.as_ref() {
                Some(Metadata::Exchange(ExchangeMetadata {
                    order_ids,
                    order_sender_addresses,
                    order_sender_public_keys,
                })) => {
                    {
                        // Sanity checks
                        assert!(
                            order_ids.is_empty() || order_ids.len() == 2,
                            "Expected to have exactly 2 order_ids in an ExchangeMetadata"
                        );
                        assert!(
                            order_sender_addresses.is_empty() || order_sender_addresses.len() == 2,
                            "Expected to have exactly 2 order_sender_addresses in an ExchangeMetadata"
                        );
                        assert!(
                            order_sender_public_keys.is_empty() || order_sender_public_keys.len() == 2,
                            "Expected to have exactly 2 order_sender_public_keys in an ExchangeMetadata"
                        );
                    }
                    [
                        OrderWithOptionalMeta {
                            order: order1,
                            order_id: order_ids.get(0).map(AsRef::as_ref),
                            sender_address: order_sender_addresses.get(0).map(AsRef::as_ref),
                            sender_public_key: order_sender_public_keys.get(0).map(AsRef::as_ref),
                        },
                        OrderWithOptionalMeta {
                            order: order2,
                            order_id: order_ids.get(1).map(AsRef::as_ref),
                            sender_address: order_sender_addresses.get(1).map(AsRef::as_ref),
                            sender_public_key: order_sender_public_keys.get(1).map(AsRef::as_ref),
                        },
                    ]
                }
                _ => {
                    // No additional metadata available - old node
                    [
                        OrderWithOptionalMeta {
                            order: order1,
                            order_id: None,
                            sender_address: None,
                            sender_public_key: None,
                        },
                        OrderWithOptionalMeta {
                            order: order2,
                            order_id: None,
                            sender_address: None,
                            sender_public_key: None,
                        },
                    ]
                }
            };
            let order1 = Order::try_from(&orders[0])?;
            let order2 = Order::try_from(&orders[1])?;
            Ok(Self {
                sender_public_key: value.sender_public_key.clone(),
                amount: exchange_data.amount,
                fee: value.fee.as_ref().map(|x| x.amount),
                r#type: 7,
                version: value.version,
                sell_matcher_fee: exchange_data.sell_matcher_fee,
                sender: value.sender_address.to_owned(),
                buy_matcher_fee: exchange_data.buy_matcher_fee,
                fee_asset_id: value.fee.as_ref().and_then(|amount| {
                    if !amount.asset_id.is_empty() {
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
            })
        } else {
            Err(Error::InvalidExchangeData(value.data.clone()))
        }
    }
}

impl TryFrom<&OrderWithOptionalMeta<'_>> for Order {
    type Error = Error;

    fn try_from(value: &OrderWithOptionalMeta) -> Result<Self, Self::Error> {
        let chain_id = value.order.chain_id as u8;
        let order_id = {
            // The preferred way is to use the new metadata field
            if let Some(order_id) = value.order_id {
                bs58::encode(order_id).into_string()
            } else {
                // The old style is to compute it later using `calculate_id()` call
                "".to_string()
            }
        };
        let sender_public_key = {
            // The preferred way is to use the new metadata field
            if let Some(sender_public_key) = value.sender_public_key {
                sender_public_key
            } else {
                // The old style is to extract it from Waves order
                match value.order.sender {
                    Some(waves::order::Sender::SenderPublicKey(ref sender_public_key)) => {
                        sender_public_key
                    }
                    Some(waves::order::Sender::Eip712Signature(_)) | None => {
                        // This shouldn't happen, Ethereum order MUST have the new metadata
                        panic!("Can't get sender_public_key from order: {:?}", value);
                    }
                }
            }
        };
        let sender_address = {
            // The preferred way is to use the new metadata field
            if let Some(sender_address) = value.sender_address {
                Address(bs58::encode(sender_address).into_string())
            } else {
                // The old style is to compute it based on sender_public_key
                super::address_from_public_key(sender_public_key, chain_id)
            }
        };
        let version = OrderVersion::try_from(value.order.version)?;
        let matcher_fee_asset_id = {
            let matcher_fee = value.order.matcher_fee.as_ref();
            MatcherFeeAssetId::from((matcher_fee, &version))
        };
        let price_mode = {
            // Strangely in protobuf we have proper type for it but the field is still just `i32`
            waves::order::PriceMode::from_i32(value.order.price_mode).expect("bad PriceMode")
        };
        let eip712_signature = {
            match value.order.sender {
                Some(waves::order::Sender::SenderPublicKey(_)) | None => None,
                Some(waves::order::Sender::Eip712Signature(ref sig)) => {
                    Some(format!("0x{}", hex::encode(sig)))
                }
            }
        };
        let mut order = Self {
            id: order_id,
            chain_id,
            version,
            sender: sender_address,
            sender_public_key: bs58::encode(sender_public_key).into_string(),
            matcher_public_key: bs58::encode(&value.order.matcher_public_key).into_string(),
            asset_pair: value.order.asset_pair.as_ref().into(),
            order_type: OrderType::try_from(value.order.order_side)?,
            amount: value.order.amount,
            price: value.order.price,
            timestamp: value.order.timestamp,
            expiration: value.order.expiration,
            matcher_fee: value
                .order
                .matcher_fee
                .as_ref()
                .map_or(0, |amount| amount.amount),
            matcher_fee_asset_id,
            proofs: value
                .order
                .proofs
                .iter()
                .map(|proof| bs58::encode(proof).into_string())
                .collect(),
            price_mode: PriceMode::from(price_mode),
            eip712_signature,
        };
        if order.id == "" {
            // Fallback to the old method only if there is no metadata
            order.calculate_id();
        }
        Ok(order)
    }
}

impl TryFrom<i32> for OrderVersion {
    type Error = Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::V1),
            2 => Ok(Self::V2),
            3 => Ok(Self::V3),
            4 => Ok(Self::V4),
            _ => Err(Error::InvalidOrderVersion(value)),
        }
    }
}

impl From<&OrderVersion> for i32 {
    fn from(value: &OrderVersion) -> i32 {
        match value {
            OrderVersion::V1 => 1,
            OrderVersion::V2 => 2,
            OrderVersion::V3 => 3,
            OrderVersion::V4 => 4,
        }
    }
}

impl serde::Serialize for OrderVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let value: u8 = match self {
            OrderVersion::V1 => 1,
            OrderVersion::V2 => 2,
            OrderVersion::V3 => 3,
            OrderVersion::V4 => 4,
        };
        serde::Serialize::serialize(&value, serializer)
    }
}

impl Order {
    fn calculate_id(&mut self) {
        let bytes = match self.version {
            OrderVersion::V1 => self.bytes_v1(),
            OrderVersion::V2 => self.bytes_v2(),
            OrderVersion::V3 => self.bytes_v3(),
            OrderVersion::V4 => self.bytes_v4(),
        };
        let id = bs58::encode(&blake2b256(bytes.as_slice())).into_string();
        self.id = id;
    }

    fn bytes_v1(&self) -> Vec<u8> {
        let sender_public_key_bytes = bs58::decode(&self.sender_public_key).into_vec().unwrap();
        let matcher_public_key_bytes = bs58::decode(&self.matcher_public_key).into_vec().unwrap();
        let asset_pair_bytes = self.asset_pair.to_bytes();
        let order_type_bytes = self.order_type.to_bytes();
        sender_public_key_bytes
            .iter()
            .chain(matcher_public_key_bytes.iter())
            .chain(asset_pair_bytes.iter())
            .chain(order_type_bytes.iter())
            .chain(self.price.to_be_bytes().iter())
            .chain(self.amount.to_be_bytes().iter())
            .chain(self.timestamp.to_be_bytes().iter())
            .chain(self.expiration.to_be_bytes().iter())
            .chain(self.matcher_fee.to_be_bytes().iter())
            .cloned()
            .collect::<Vec<_>>()
    }

    fn bytes_v2(&self) -> Vec<u8> {
        vec![vec![2], self.bytes_v1()].concat()
    }

    fn bytes_v3(&self) -> Vec<u8> {
        let matcher_fee_asset_bytes = self.matcher_fee_asset_id.to_bytes();
        vec![vec![3], self.bytes_v1(), matcher_fee_asset_bytes].concat()
    }

    fn bytes_v4(&self) -> Vec<u8> {
        let mut order_proto: waves_protobuf_schemas::waves::Order = self.into();
        order_proto.proofs = vec![];
        let mut buffer = vec![];
        order_proto.encode(&mut buffer).unwrap();
        sort_protobuf_message_inplace(&mut buffer).expect("bad protobuf message");
        buffer
    }
}

impl From<&Order> for waves_protobuf_schemas::waves::Order {
    fn from(value: &Order) -> Self {
        Self {
            chain_id: value.chain_id as i32,
            matcher_public_key: bs58::decode(&value.matcher_public_key).into_vec().unwrap(),
            asset_pair: (&value.asset_pair).into(),
            order_side: (&value.order_type).into(),
            amount: value.amount,
            price: value.price,
            timestamp: value.timestamp,
            expiration: value.expiration,
            matcher_fee: get_matcher_fee(value),
            version: (&value.version).into(),
            proofs: value
                .proofs
                .iter()
                .map(|proof| bs58::decode(proof).into_vec().unwrap())
                .collect(),
            price_mode: waves::order::PriceMode::from(value.price_mode) as i32,
            sender: match value.eip712_signature {
                None => {
                    let sender_public_key_bytes =
                        bs58::decode(&value.sender_public_key).into_vec().unwrap();
                    Some(waves::order::Sender::SenderPublicKey(
                        sender_public_key_bytes,
                    ))
                }
                Some(ref eip712_signature) if eip712_signature.starts_with("0x") => {
                    let eip712_signature = eip712_signature.strip_prefix("0x").unwrap();
                    let eip712_signature_bytes = hex::decode(eip712_signature).unwrap();
                    Some(waves::order::Sender::Eip712Signature(
                        eip712_signature_bytes,
                    ))
                }
                Some(_) => None, // Malformed signature, should not normally happen
            },
        }
    }
}

fn get_matcher_fee(value: &Order) -> Option<waves_protobuf_schemas::waves::Amount> {
    match value {
        Order {
            matcher_fee: 0,
            matcher_fee_asset_id: MatcherFeeAssetId::NotExist,
            ..
        } => None,
        Order {
            matcher_fee: 0,
            matcher_fee_asset_id: MatcherFeeAssetId::Exist(None),
            ..
        } => None,
        Order {
            matcher_fee,
            matcher_fee_asset_id: MatcherFeeAssetId::Exist(v),
            ..
        } => Some(waves_protobuf_schemas::waves::Amount {
            asset_id: decode_asset(v.as_ref()),
            amount: *matcher_fee,
        }),
        Order {
            matcher_fee,
            matcher_fee_asset_id: MatcherFeeAssetId::NotExist,
            ..
        } => Some(waves_protobuf_schemas::waves::Amount {
            asset_id: vec![],
            amount: *matcher_fee,
        }),
    }
}

fn asset_to_bytes(asset: Option<impl AsRef<[u8]>>) -> Vec<u8> {
    if let Some(asset_str) = asset {
        [vec![1], bs58::decode(asset_str).into_vec().unwrap()].concat()
    } else {
        vec![0]
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

impl From<&AssetPair> for Option<waves_protobuf_schemas::waves::AssetPair> {
    fn from(value: &AssetPair) -> Self {
        if let AssetPair {
            amount_asset: None,
            price_asset: None,
        } = value
        {
            None
        } else {
            Some(waves_protobuf_schemas::waves::AssetPair {
                amount_asset_id: decode_asset(value.amount_asset.as_ref()),
                price_asset_id: decode_asset(value.price_asset.as_ref()),
            })
        }
    }
}

impl AssetPair {
    fn to_bytes(&self) -> Vec<u8> {
        vec![
            asset_to_bytes(self.amount_asset.as_ref()),
            asset_to_bytes(self.price_asset.as_ref()),
        ]
        .concat()
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

impl From<&OrderType> for i32 {
    fn from(value: &OrderType) -> Self {
        match value {
            OrderType::Buy => 0,
            OrderType::Sell => 1,
        }
    }
}

impl OrderType {
    fn to_bytes(&self) -> Vec<u8> {
        vec![if let Self::Buy = self { 0 } else { 1 }]
    }
}

fn encode_asset(value: &[u8]) -> Option<String> {
    if !value.is_empty() {
        Some(bs58::encode(value).into_string())
    } else {
        None
    }
}

fn decode_asset(value: Option<&String>) -> Vec<u8> {
    if let Some(v) = value {
        bs58::decode(v).into_vec().unwrap()
    } else {
        vec![]
    }
}

#[test]
fn test_order_id_v1() {
    let mut order = Order {
        id: "".to_string(),
        chain_id: 87,
        sender: Address("3PJUhtX97wj2nCGmN8tYWQ9e9FAu6wVsqQL".to_string()),
        sender_public_key: "CHnZGZ8QBjeDTeaPqg8zirAR8wB6xfAap8echHrLz2Zw".to_string(),
        version: OrderVersion::V1,
        matcher_public_key: "GoVxp9iFXvDhGFUYG4fZ4GU3yQDsxyti8f3D5tNYiAXX".to_string(),
        asset_pair: AssetPair { price_asset: None, amount_asset: Some("8LQW8f7P5d5PZM7GtZEBgaqRPGSzS3DfPuiXrURJ4AJS".to_string()) },
        order_type: OrderType::Buy,
        amount: 10000,
        price: 30000000,
        timestamp: 1491193192031,
        expiration: 1491365992031,
        matcher_fee: 1000000,
        matcher_fee_asset_id: MatcherFeeAssetId::NotExist,
        proofs: vec!["5odnp8FcvrsPjQEM7d6bKcEYBVKRKmw526WAruaQtwJUS1i92GU8DXY4p7y4nP1ae7ktbEo7Wbm4XVVxuvadcsuT".to_string()],
        price_mode: Default::default(),
        eip712_signature: None,
    };
    order.calculate_id();
    assert_eq!(
        "LGzcHf1Pq9ZTfF4kQ4PdXyCuBuMRy9ksKxzDxkn9Js9".to_string(),
        order.id
    );
}

#[test]
fn test_order_id_v2() {
    let mut order = Order {
        id: "".to_string(),
        chain_id: 87,
        sender: Address("3P6WEqUiHb5wBCm25Mz2YjZEJMvZtVwWm7k".to_string()),
        sender_public_key: "EMcQtZeRT7L65MJjs8WCMDZsaQPK6EgL4UuTojejwmjT".to_string(),
        version: OrderVersion::V2,
        matcher_public_key: "7kPFrHDiGw1rCm7LPszuECwWYL3dMf6iMifLRDJQZMzy".to_string(),
        asset_pair: AssetPair { price_asset: None, amount_asset: Some("7qJUQFxniMQx45wk12UdZwknEW9cDgvfoHuAvwDNVjYv".to_string()) },
        order_type: OrderType::Buy,
        amount: 200000000,
        price: 150000000,
        timestamp: 1547223831381,
        expiration: 1547231031382,
        matcher_fee: 700000,
        matcher_fee_asset_id: MatcherFeeAssetId::NotExist,
        proofs: vec!["3j7b2Jz94cYHJRsXGsrKCccJRctJEe1GFEJTMCeAt92oReoX277ihkKNU4MC7SmKS1Rn9Y3SFB9cfj8ovbUpfykh".to_string()],
        price_mode: Default::default(),
        eip712_signature: None,
    };
    order.calculate_id();
    assert_eq!(
        "6c83pXf4fLbp3TBk3qruBChB7v6sjasK843e21jGjgba".to_string(),
        order.id
    );
}

#[test]
fn test_order_id_v3() {
    let mut order = Order {
        id: "".to_string(),
        chain_id: 87,
        sender: Address("3PMvT4mGVEe1So9DCkKa3uMpd8QQrpSfe6f".to_string()),
        sender_public_key: "AB7REBdFxc1WZoMRjbJ5yYddBQtGkBEncryW6cRo5sC2".to_string(),
        version: OrderVersion::V3,
        matcher_public_key: "9cpfKN9suPNvfeUNphzxXMjcnn974eme8ZhWUjaktzU5".to_string(),
        asset_pair: AssetPair { amount_asset: None, price_asset: Some("DG2xFkPdDwKUoBkzGAhQtLpSGzfXLiCYPEzeKH2Ad24p".to_string()) },
        order_type: OrderType::Buy,
        amount: 200000000,
        price: 11962000,
        timestamp: 1617526620343,
        expiration: 1620032220343,
        matcher_fee: 300000,
        matcher_fee_asset_id: MatcherFeeAssetId::Exist(None),
        proofs: vec!["5EENS5qrCQS2vVqopUoj6a4G3n6nryVKajzAi9KDo7R6rhjCCxFZ6LtnPBDZhieWAZDQmwg4aVjuacc8RtB5zPda".to_string()],
        price_mode: Default::default(),
        eip712_signature: None,
    };
    order.calculate_id();
    assert_eq!(
        "3Q9C79wgadXCX4gCp2AEvtQZEYCoZkGXei7zoTmd2DEm".to_string(),
        order.id
    );
}

#[allow(deprecated)] // Allow old-style base64
#[test]
fn test_order_id_v4_a() {
    let mut order = Order {
        id: "".to_string(),
        chain_id: 84,
        sender: Address("3N2PfQcNqimdTzwXhnEQy8RXBkZQQTfWZ48".to_string()),
        sender_public_key: "4AA2W4CTE3eX4kgeW5SgqcuFKRLG9RD3SqVKSCJ8Xf9Q".to_string(),
        version: OrderVersion::V4,
        matcher_public_key: "7oeqv1MBNFpAZqV68VhUWcceemnhkqFus3ayFJqX6nNV".to_string(),
        asset_pair: AssetPair { amount_asset: Some("56BWjKYmRs2jQz8vu7aXw7YMFebgTEvKQKtoD7o3Y2Ze".to_string()), price_asset: None },
        order_type: OrderType::Buy,
        amount: 10000,
        price: 10000,
        timestamp: 1654102940663,
        expiration: 1655542940563,
        matcher_fee: 300000,
        matcher_fee_asset_id: MatcherFeeAssetId::NotExist,
        proofs: vec!["56KgPM7N7qA29VDhaxFYLP3E84dPjcS1kvrwihDp4MCVXbCjDTccyW45anDK3tPDp6y95WUKJXescycZKHjFFVDJ".to_string()],
        price_mode: PriceMode::FixedDecimals,
        eip712_signature: None,
    };
    assert_eq!(
        order.bytes_v4(),
        base64::decode(concat!(
            "CFQSIC7rfmAccyl3Uf9UavPXiFeV6p02TYJGy3+6/ic5pc4/GiBlGaDdglW+ZWAU",
            "/B6J761ocaERvAg37BO4hslL0Iz0GiIiCiA8wonSKjAVV9BOPoi3axKZeFod7pLB",
            "zLIzTchsmFAbwTCQTjiQTkD3t9+BkjBIk4eysJcwUgQQ4KcSWARwAQ==",
        ))
        .expect("bad test data"),
    );
    order.calculate_id();
    assert_eq!(
        "2eJzm4LvRUgSgVQ2UaMeTtzJv88FEhg2ZUJZnpRikiL7".to_string(),
        order.id
    );
}

#[allow(deprecated)] // Allow old-style base64
#[test]
fn test_order_id_v4_b() {
    let mut order = Order {
        id: "".to_string(),
        chain_id: 84,
        sender: Address("3NAwHNM4bit7zTdQvixGoyWjvu9tbMxVx7z".to_string()),
        sender_public_key: "5gJT98dYcM6VPKGp1AM8jKwqi9VHkpmfqgotMirNiG8ZPbLRN4qHXMR14fwHhrdaaaDZDmK9rSRweMQhTuLb1BrV".to_string(),
        version: OrderVersion::V4,
        matcher_public_key: "7oeqv1MBNFpAZqV68VhUWcceemnhkqFus3ayFJqX6nNV".to_string(),
        asset_pair: AssetPair { amount_asset: Some("56BWjKYmRs2jQz8vu7aXw7YMFebgTEvKQKtoD7o3Y2Ze".to_string()), price_asset: None },
        order_type: OrderType::Sell,
        amount: 10000,
        price: 10000,
        timestamp: 1654102940663,
        expiration: 1655542940663,
        matcher_fee: 300000,
        matcher_fee_asset_id: MatcherFeeAssetId::NotExist,
        proofs: vec![],
        price_mode: PriceMode::AssetDecimals,
        eip712_signature: Some("0xc95509f78a317a01e39c9fbf5a7541f04b47181ac46a3e12a31c0489d14bddd54cb71b13554b1acae37ffecc936bd448db604d2796a94c812482133e343a9d0e1b".to_string()),
    };
    assert_eq!(
        order.bytes_v4(),
        base64::decode(concat!(
            "CFQaIGUZoN2CVb5lYBT8HonvrWhxoRG8CDfsE7iGyUvQjPQaIiIKIDzCidIqMBVX",
            "0E4+iLdrEpl4Wh3uksHMsjNNyGyYUBvBKAEwkE44kE5A97ffgZIwSPeHsrCXMFIE",
            "EOCnElgEakHJVQn3ijF6AeOcn79adUHwS0cYGsRqPhKjHASJ0Uvd1Uy3GxNVSxrK",
            "43/+zJNr1EjbYE0nlqlMgSSCEz40Op0OG3AC",
        ))
        .expect("bad test data"),
    );
    order.calculate_id();
    assert_eq!(
        "94syx42gegn8HXK7s3hFii4KFxxfLepgprJbc5eicseq".to_string(),
        order.id
    );
}

#[test]
fn test_order_conversion() {
    let waves_order = waves::Order {
        chain_id: 87,
        sender: Some(waves::order::Sender::SenderPublicKey(vec![
            220, 50, 71, 14, 248, 224, 41, 231, 130, 10, 175, 36, 161, 42, 123, 94, 89, 192, 200,
            248, 168, 120, 220, 203, 122, 77, 140, 219, 31, 133, 85, 36,
        ])),
        matcher_public_key: vec![
            128, 10, 102, 186, 12, 222, 12, 94, 172, 144, 109, 83, 59, 213, 227, 244, 220, 226, 36,
            47, 251, 233, 206, 3, 182, 186, 67, 156, 191, 213, 123, 110,
        ],
        asset_pair: Some(waves::AssetPair {
            amount_asset_id: vec![],
            price_asset_id: vec![
                30, 148, 7, 19, 82, 118, 161, 37, 149, 253, 200, 97, 168, 130, 95, 16, 127, 223,
                58, 79, 41, 187, 252, 154, 70, 63, 90, 253, 54, 79, 159, 145,
            ],
        }),
        order_side: 0,
        amount: 11446590227,
        price: 30694623,
        timestamp: 1620720516480,
        expiration: 1620720581480,
        matcher_fee: Some(waves::Amount {
            asset_id: vec![],
            amount: 300000,
        }),
        version: 3,
        proofs: vec![vec![
            159, 10, 173, 216, 5, 7, 39, 152, 173, 7, 100, 193, 164, 103, 34, 34, 180, 85, 250, 34,
            27, 218, 29, 167, 14, 76, 25, 112, 171, 121, 145, 104, 198, 46, 6, 225, 34, 54, 59, 13,
            255, 181, 90, 188, 133, 224, 138, 142, 104, 158, 36, 19, 80, 11, 237, 65, 69, 42, 163,
            49, 72, 229, 37, 132,
        ]],
        price_mode: waves::order::PriceMode::FixedDecimals as i32,
    };
    let order_with_meta = OrderWithOptionalMeta {
        order: &waves_order,
        order_id: None,
        sender_address: None,
        sender_public_key: None,
    };
    let order = Order::try_from(&order_with_meta).unwrap();
    let waves_order_2 = (&order).into();
    assert_eq!(waves_order, waves_order_2);
}
