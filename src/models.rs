use crate::error::{self, Error};
use crate::providers::watchlist::{MaybeFromTopic, WatchListItem};
use crate::transactions::TransactionType;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::str::FromStr;
use url::Url;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Topic {
    Config(ConfigFile),
    State(State),
    TestResource(TestResource),
    BlockchainHeight,
    Transaction(Transaction),
}

impl TryFrom<&str> for Topic {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let url = Url::parse(s)?;

        match url.scheme() {
            "topic" => match url.host_str() {
                Some("config") => {
                    let config_file = ConfigFile::try_from(url.path())?;
                    Ok(Topic::Config(config_file))
                }
                Some("state") => {
                    let state = State::try_from(url.path())?;
                    Ok(Topic::State(state))
                }
                Some("test.resource") => {
                    let ps = TestResource::try_from(&url)?;
                    Ok(Topic::TestResource(ps))
                }
                Some("blockchain_height") => Ok(Topic::BlockchainHeight),
                Some("transaction") => {
                    let transaction = Transaction::try_from(url)?;
                    Ok(Topic::Transaction(transaction))
                }
                _ => Err(Error::InvalidTopic(s.to_owned())),
            },
            _ => Err(Error::InvalidTopic(s.to_owned())),
        }
    }
}

#[test]
fn string_to_topic() {
    let s = "topic://config/asd/qwe";
    if let Topic::Config(config) = Topic::try_from(s).unwrap() {
        assert_eq!(config.path, "/asd/qwe".to_string());
    } else {
        panic!("not config")
    }
    let s = "topic://state/asd/qwe";
    if let Topic::State(state) = Topic::try_from(s).unwrap() {
        assert_eq!(state.address, "asd".to_string());
        assert_eq!(state.key, "qwe".to_string());
    } else {
        panic!("not state")
    }
    let s = "topic://test.resource/asd/qwe?a=b";
    if let Topic::TestResource(test_resource) = Topic::try_from(s).unwrap() {
        assert_eq!(test_resource.path, "/asd/qwe".to_string());
        assert_eq!(test_resource.query, Some("a=b".to_string()));
    } else {
        panic!("not test_resource")
    }
    let s = "topic://blockchain_height";
    if let Topic::BlockchainHeight = Topic::try_from(s).unwrap() {
    } else {
        panic!("not blockchain_height")
    }
}

impl ToString for Topic {
    fn to_string(&self) -> String {
        let mut url = Url::parse("topic://").unwrap();
        match self {
            Topic::Config(cf) => {
                url.set_host(Some("config")).unwrap();
                url.set_path(&cf.path);
                url.as_str().to_owned()
            }
            Topic::State(state) => {
                url.set_host(Some("state")).unwrap();
                url.set_path(state.to_string().as_str());
                url.as_str().to_owned()
            }
            Topic::TestResource(ps) => {
                url.set_host(Some("test.resource")).unwrap();
                url.set_path(&ps.path);
                if let Some(query) = ps.query.clone() {
                    url.set_query(Some(query.as_str()));
                }
                url.as_str().to_owned()
            }
            Topic::BlockchainHeight => {
                url.set_host(Some("blockchain_height")).unwrap();
                url.as_str().to_owned()
            }
            Topic::Transaction(Transaction::ByAddress(transaction)) => {
                url.set_host(Some("transaction")).unwrap();
                url.set_path(&transaction.tx_type.to_string());
                url.set_query(Some(format!("address={}", &transaction.address).as_str()));
                url.as_str().to_owned()
            }
            Topic::Transaction(Transaction::Exchange(transaction)) => {
                url.set_host(Some("transaction")).unwrap();
                url.set_path(Type::Exchange.to_string().as_str());
                url.set_query(Some(
                    format!(
                        "amount_asset={}&price_asset={}",
                        &transaction.amount_asset, &transaction.price_asset
                    )
                    .as_str(),
                ));
                url.as_str().to_owned()
            }
        }
    }
}

#[test]
fn topic_to_string_test() {
    let t = Topic::Config(ConfigFile {
        path: "asd/qwe".to_string(),
    });
    assert_eq!(t.to_string(), "topic://config/asd/qwe".to_string());
    let t = Topic::State(State {
        address: "asd".to_string(),
        key: "qwe".to_string(),
    });
    assert_eq!(t.to_string(), "topic://state/asd/qwe".to_string());
    let t = Topic::TestResource(TestResource {
        path: "asd/qwe".to_string(),
        query: Some("a=b".to_string()),
    });
    assert_eq!(
        t.to_string(),
        "topic://test.resource/asd/qwe?a=b".to_string()
    );
    let t = Topic::BlockchainHeight;
    assert_eq!(t.to_string(), "topic://blockchain_height".to_string());
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ConfigFile {
    path: String,
}

impl ToString for ConfigFile {
    fn to_string(&self) -> String {
        self.path.clone()
    }
}

impl TryFrom<&str> for ConfigFile {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let p = std::path::Path::new(s)
            .to_str()
            .ok_or(Error::InvalidConfigPath(s.to_owned()))?;

        Ok(ConfigFile { path: p.to_owned() })
    }
}

impl From<ConfigFile> for Topic {
    fn from(config_file: ConfigFile) -> Self {
        Self::Config(config_file)
    }
}

impl MaybeFromTopic for ConfigFile {
    fn maybe_item(topic: Topic) -> Option<Self> {
        if let Topic::Config(config_file) = topic {
            return Some(config_file);
        }
        return None;
    }
}

impl WatchListItem for ConfigFile {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct State {
    address: String,
    key: String,
}

impl ToString for State {
    fn to_string(&self) -> String {
        format!("{}/{}", self.address, self.key)
    }
}

impl TryFrom<&str> for State {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let parts = s
            .trim_start_matches("/")
            .split("/")
            .take(2)
            .collect::<Vec<_>>();
        if parts.len() == 2 {
            let address = parts[0].to_string();
            let key = parts[1].to_string();
            Ok(State { address, key })
        } else {
            Err(Error::InvalidStatePath(s.to_owned()))
        }
    }
}

impl From<State> for Topic {
    fn from(state: State) -> Self {
        Self::State(state)
    }
}

impl MaybeFromTopic for State {
    fn maybe_item(topic: Topic) -> Option<Self> {
        if let Topic::State(state) = topic {
            return Some(state);
        }
        return None;
    }
}

impl WatchListItem for State {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TestResource {
    pub path: String,
    pub query: Option<String>,
}

impl ToString for TestResource {
    fn to_string(&self) -> String {
        let mut s = self.path.clone();
        if let Some(query) = self.query.clone() {
            s = format!("{}?{}", s, query).to_string();
        }
        s
    }
}

impl TryFrom<&url::Url> for TestResource {
    type Error = Error;

    fn try_from(u: &url::Url) -> Result<Self, Self::Error> {
        Ok(Self {
            path: u.path().to_string(),
            query: u.query().map(|q| q.to_owned()),
        })
    }
}

impl From<TestResource> for Topic {
    fn from(test_resource: TestResource) -> Self {
        Self::TestResource(test_resource)
    }
}

impl MaybeFromTopic for TestResource {
    fn maybe_item(topic: Topic) -> Option<Self> {
        if let Topic::TestResource(test_resource) = topic {
            return Some(test_resource);
        }
        return None;
    }
}

impl WatchListItem for TestResource {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BlockchainHeight {}

impl ToString for BlockchainHeight {
    fn to_string(&self) -> String {
        "".to_string()
    }
}

impl TryFrom<&url::Url> for BlockchainHeight {
    type Error = Error;

    fn try_from(_u: &url::Url) -> Result<Self, Self::Error> {
        Ok(Self {})
    }
}

impl From<BlockchainHeight> for Topic {
    fn from(_blockchain_height: BlockchainHeight) -> Self {
        Self::BlockchainHeight
    }
}

impl MaybeFromTopic for BlockchainHeight {
    fn maybe_item(topic: Topic) -> Option<Self> {
        if let Topic::BlockchainHeight = topic {
            return Some(Self {});
        }
        return None;
    }
}

impl WatchListItem for BlockchainHeight {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Transaction {
    ByAddress(TransactionByAddress),
    Exchange(TransactionExchange),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TransactionExchange {
    pub amount_asset: String,
    pub price_asset: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TransactionByAddress {
    pub tx_type: Type,
    pub address: String,
}

impl TryFrom<Url> for Transaction {
    type Error = Error;

    fn try_from(value: Url) -> Result<Self, Self::Error> {
        let tx_type: Type = FromStr::from_str(
            &value
                .path_segments()
                .ok_or_else(|| Error::InvalidTransactionType(value.path().to_string()))?
                .next()
                .ok_or_else(|| Error::InvalidTransactionType(value.path().to_string()))?,
        )?;
        match tx_type {
            Type::Exchange => {
                if let Ok(tx) = TransactionExchange::try_from(value.clone()) {
                    return Ok(Self::Exchange(tx));
                }
            }
            _ => (),
        }
        let tx = TransactionByAddress::try_from(value)?;
        Ok(Self::ByAddress(tx))
    }
}

impl TryFrom<Url> for TransactionByAddress {
    type Error = Error;

    fn try_from(value: Url) -> Result<Self, Self::Error> {
        let tx_type = FromStr::from_str(
            &value
                .path_segments()
                .ok_or_else(|| Error::InvalidTransactionType(value.path().to_string()))?
                .next()
                .ok_or_else(|| Error::InvalidTransactionType(value.path().to_string()))?,
        )?;
        let address = get_value_from_query(&value, "address")?;
        Ok(Self { address, tx_type })
    }
}

impl ToString for TransactionExchange {
    fn to_string(&self) -> String {
        format!(
            "exchange?amount_asset={}&price_asset={}",
            self.amount_asset, self.price_asset
        )
    }
}

impl ToString for TransactionByAddress {
    fn to_string(&self) -> String {
        format!("{}?{}", self.tx_type.to_string(), self.address)
    }
}

impl ToString for Transaction {
    fn to_string(&self) -> String {
        match self {
            Self::ByAddress(tx) => tx.to_string(),
            Self::Exchange(tx) => tx.to_string(),
        }
    }
}

impl TryFrom<Url> for TransactionExchange {
    type Error = Error;

    fn try_from(value: Url) -> Result<Self, Self::Error> {
        let price_asset = get_value_from_query(&value, "price_asset")?;
        let amount_asset = get_value_from_query(&value, "amount_asset")?;
        Ok(Self {
            price_asset,
            amount_asset,
        })
    }
}

fn get_value_from_query(value: &Url, key: &str) -> Result<String, Error> {
    for (k, v) in value.query_pairs() {
        if k == key {
            return Ok(v.to_string());
        }
    }
    return Err(Error::InvalidTransactionQuery(error::ErrorQuery(
        value.query().map(ToString::to_string),
    )));
}

#[test]
fn transaction_topic_test() {
    let url = Url::parse("topic://transaction/all?address=some_address").unwrap();
    if let Transaction::ByAddress(transaction) = Transaction::try_from(url).unwrap() {
        assert_eq!(transaction.tx_type.to_string(), "all".to_string());
        assert_eq!(transaction.address, "some_address".to_string());
        assert_eq!(
            "topic://transaction/all?address=some_address".to_string(),
            Topic::Transaction(Transaction::ByAddress(transaction)).to_string()
        );
    } else {
        panic!("wrong transaction")
    }
    let url = Url::parse("topic://transaction/issue?address=some_other_address").unwrap();
    if let Transaction::ByAddress(transaction) = Transaction::try_from(url).unwrap() {
        assert_eq!(transaction.tx_type.to_string(), "issue".to_string());
        assert_eq!(transaction.address, "some_other_address".to_string());
        assert_eq!(
            "topic://transaction/issue?address=some_other_address".to_string(),
            Topic::Transaction(Transaction::ByAddress(transaction)).to_string()
        );
    }
    let url = Url::parse("topic://transaction/exchange").unwrap();
    let error = Transaction::try_from(url);
    assert!(error.is_err());
    assert_eq!(
        format!("{}", error.unwrap_err()),
        "InvalidTransactionQuery: None".to_string()
    );
    let url = Url::parse("topic://transaction/exchange?amount_asset=asd&price_asset=qwe").unwrap();
    if let Transaction::Exchange(transaction) = Transaction::try_from(url).unwrap() {
        assert_eq!(transaction.amount_asset, "asd".to_string());
        assert_eq!(transaction.price_asset, "qwe".to_string());
        assert_eq!(
            "topic://transaction/exchange?amount_asset=asd&price_asset=qwe".to_string(),
            Topic::Transaction(Transaction::Exchange(transaction)).to_string()
        );
    } else {
        panic!("wrong exchange transaction")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Type {
    All,
    Genesis,
    Payment,
    Issue,
    Transfer,
    Reissue,
    Burn,
    Exchange,
    Lease,
    LeaseCancel,
    CreateAlias,
    MassTransfer,
    DataTransaction,
    SetScript,
    SponsorFee,
    SetAssetScript,
    InvokeScript,
    UpdateAssetInfo,
}

impl ToString for Type {
    fn to_string(&self) -> String {
        let s = match self {
            Self::All => "all",
            Self::Genesis => "genesis",
            Self::Payment => "payment",
            Self::Issue => "issue",
            Self::Transfer => "transfer",
            Self::Reissue => "reissue",
            Self::Burn => "burn",
            Self::Exchange => "exchange",
            Self::Lease => "lease",
            Self::LeaseCancel => "lease_cancel",
            Self::CreateAlias => "create_alias",
            Self::MassTransfer => "mass_transfer",
            Self::DataTransaction => "data_transaction",
            Self::SetScript => "set_script",
            Self::SponsorFee => "sponsor_fee",
            Self::SetAssetScript => "set_asset_script",
            Self::InvokeScript => "invoke_script",
            Self::UpdateAssetInfo => "update_asset_info",
        };
        s.to_string()
    }
}

impl FromStr for Type {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let transaction_type = match s {
            "all" => Self::All,
            "genesis" => Self::Genesis,
            "payment" => Self::Payment,
            "issue" => Self::Issue,
            "transfer" => Self::Transfer,
            "reissue" => Self::Reissue,
            "burn" => Self::Burn,
            "exchange" => Self::Exchange,
            "lease" => Self::Lease,
            "lease_cancel" => Self::LeaseCancel,
            "create_alias" => Self::CreateAlias,
            "mass_transfer" => Self::MassTransfer,
            "data_transaction" => Self::DataTransaction,
            "set_script" => Self::SetScript,
            "sponsor_fee" => Self::SponsorFee,
            "set_asset_script" => Self::SetAssetScript,
            "invoke_script" => Self::InvokeScript,
            "update_asset_info" => Self::UpdateAssetInfo,
            _ => return Err(Error::InvalidTransactionType(s.to_string())),
        };
        Ok(transaction_type)
    }
}

impl From<TransactionType> for Type {
    fn from(value: TransactionType) -> Self {
        match value {
            TransactionType::Genesis => Self::Genesis,
            TransactionType::Payment => Self::Payment,
            TransactionType::Issue => Self::Issue,
            TransactionType::Transfer => Self::Transfer,
            TransactionType::Reissue => Self::Reissue,
            TransactionType::Burn => Self::Burn,
            TransactionType::Exchange => Self::Exchange,
            TransactionType::Lease => Self::Lease,
            TransactionType::LeaseCancel => Self::LeaseCancel,
            TransactionType::CreateAlias => Self::CreateAlias,
            TransactionType::MassTransfer => Self::MassTransfer,
            TransactionType::DataTransaction => Self::DataTransaction,
            TransactionType::SetScript => Self::SetScript,
            TransactionType::SponsorFee => Self::SponsorFee,
            TransactionType::SetAssetScript => Self::SetAssetScript,
            TransactionType::InvokeScript => Self::InvokeScript,
            TransactionType::UpdateAssetInfo => Self::UpdateAssetInfo,
        }
    }
}

impl From<TransactionByAddress> for Topic {
    fn from(transaction: TransactionByAddress) -> Self {
        Self::Transaction(Transaction::ByAddress(transaction))
    }
}

impl From<TransactionExchange> for Topic {
    fn from(transaction: TransactionExchange) -> Self {
        Self::Transaction(Transaction::Exchange(transaction))
    }
}

impl From<Transaction> for Topic {
    fn from(transaction: Transaction) -> Self {
        Self::Transaction(transaction)
    }
}

impl MaybeFromTopic for TransactionByAddress {
    fn maybe_item(topic: Topic) -> Option<Self> {
        if let Topic::Transaction(Transaction::ByAddress(transaction)) = topic {
            return Some(transaction);
        }
        return None;
    }
}

impl MaybeFromTopic for TransactionExchange {
    fn maybe_item(topic: Topic) -> Option<Self> {
        if let Topic::Transaction(Transaction::Exchange(transaction)) = topic {
            return Some(transaction);
        }
        return None;
    }
}

impl MaybeFromTopic for Transaction {
    fn maybe_item(topic: Topic) -> Option<Self> {
        if let Topic::Transaction(transaction) = topic {
            return Some(transaction);
        }
        return None;
    }
}

impl WatchListItem for TransactionByAddress {}
impl WatchListItem for TransactionExchange {}
impl WatchListItem for Transaction {}
