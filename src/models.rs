#![allow(clippy::unused_unit)]

use wx_topic::{
    BlockchainHeight, ConfigFile, ExchangePair, LeasingBalance, State, TestResource, Topic,
    Transaction, TransactionByAddress, TransactionExchange, TransactionType as Type,
};

use crate::providers::watchlist::{KeyPattern, MaybeFromTopic, WatchListItem};
use crate::waves::transactions::TransactionType;

impl MaybeFromTopic for ExchangePair {
    fn maybe_item(topic: &Topic) -> Option<Self> {
        topic.data().as_pair().cloned()
    }
}

impl WatchListItem for ExchangePair {}

impl MaybeFromTopic for ConfigFile {
    fn maybe_item(topic: &Topic) -> Option<Self> {
        if let Some(conf_res) = topic.data().as_config() {
            return Some(conf_res.file.to_owned());
        }
        None
    }
}

impl WatchListItem for ConfigFile {}

impl MaybeFromTopic for State {
    fn maybe_item(topic: &Topic) -> Option<Self> {
        topic.data().as_state().cloned()
    }
}

impl WatchListItem for State {}

impl MaybeFromTopic for TestResource {
    fn maybe_item(topic: &Topic) -> Option<Self> {
        topic.data().as_test_resource().cloned()
    }
}

impl WatchListItem for TestResource {}

impl MaybeFromTopic for BlockchainHeight {
    fn maybe_item(topic: &Topic) -> Option<Self> {
        topic.data().as_blockchain_height().cloned()
    }
}

impl WatchListItem for BlockchainHeight {}

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
            TransactionType::Alias => Self::Alias,
            TransactionType::MassTransfer => Self::MassTransfer,
            TransactionType::Data => Self::Data,
            TransactionType::SetScript => Self::SetScript,
            TransactionType::Sponsorship => Self::Sponsorship,
            TransactionType::SetAssetScript => Self::SetAssetScript,
            TransactionType::InvokeScript => Self::InvokeScript,
            TransactionType::UpdateAssetInfo => Self::UpdateAssetInfo,
            TransactionType::InvokeExpression => Self::InvokeExpression,
        }
    }
}

impl MaybeFromTopic for TransactionByAddress {
    fn maybe_item(topic: &Topic) -> Option<Self> {
        if let Some(transaction) = topic.data().as_transaction() {
            if let Transaction::ByAddress(transaction) = transaction {
                return Some(transaction.to_owned());
            }
        }
        None
    }
}

impl MaybeFromTopic for TransactionExchange {
    fn maybe_item(topic: &Topic) -> Option<Self> {
        if let Some(transaction) = topic.data().as_transaction() {
            if let Transaction::Exchange(transaction) = transaction {
                return Some(transaction.to_owned());
            }
        }
        None
    }
}

impl MaybeFromTopic for Transaction {
    fn maybe_item(topic: &Topic) -> Option<Self> {
        if let Some(transaction) = topic.data().as_transaction() {
            return Some(transaction.to_owned());
        }
        None
    }
}

impl WatchListItem for TransactionByAddress {}
impl WatchListItem for TransactionExchange {}
impl WatchListItem for Transaction {}

impl MaybeFromTopic for LeasingBalance {
    fn maybe_item(topic: &Topic) -> Option<Self> {
        if let Some(leasing_balance) = topic.data().as_leasing_balance() {
            return Some(leasing_balance.to_owned());
        }
        None
    }
}

impl WatchListItem for LeasingBalance {}

impl KeyPattern for ExchangePair {
    const PATTERNS_SUPPORTED: bool = false;
    type PatternMatcher = ();

    fn new_matcher(&self) -> Self::PatternMatcher {
        ()
    }
}

impl KeyPattern for ConfigFile {
    const PATTERNS_SUPPORTED: bool = false;
    type PatternMatcher = ();

    fn new_matcher(&self) -> Self::PatternMatcher {
        ()
    }
}

impl KeyPattern for TestResource {
    const PATTERNS_SUPPORTED: bool = false;
    type PatternMatcher = ();

    fn new_matcher(&self) -> Self::PatternMatcher {
        ()
    }
}

impl KeyPattern for BlockchainHeight {
    const PATTERNS_SUPPORTED: bool = false;
    type PatternMatcher = ();

    fn new_matcher(&self) -> Self::PatternMatcher {
        ()
    }
}

impl KeyPattern for TransactionByAddress {
    const PATTERNS_SUPPORTED: bool = false;
    type PatternMatcher = ();

    fn new_matcher(&self) -> Self::PatternMatcher {
        ()
    }
}

impl KeyPattern for TransactionExchange {
    const PATTERNS_SUPPORTED: bool = false;
    type PatternMatcher = ();

    fn new_matcher(&self) -> Self::PatternMatcher {
        ()
    }
}
