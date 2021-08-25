use wavesexchange_topic::{
    BlockchainHeight, ConfigFile, ConfigParameters, LeasingBalance, State, TestResource, Topic,
    Transaction, TransactionByAddress, TransactionExchange, TransactionType as Type,
};

use crate::providers::watchlist::{KeyPattern, MaybeFromTopic, WatchListItem};
use crate::waves::transactions::TransactionType;

impl MaybeFromTopic for ConfigFile {
    fn maybe_item(topic: &Topic) -> Option<Self> {
        if let Topic::Config(ConfigParameters { file }) = topic {
            return Some(file.to_owned());
        }
        None
    }
}

impl WatchListItem for ConfigFile {}

impl MaybeFromTopic for State {
    fn maybe_item(topic: &Topic) -> Option<Self> {
        if let Topic::State(state) = topic {
            return Some(state.to_owned());
        }
        None
    }
}

impl WatchListItem for State {}

impl MaybeFromTopic for TestResource {
    fn maybe_item(topic: &Topic) -> Option<Self> {
        if let Topic::TestResource(test_resource) = topic {
            return Some(test_resource.to_owned());
        }
        None
    }
}

impl WatchListItem for TestResource {}

impl MaybeFromTopic for BlockchainHeight {
    fn maybe_item(topic: &Topic) -> Option<Self> {
        if let Topic::BlockchainHeight = topic {
            return Some(Self {});
        }
        None
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
        }
    }
}

impl MaybeFromTopic for TransactionByAddress {
    fn maybe_item(topic: &Topic) -> Option<Self> {
        if let Topic::Transaction(Transaction::ByAddress(transaction)) = topic {
            return Some(transaction.to_owned());
        }
        None
    }
}

impl MaybeFromTopic for TransactionExchange {
    fn maybe_item(topic: &Topic) -> Option<Self> {
        if let Topic::Transaction(Transaction::Exchange(transaction)) = topic {
            return Some(transaction.to_owned());
        }
        None
    }
}

impl MaybeFromTopic for Transaction {
    fn maybe_item(topic: &Topic) -> Option<Self> {
        if let Topic::Transaction(transaction) = topic {
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
        if let Topic::LeasingBalance(leasing_balance) = topic {
            return Some(leasing_balance.to_owned());
        }
        None
    }
}

impl WatchListItem for LeasingBalance {}

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
