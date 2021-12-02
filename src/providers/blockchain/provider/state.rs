use async_trait::async_trait;
use itertools::Itertools;
use regex::Regex;
use std::collections::HashSet;
use wavesexchange_topic::{State, StateMultiPatterns, StateSingle, Topic};

use super::{DataFromBlock, Item, LastValue};
use crate::db::repo_provider::ProviderRepo;
use crate::error::Result;
use crate::providers::watchlist::{KeyPattern, PatternMatcher};
use crate::waves;

impl DataFromBlock for State {
    fn data_from_block(block: &waves::BlockMicroblockAppend) -> Vec<(String, Self)> {
        block
            .data_entries
            .iter()
            .map(|de| {
                let data = State::Single(StateSingle {
                    address: de.address.to_owned(),
                    key: de.key.to_owned(),
                });
                let current_value = serde_json::to_string(de).unwrap();
                (current_value, data)
            })
            .collect()
    }
}

#[async_trait]
impl<R: ProviderRepo + Sync> LastValue<R> for State {
    async fn last_value(self, repo: &R) -> Result<String> {
        Ok(match self {
            State::Single(StateSingle { address, key }) => {
                let maybe_data_entry =
                    tokio::task::block_in_place(move || repo.last_data_entry(address, key))?;
                if let Some(de) = maybe_data_entry {
                    let de = waves::DataEntry::from(de);
                    serde_json::to_string(&de)?
                } else {
                    serde_json::to_string(&None::<waves::DataEntry>)?
                }
            }
            State::MultiPatterns(StateMultiPatterns {
                addresses,
                key_patterns,
            }) => {
                let matching_keys = tokio::task::block_in_place(move || {
                    repo.find_matching_data_keys(addresses, key_patterns)
                })?;
                let matching_topics = matching_keys
                    .into_iter()
                    .map_into::<Topic>()
                    .map_into::<String>()
                    .collect_vec();
                serde_json::to_string(&matching_topics)?
            }
        })
    }
}

impl<R: ProviderRepo + Sync> Item<R> for State {}

impl KeyPattern for State {
    const PATTERNS_SUPPORTED: bool = true;
    type PatternMatcher = StateMatcher;

    fn new_matcher(&self) -> Self::PatternMatcher {
        match self {
            State::Single(state) => StateMatcher {
                addresses: std::iter::once(state.address.clone()).collect(),
                key_regex: pattern_utils::patterns_to_regex(&[&state.key]),
            },
            State::MultiPatterns(state) => StateMatcher {
                addresses: state.addresses.iter().cloned().collect(),
                key_regex: pattern_utils::patterns_to_regex(&state.key_patterns),
            },
        }
    }
}

pub struct StateMatcher {
    addresses: HashSet<String>,
    key_regex: Regex,
}

impl PatternMatcher<State> for StateMatcher {
    fn is_match(&self, value: &State) -> bool {
        match value {
            State::Single(state) => {
                if self.addresses.contains(&state.address) {
                    self.key_regex.is_match(&state.key)
                } else {
                    false
                }
            }
            State::MultiPatterns(_) => false,
        }
    }
}

mod pattern_utils {
    use itertools::Itertools;
    use regex::Regex;

    pub(super) fn patterns_to_regex<S: AsRef<str>>(patterns: &[S]) -> Regex {
        const WILDCARD_CHAR: char = '*';
        let mut patterns = patterns.iter().map(AsRef::as_ref).map(|pattern| {
            if pattern.contains(WILDCARD_CHAR) {
                pattern.split(WILDCARD_CHAR).map(regex::escape).join(".*")
            } else {
                regex::escape(pattern)
            }
        });
        let regex_text = "^(".to_owned() + &patterns.join(")|(") + ")$";
        Regex::new(&regex_text).expect("internal error: failed to build Regex")
    }

    #[test]
    fn pattern_to_regex_test() {
        let check = |pattern: &str, expected_regex: &str| {
            let patterns = &[pattern.to_owned()];
            let actual_regex = patterns_to_regex(patterns);
            assert_eq!(
                expected_regex,
                actual_regex.as_str(),
                "Failed: {} -> {}",
                pattern,
                expected_regex
            );
        };
        check("", "^()$");
        check("abc", "^(abc)$");
        check("*", "^(.*)$");
        check("*foo*", "^(.*foo.*)$");
        check("foo*bar", "^(foo.*bar)$");
        check("%", "^(%)$");
        check("_", "^(_)$");
        check("?", "^(\\?)$");
        check("%s%d_foo*", "^(%s%d_foo.*)$");
    }

    #[test]
    fn patterns_to_regex_test() {
        let check = |patterns: &[&str], expected_regex: &str| {
            let actual_regex = patterns_to_regex(patterns);
            assert_eq!(
                expected_regex,
                actual_regex.as_str(),
                "Failed: {:?} -> {}",
                patterns,
                expected_regex
            );
        };
        check(&[], "^()$");
        check(&[""], "^()$");
        check(&["abc"], "^(abc)$");
        check(&["abc", "def"], "^(abc)|(def)$");
        check(&["abc", "def", "ghi"], "^(abc)|(def)|(ghi)$");
    }
}
