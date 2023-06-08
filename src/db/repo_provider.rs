//! Provider's database view (read-only operations).

use super::{BlockMicroblock, DataEntry, LeasingBalance};
use crate::error::Result;
use crate::providers::blockchain::provider::exchange_pair::ExchangePairData;
use crate::waves::transactions::{Transaction, TransactionType};
use async_trait::async_trait;
use wx_topic::{ExchangePair, StateSingle};

pub use self::repo_impl::PostgresProviderRepo;

#[async_trait]
pub trait ProviderRepo {
    async fn last_transaction_by_address(&self, address: String) -> Result<Option<Transaction>>;

    async fn last_transaction_by_address_and_type(
        &self,
        address: String,
        transaction_type: TransactionType,
    ) -> Result<Option<Transaction>>;

    async fn last_exchange_transaction(
        &self,
        amount_asset: String,
        price_asset: String,
    ) -> Result<Option<Transaction>>;

    async fn last_leasing_balance(&self, address: String) -> Result<Option<LeasingBalance>>;

    async fn last_data_entry(&self, address: String, key: String) -> Result<Option<DataEntry>>;

    async fn last_exchange_pairs_transactions(
        &self,
        pair: ExchangePair,
    ) -> Result<Vec<ExchangePairData>>;

    async fn last_blocks_microblocks(&self) -> Result<Vec<BlockMicroblock>>;

    async fn find_matching_data_keys(
        &self,
        addresses: Vec<String>,
        key_patterns: Vec<String>,
    ) -> Result<Vec<StateSingle>>;
}

mod repo_impl {
    use diesel::prelude::*;
    use diesel::sql_types::{BigInt, VarChar};

    use super::ProviderRepo;
    use crate::db::pool::{PgPoolWithStats, PooledPgConnection};
    use crate::db::{BlockMicroblock, DataEntry, LeasingBalance};
    use crate::error::Result;
    use crate::providers::blockchain::provider::exchange_pair::ExchangePairData;
    use crate::schema::{associated_addresses, data_entries, leasing_balances, transactions};
    use crate::waves::transactions::{Transaction, TransactionType};
    use async_trait::async_trait;
    use diesel::dsl::sql;
    use itertools::Itertools;
    use wavesexchange_log::{debug, timer};
    use wx_topic::{ExchangePair, StateSingle};

    const MAX_UID: i64 = i64::MAX - 1;

    /// Provider's repo implementation that uses Postgres database as the storage.
    ///
    /// Can be cloned freely, no need to wrap in `Arc`.
    #[derive(Clone)]
    pub struct PostgresProviderRepo {
        pool: PgPoolWithStats,
    }

    impl PostgresProviderRepo {
        pub fn new(pool: PgPoolWithStats) -> Self {
            PostgresProviderRepo { pool }
        }

        async fn get_conn(&self) -> Result<PooledPgConnection> {
            let conn = self.pool.get().await?;
            Ok(conn)
        }
    }

    #[async_trait]
    impl ProviderRepo for PostgresProviderRepo {
        async fn last_transaction_by_address(
            &self,
            address: String,
        ) -> Result<Option<Transaction>> {
            self.get_conn()
                .await?
                .last_transaction_by_address(address)
                .await
        }

        async fn last_transaction_by_address_and_type(
            &self,
            address: String,
            transaction_type: TransactionType,
        ) -> Result<Option<Transaction>> {
            self.get_conn()
                .await?
                .last_transaction_by_address_and_type(address, transaction_type)
                .await
        }

        async fn last_exchange_transaction(
            &self,
            amount_asset: String,
            price_asset: String,
        ) -> Result<Option<Transaction>> {
            self.get_conn()
                .await?
                .last_exchange_transaction(amount_asset, price_asset)
                .await
        }

        async fn last_leasing_balance(&self, address: String) -> Result<Option<LeasingBalance>> {
            self.get_conn().await?.last_leasing_balance(address).await
        }

        async fn last_data_entry(&self, address: String, key: String) -> Result<Option<DataEntry>> {
            self.get_conn().await?.last_data_entry(address, key).await
        }

        async fn last_exchange_pairs_transactions(
            &self,
            pair: ExchangePair,
        ) -> Result<Vec<ExchangePairData>> {
            self.get_conn()
                .await?
                .last_exchange_pairs_transactions(pair)
                .await
        }

        async fn last_blocks_microblocks(&self) -> Result<Vec<BlockMicroblock>> {
            self.get_conn().await?.last_blocks_microblocks().await
        }

        async fn find_matching_data_keys(
            &self,
            addresses: Vec<String>,
            key_patterns: Vec<String>,
        ) -> Result<Vec<StateSingle>> {
            self.get_conn()
                .await?
                .find_matching_data_keys(addresses, key_patterns)
                .await
        }
    }

    #[async_trait]
    impl ProviderRepo for PooledPgConnection {
        async fn last_transaction_by_address(
            &self,
            address: String,
        ) -> Result<Option<Transaction>> {
            self.interact(|conn| {
                timer!("last_transaction_by_address()", verbose);

                Ok(associated_addresses::table
                    .inner_join(transactions::table)
                    .filter(associated_addresses::address.eq(address))
                    .select(transactions::all_columns.nullable())
                    .order(transactions::uid.desc())
                    .first::<Option<Transaction>>(conn)
                    .optional()?
                    .flatten())
            })
            .await?
        }

        async fn last_transaction_by_address_and_type(
            &self,
            address: String,
            transaction_type: TransactionType,
        ) -> Result<Option<Transaction>> {
            self.interact(move |conn| {
                timer!("last_transaction_by_address_and_type()", verbose);

                Ok(associated_addresses::table
                    .inner_join(transactions::table)
                    .filter(associated_addresses::address.eq(address))
                    .filter(transactions::tx_type.eq(transaction_type))
                    .select(transactions::all_columns.nullable())
                    .order(transactions::uid.desc())
                    .first::<Option<Transaction>>(conn)
                    .optional()?
                    .flatten())
            })
            .await?
        }

        async fn last_exchange_transaction(
            &self,
            amount_asset: String,
            price_asset: String,
        ) -> Result<Option<Transaction>> {
            self.interact(|conn| {
                timer!("last_exchange_transaction()", verbose);

                Ok(transactions::table
                    .filter(transactions::exchange_amount_asset.eq(amount_asset))
                    .filter(transactions::exchange_price_asset.eq(price_asset))
                    .select(transactions::all_columns.nullable())
                    .order(transactions::uid.desc())
                    .first::<Option<Transaction>>(conn)
                    .optional()?
                    .flatten())
            })
            .await?
        }

        async fn last_leasing_balance(&self, address: String) -> Result<Option<LeasingBalance>> {
            self.interact(|conn| {
                timer!("last_leasing_balance()", verbose);

                Ok(leasing_balances::table
                    .filter(leasing_balances::address.eq(address))
                    .select(leasing_balances::all_columns.nullable())
                    .filter(leasing_balances::superseded_by.eq(MAX_UID))
                    .first::<Option<LeasingBalance>>(conn)
                    .optional()?
                    .flatten())
            })
            .await?
        }

        async fn last_data_entry(&self, address: String, key: String) -> Result<Option<DataEntry>> {
            self.interact(|conn| {
                timer!("last_data_entry()", verbose);

                Ok(data_entries::table
                    .filter(data_entries::address.eq(address))
                    .filter(data_entries::key.eq(key))
                    .filter(
                        data_entries::value_binary
                            .is_not_null()
                            .or(data_entries::value_bool.is_not_null())
                            .or(data_entries::value_integer.is_not_null())
                            .or(data_entries::value_string.is_not_null()),
                    )
                    .select(data_entries::all_columns.nullable())
                    .filter(data_entries::superseded_by.eq(MAX_UID))
                    .first::<Option<DataEntry>>(conn)
                    .optional()?
                    .flatten())
            })
            .await?
        }

        async fn last_exchange_pairs_transactions(
            &self,
            pair: ExchangePair,
        ) -> Result<Vec<ExchangePairData>> {
            self.interact(|conn| {
                let first_uid = block_uid_1day_ago(conn)?;

                Ok(diesel::sql_query(r#"
                    select t.uid, b.height, b.id block_id, b.time_stamp::BIGINT as block_time_stamp, exchange_amount_asset as amount_asset, exchange_price_asset as price_asset,
                        (body::json->'amount')::TEXT::BIGINT as amount_asset_volume, (body::json->'price')::TEXT::BIGINT as price_asset_volume
                    from blocks_microblocks b
                        inner join transactions t on t.block_uid = b.uid
                    where t.block_uid > $1
                        and t.tx_type = 7
                        and t.exchange_amount_asset = $2
                        and t.exchange_price_asset = $3
                        and b.time_stamp > 0
                    "#)
                    .bind::<BigInt, _>(first_uid)
                    .bind::<VarChar, _>(pair.amount_asset)
                    .bind::<VarChar, _>(pair.price_asset)
                    .load::<ExchangePairData>(conn)?
                )
            }).await?
        }

        async fn last_blocks_microblocks(&self) -> Result<Vec<BlockMicroblock>> {
            self.interact(|conn| {
                let first_uid = block_uid_1day_ago(conn)?;
                Ok(diesel::sql_query("select id, time_stamp, height from blocks_microblocks where uid > $1 and time_stamp > 0 order by uid")
                    .bind::<BigInt, _>(first_uid)
                    .load::<BlockMicroblock>(conn)?
                )
            }).await?
        }

        async fn find_matching_data_keys(
            &self,
            addresses: Vec<String>,
            key_patterns: Vec<String>,
        ) -> Result<Vec<StateSingle>> {
            // Function `dsl::any()` is deprecated in Diesel 2.0 in favor of `.eq_any()`,
            // but there is no `.like_any()` function - don't know how to fix `like(any(...))` call
            #[allow(deprecated)] // for import
            use diesel::dsl::any;
            #[allow(deprecated)] // for usages
            self.interact(|conn| {
                timer!("find_matching_data_keys()", verbose);

                let start_time = std::time::Instant::now();
                let (n_addr, n_patt) = (addresses.len(), key_patterns.len());
                let has_patterns = key_patterns.iter().any(|s| pattern_utils::has_patterns(s));
                let res: Vec<(String, String)> = if has_patterns {
                    let key_patterns_fragmented = key_patterns
                        .iter()
                        .map(|s| pattern_utils::key_frag::try_split_key(s))
                        .collect::<Option<Vec<_>>>();
                    if let Some(keys_frags) = key_patterns_fragmented {
                        // Key is a proper fragmented string - use optimized search
                        #[allow(unstable_name_collisions)] // for `intersperse` from Itertools
                        let mut condition = keys_frags
                            .iter()
                            .map(|key_frags| {
                                key_frags
                                    .iter()
                                    .enumerate()
                                    .filter_map(|(i, frag)| frag.map(|s| (i, s)))
                                    .map(|(i, v)| {
                                        let v = v.replace('\'', r"\'"); // escape quotes
                                        format!("key_frag_{} = '{}'", i, v)
                                    })
                                    .intersperse(" AND ".to_string())
                                    .collect::<String>()
                            })
                            .map(|mut s| {
                                if !s.is_empty() {
                                    s.insert_str(0, "(");
                                    s.push_str(")")
                                }
                                s
                            })
                            .intersperse(" OR ".to_string())
                            .collect::<String>();
                        if condition != "" {
                            condition.insert_str(0, "(");
                            condition.push_str(")")
                        }
                        data_entries::table
                            .filter(data_entries::address.eq(any(addresses)))
                            .filter(sql::<diesel::sql_types::Bool>(&condition))
                            .filter(data_entries::superseded_by.eq(MAX_UID))
                            .select((data_entries::address, data_entries::key))
                            .order((data_entries::address, data_entries::key))
                            .load(conn)?
                    } else {
                        // Fallback to the 'LIKE' approach
                        let key_likes = key_patterns
                            .iter()
                            .map(String::as_str)
                            .map(pattern_utils::pattern_to_sql_like)
                            .collect::<Vec<_>>();
                        data_entries::table
                            .filter(data_entries::address.eq(any(addresses)))
                            .filter(data_entries::key.like(any(key_likes)))
                            .filter(data_entries::superseded_by.eq(MAX_UID))
                            .select((data_entries::address, data_entries::key))
                            .order((data_entries::address, data_entries::key))
                            .load(conn)?
                    }
                } else {
                    data_entries::table
                        .filter(data_entries::address.eq(any(addresses)))
                        .filter(data_entries::key.eq(any(key_patterns)))
                        .filter(data_entries::superseded_by.eq(MAX_UID))
                        .select((data_entries::address, data_entries::key))
                        .order((data_entries::address, data_entries::key))
                        .load(conn)?
                };
                let res = res
                    .into_iter()
                    .map(|(address, key)| StateSingle { address, key })
                    .collect::<Vec<_>>();
                let elapsed = start_time.elapsed().as_millis();
                debug!(
                    "fetched {} keys from {} addrs and {} patterns in {}ms",
                    res.len(),
                    n_addr,
                    n_patt,
                    elapsed,
                );

                Ok(res)
            })
            .await?
        }
    }

    fn block_uid_1day_ago(conn: &mut PgConnection) -> Result<i64> {
        #[derive(Debug, QueryableByName)]
        struct BlockUid {
            #[diesel(sql_type = BigInt)]
            uid: i64,
        }

        let blocks = diesel::sql_query(r#"
            select uid from blocks_microblocks where time_stamp < (
                select extract (epoch from (to_timestamp(time_stamp/1000) - '1 day'::interval) ) * 1000 as stamp from blocks_microblocks where time_stamp > 0 order by uid desc limit 1
            ) order by time_stamp desc limit 1
        "#)
        .load::<BlockUid>(conn)?;

        let block = blocks.first().unwrap_or(&BlockUid { uid: MAX_UID });

        Ok(block.uid)
    }

    mod pattern_utils {
        use itertools::Itertools;
        use std::borrow::Cow;

        const WILDCARD_CHAR: char = '*';

        pub(super) fn has_patterns(pattern: &str) -> bool {
            pattern.contains(WILDCARD_CHAR)
        }

        pub(super) fn pattern_to_sql_like(pattern: &str) -> String {
            if !pattern.contains(WILDCARD_CHAR) {
                return escape_literal(pattern).into_owned();
            }

            pattern.split(WILDCARD_CHAR).map(escape_literal).join("%")
        }

        fn escape_literal(s: &str) -> Cow<str> {
            let mut s = Cow::Borrowed(s);
            if s.contains('%') {
                s = Cow::Owned(s.replace('%', "\\%"));
            }
            if s.contains('_') {
                s = Cow::Owned(s.replace('_', "\\_"));
            }
            s
        }

        #[test]
        fn test_has_patterns() {
            assert!(!has_patterns(""));
            assert!(!has_patterns("abc"));
            assert!(has_patterns("*"));
            assert!(has_patterns("*foo*"));
            assert!(has_patterns("foo*bar"));
            assert!(!has_patterns("%"));
            assert!(!has_patterns("_"));
            assert!(has_patterns("%s%d_foo*"));
            assert!(has_patterns("%s%d__foo*"));
            assert!(has_patterns("%s%d__foo__*"));
            assert!(has_patterns("%d%s__*__foo"));
        }

        #[test]
        fn pattern_to_sql_like_test() {
            let check = |pattern, expected_like| {
                let actual_like = pattern_to_sql_like(pattern);
                assert_eq!(
                    expected_like, actual_like,
                    "Failed: {} -> {}",
                    pattern, expected_like
                );
            };
            check("", "");
            check("abc", "abc");
            check("*", "%");
            check("*foo*", "%foo%");
            check("foo*bar", "foo%bar");
            check("%", "\\%");
            check("_", "\\_");
            check("%s%d_foo*", "\\%s\\%d\\_foo%");
        }

        pub(super) mod key_frag {
            use super::WILDCARD_CHAR;
            use itertools::Itertools;

            /// This constant's value must correspond to the number of "key_frag_#" columns
            /// int the `data-entries` table.
            const MAX_FRAGMENTS: usize = 8;

            /// Split the key pattern into fragments so that each pattern is either a literal string or a wildcard.
            pub fn try_split_key(key_pattern: &str) -> Option<Vec<Option<&str>>> {
                let mut frags = key_pattern
                    .split("__")
                    .map(|s| {
                        if s.contains(WILDCARD_CHAR) {
                            if s.len() == 1 {
                                Ok(None)
                            } else {
                                Err(())
                            }
                        } else {
                            Ok(Some(s))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .ok()?;
                if frags.len() < 2 {
                    return None;
                }
                let frags_descr = frags[0]?;
                let valid_descr = frags_descr
                    .chars()
                    .tuples::<(char, char)>()
                    .all(|(ch1, ch2)| ch1 == '%' && (ch2 == 's' || ch2 == 'd'));
                let valid_descr = valid_descr
                    && frags_descr.len() % 2 == 0
                    && frags_descr.len() / 2 + 1 == frags.len();
                if !valid_descr {
                    return None;
                }
                frags.remove(0);
                if frags.len() <= MAX_FRAGMENTS {
                    Some(frags)
                } else {
                    None
                }
            }

            #[rustfmt::skip]
            #[test]
            fn test_try_split_key() {
                // Negative cases
                assert!(try_split_key("").is_none()); // Empty
                assert!(try_split_key("123").is_none()); // Not fragmented
                assert!(try_split_key("abc_123").is_none()); // Single underscore - not fragmented
                assert!(try_split_key("abc__123").is_none()); // No schema
                assert!(try_split_key("%s%__123").is_none()); // Bad schema
                assert!(try_split_key("%s%d__abc").is_none()); // Schema is 2 frags, but value is 1 frag only

                // Positive cases
                assert!(try_split_key("%s__abc").is_some());
                assert!(try_split_key("%s%d__abc__123").is_some());

                // Patterns
                assert!(try_split_key("%s__*").is_some()); // Ok, pattern is valid
                assert!(try_split_key("%s%d__*__*").is_some()); // Ok, pattern is valid
                assert!(try_split_key("%s%d__a__*").is_some()); // Ok, pattern is valid
                assert!(try_split_key("%s__x*").is_none()); // Bad - pattern doesn't cover whole fragment

                assert_eq!(try_split_key("%s__abc").unwrap(), vec![Some("abc")]);
                assert_eq!(try_split_key("%s__*").unwrap(), vec![None]);
                assert_eq!(try_split_key("%d__123").unwrap(), vec![Some("123")]);
                assert_eq!(try_split_key("%d__*").unwrap(), vec![None]);
                assert_eq!(try_split_key("%d%s__123__abc").unwrap(), vec![Some("123"), Some("abc")]);
                assert_eq!(try_split_key("%d%s__*__abc").unwrap(), vec![None, Some("abc")]);
                assert_eq!(try_split_key("%d%s__123__*").unwrap(), vec![Some("123"), None]);

                // Number of columns
                assert!(try_split_key("%d__*").is_some());
                assert!(try_split_key("%d%d__*__*").is_some());
                assert!(try_split_key("%d%d%d__*__*__*").is_some());
                assert!(try_split_key("%d%d%d%d__*__*__*__*").is_some());
                assert!(try_split_key("%d%d%d%d%d__*__*__*__*__*").is_some());
                assert!(try_split_key("%d%d%d%d%d%d__*__*__*__*__*__*").is_some());
                assert!(try_split_key("%d%d%d%d%d%d%d__*__*__*__*__*__*__*").is_some());
                assert!(try_split_key("%d%d%d%d%d%d%d%d__*__*__*__*__*__*__*__*").is_some());
                assert!(try_split_key("%d%d%d%d%d%d%d%d%d__*__*__*__*__*__*__*__*__*").is_none()); // Too many
            }
        }
    }
}
