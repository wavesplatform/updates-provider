//! Provider's database view (read-only operations).

use super::{DataEntry, LeasingBalance};
use crate::error::Result;
use crate::waves::transactions::{Transaction, TransactionType};
use async_trait::async_trait;
use wx_topic::StateSingle;

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

    async fn find_matching_data_keys(
        &self,
        addresses: Vec<String>,
        key_patterns: Vec<String>,
    ) -> Result<Vec<StateSingle>>;
}

mod repo_impl {
    use diesel::dsl::any;
    use diesel::prelude::*;

    use super::ProviderRepo;
    use crate::db::pool::{PgPoolWithStats, PooledPgConnection};
    use crate::db::{DataEntry, LeasingBalance};
    use crate::error::Result;
    use crate::schema::{associated_addresses, data_entries, leasing_balances, transactions};
    use crate::waves::transactions::{Transaction, TransactionType};
    use async_trait::async_trait;
    use wavesexchange_log::{debug, timer};
    use wx_topic::StateSingle;

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

        async fn find_matching_data_keys(
            &self,
            addresses: Vec<String>,
            key_patterns: Vec<String>,
        ) -> Result<Vec<StateSingle>> {
            self.interact(|conn| {
                timer!("find_matching_data_keys()", verbose);

                let start_time = std::time::Instant::now();
                let (n_addr, n_patt) = (addresses.len(), key_patterns.len());
                let has_patterns = key_patterns.iter().any(|s| pattern_utils::has_patterns(s));
                let res: Vec<(String, String)> = if has_patterns {
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
    }
}
