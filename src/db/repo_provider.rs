//! Provider's database view (read-only operations).

use super::{DataEntry, LeasingBalance};
use crate::error::Result;
use crate::waves::transactions::{Transaction, TransactionType};
use wavesexchange_topic::StateSingle;

pub use self::repo_impl::PostgresProviderRepo;

pub trait ProviderRepo {
    fn last_transaction_by_address(&self, address: String) -> Result<Option<Transaction>>;

    fn last_transaction_by_address_and_type(
        &self,
        address: String,
        transaction_type: TransactionType,
    ) -> Result<Option<Transaction>>;

    fn last_exchange_transaction(
        &self,
        amount_asset: String,
        price_asset: String,
    ) -> Result<Option<Transaction>>;

    fn last_leasing_balance(&self, address: String) -> Result<Option<LeasingBalance>>;

    fn last_data_entry(&self, address: String, key: String) -> Result<Option<DataEntry>>;

    fn find_matching_data_keys(
        &self,
        addresses: Vec<String>,
        key_patterns: Vec<String>,
    ) -> Result<Vec<StateSingle>>;
}

mod repo_impl {
    use diesel::dsl::any;
    use diesel::{prelude::*, r2d2::ConnectionManager};
    use r2d2::PooledConnection;

    use super::ProviderRepo;
    use crate::db::pool::PgPool;
    use crate::db::{DataEntry, LeasingBalance};
    use crate::error::Result;
    use crate::schema::{associated_addresses, data_entries, leasing_balances, transactions};
    use crate::waves::transactions::{Transaction, TransactionType};
    use wavesexchange_log::{debug, timer};
    use wavesexchange_topic::StateSingle;

    const MAX_UID: i64 = i64::MAX - 1;

    /// Provider's repo implementation that uses Postgres database as the storage.
    ///
    /// Can be cloned freely, no need to wrap in `Arc`.
    #[derive(Clone)]
    pub struct PostgresProviderRepo {
        pool: PgPool,
    }

    impl PostgresProviderRepo {
        pub fn new(pool: PgPool) -> Self {
            PostgresProviderRepo { pool }
        }

        fn get_conn(&self) -> Result<PooledConnection<ConnectionManager<PgConnection>>> {
            Ok(self.pool.get()?)
        }
    }

    impl ProviderRepo for PostgresProviderRepo {
        fn last_transaction_by_address(&self, address: String) -> Result<Option<Transaction>> {
            self.get_conn()?.last_transaction_by_address(address)
        }

        fn last_transaction_by_address_and_type(
            &self,
            address: String,
            transaction_type: TransactionType,
        ) -> Result<Option<Transaction>> {
            self.get_conn()?
                .last_transaction_by_address_and_type(address, transaction_type)
        }

        fn last_exchange_transaction(
            &self,
            amount_asset: String,
            price_asset: String,
        ) -> Result<Option<Transaction>> {
            self.get_conn()?
                .last_exchange_transaction(amount_asset, price_asset)
        }

        fn last_leasing_balance(&self, address: String) -> Result<Option<LeasingBalance>> {
            self.get_conn()?.last_leasing_balance(address)
        }

        fn last_data_entry(&self, address: String, key: String) -> Result<Option<DataEntry>> {
            self.get_conn()?.last_data_entry(address, key)
        }

        fn find_matching_data_keys(
            &self,
            addresses: Vec<String>,
            key_patterns: Vec<String>,
        ) -> Result<Vec<StateSingle>> {
            self.get_conn()?
                .find_matching_data_keys(addresses, key_patterns)
        }
    }

    impl ProviderRepo for PooledConnection<ConnectionManager<PgConnection>> {
        fn last_transaction_by_address(&self, address: String) -> Result<Option<Transaction>> {
            timer!("last_transaction_by_address()", verbose);

            Ok(associated_addresses::table
                .inner_join(transactions::table)
                .filter(associated_addresses::address.eq(address))
                .select(transactions::all_columns.nullable())
                .order(transactions::uid.desc())
                .first::<Option<Transaction>>(self)
                .optional()?
                .flatten())
        }

        fn last_transaction_by_address_and_type(
            &self,
            address: String,
            transaction_type: TransactionType,
        ) -> Result<Option<Transaction>> {
            timer!("last_transaction_by_address_and_type()", verbose);

            Ok(associated_addresses::table
                .inner_join(transactions::table)
                .filter(associated_addresses::address.eq(address))
                .filter(transactions::tx_type.eq(transaction_type))
                .select(transactions::all_columns.nullable())
                .order(transactions::uid.desc())
                .first::<Option<Transaction>>(self)
                .optional()?
                .flatten())
        }

        fn last_exchange_transaction(
            &self,
            amount_asset: String,
            price_asset: String,
        ) -> Result<Option<Transaction>> {
            timer!("last_exchange_transaction()", verbose);

            Ok(transactions::table
                .filter(transactions::exchange_amount_asset.eq(amount_asset))
                .filter(transactions::exchange_price_asset.eq(price_asset))
                .select(transactions::all_columns.nullable())
                .order(transactions::uid.desc())
                .first::<Option<Transaction>>(self)
                .optional()?
                .flatten())
        }

        fn last_leasing_balance(&self, address: String) -> Result<Option<LeasingBalance>> {
            timer!("last_leasing_balance()", verbose);

            Ok(leasing_balances::table
                .filter(leasing_balances::address.eq(address))
                .select(leasing_balances::all_columns.nullable())
                .filter(leasing_balances::superseded_by.eq(MAX_UID))
                .first::<Option<LeasingBalance>>(self)
                .optional()?
                .flatten())
        }

        fn last_data_entry(&self, address: String, key: String) -> Result<Option<DataEntry>> {
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
                .first::<Option<DataEntry>>(self)
                .optional()?
                .flatten())
        }

        fn find_matching_data_keys(
            &self,
            addresses: Vec<String>,
            key_patterns: Vec<String>,
        ) -> Result<Vec<StateSingle>> {
            timer!("find_matching_data_keys()", verbose);

            let start_time = std::time::Instant::now();
            let (n_addr, n_patt) = (addresses.len(), key_patterns.len());
            let key_likes = key_patterns
                .iter()
                .map(String::as_str)
                .map(pattern_utils::pattern_to_sql_like)
                .collect::<Vec<_>>();
            let res: Vec<(String, String)> = data_entries::table
                .filter(data_entries::address.eq(any(addresses)))
                .filter(data_entries::key.like(any(key_likes)))
                .filter(data_entries::superseded_by.eq(MAX_UID))
                .select((data_entries::address, data_entries::key))
                .order((data_entries::address, data_entries::key))
                .load(self)?;
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
        }
    }

    mod pattern_utils {
        use itertools::Itertools;
        use std::borrow::Cow;

        pub(super) fn pattern_to_sql_like(pattern: &str) -> String {
            const WILDCARD_CHAR: char = '*';
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
