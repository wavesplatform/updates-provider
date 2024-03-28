//! Consumer's database view (read-write operations).

use async_trait::async_trait;

use super::{
    AssociatedAddress, BlockMicroblock, DataEntry, DataEntryUpdate, DeletedDataEntry,
    DeletedLeasingBalance, InsertableTransaction, LeasingBalance, LeasingBalanceUpdate,
    PrevHandledHeight,
};
use crate::error::Result;

pub use self::repo_impl::PostgresConsumerRepo;

#[async_trait]
pub trait ConsumerRepo {
    type Operations: ConsumerRepoOperations;

    /// Execute some operations on a pooled connection without creating a database transaction.
    async fn execute<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::Operations) -> Result<R> + Send + 'static,
        R: Send + 'static;

    /// Execute some operations within a database transaction.
    async fn transaction<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Self::Operations) -> Result<R> + Send + 'static,
        R: Send + 'static;
}

pub trait ConsumerRepoOperations {
    fn get_prev_handled_height(&mut self, depth: u32) -> Result<Option<PrevHandledHeight>>;

    fn get_block_uid(&mut self, block_id: &str) -> Result<i64>;

    fn get_key_block_uid(&mut self) -> Result<i64>;

    fn get_total_block_id(&mut self) -> Result<Option<String>>;

    fn get_next_update_uid(&mut self) -> Result<i64>;

    fn insert_blocks_or_microblocks(&mut self, blocks: &[BlockMicroblock]) -> Result<Vec<i64>>;

    fn insert_transactions(&mut self, transactions: &[InsertableTransaction]) -> Result<()>;

    fn insert_associated_addresses(&mut self, addrs: &[AssociatedAddress]) -> Result<()>;

    fn insert_data_entries(&mut self, entries: &[DataEntry]) -> Result<()>;

    fn close_superseded_by(&mut self, updates: &[DataEntryUpdate]) -> Result<()>;

    fn reopen_superseded_by(&mut self, current_superseded_by: &[i64]) -> Result<()>;

    fn set_next_update_uid(&mut self, uid: i64) -> Result<()>;

    fn change_block_id(&mut self, block_uid: &i64, new_block_id: &str) -> Result<()>;

    fn update_transactions_block_references(&mut self, block_uid: &i64) -> Result<()>;

    fn delete_microblocks(&mut self) -> Result<()>;

    fn rollback_blocks_microblocks(&mut self, block_uid: &i64) -> Result<()>;

    fn rollback_data_entries(&mut self, block_uid: &i64) -> Result<Vec<DeletedDataEntry>>;

    fn update_data_entries_block_references(&mut self, block_uid: &i64) -> Result<()>;

    fn close_lease_superseded_by(&mut self, updates: &[LeasingBalanceUpdate]) -> Result<()>;

    fn reopen_lease_superseded_by(&mut self, current_superseded_by: &[i64]) -> Result<()>;

    fn insert_leasing_balances(&mut self, entries: &[LeasingBalance]) -> Result<()>;

    fn set_next_lease_update_uid(&mut self, new_uid: i64) -> Result<()>;

    fn rollback_leasing_balances(&mut self, block_uid: &i64) -> Result<Vec<DeletedLeasingBalance>>;

    fn update_leasing_balances_block_references(&mut self, block_uid: &i64) -> Result<()>;

    fn get_next_lease_update_uid(&mut self) -> Result<i64>;
}

mod repo_impl {
    use async_trait::async_trait;

    use diesel::prelude::*;
    use diesel::sql_types::{Array, BigInt, Integer, VarChar};

    use super::{
        AssociatedAddress, BlockMicroblock, DataEntry, DataEntryUpdate, DeletedDataEntry,
        DeletedLeasingBalance, LeasingBalance, LeasingBalanceUpdate, PrevHandledHeight,
    };
    use super::{ConsumerRepo, ConsumerRepoOperations};
    use crate::db::pool::{PgPoolWithStats, PooledPgConnection};
    use crate::db::MAX_UID;
    use crate::error::Result;
    use crate::schema::blocks_microblocks::dsl::*;
    use crate::schema::data_entries_uid_seq;
    use crate::schema::data_entries_uid_seq::dsl::*;
    use crate::schema::leasing_balances_uid_seq;
    use crate::schema::leasing_balances_uid_seq::dsl::*;
    use crate::schema::{
        associated_addresses, blocks_microblocks, data_entries, leasing_balances, transactions,
    };
    use crate::utils::chunks::ToChunks;
    use crate::waves::transactions::InsertableTransaction;
    use wavesexchange_log::timer;

    /// Consumer's repo implementation that uses Postgres database as the storage.
    ///
    /// Can be cloned freely, no need to wrap in `Arc`.
    #[derive(Clone)]
    pub struct PostgresConsumerRepo {
        pool: PgPoolWithStats,
    }

    impl PostgresConsumerRepo {
        pub fn new(pool: PgPoolWithStats) -> Self {
            Self { pool }
        }

        async fn get_conn(&self) -> Result<PooledPgConnection> {
            let conn = self.pool.get().await?;
            Ok(conn)
        }
    }

    #[async_trait]
    impl ConsumerRepo for PostgresConsumerRepo {
        type Operations = PgConnection;

        async fn execute<F, R>(&self, f: F) -> Result<R>
        where
            F: FnOnce(&mut PgConnection) -> Result<R> + Send + 'static,
            R: Send + 'static,
        {
            let conn = self.get_conn().await?;
            conn.interact(|conn| f(conn)).await?
        }

        async fn transaction<F, R>(&self, f: F) -> Result<R>
        where
            F: FnOnce(&mut PgConnection) -> Result<R> + Send + 'static,
            R: Send + 'static,
        {
            let conn = self.get_conn().await?;
            conn.interact(|conn| conn.transaction(|conn| f(conn)))
                .await?
        }
    }

    impl ConsumerRepoOperations for PgConnection {
        fn get_prev_handled_height(&mut self, depth: u32) -> Result<Option<PrevHandledHeight>> {
            timer!("get_prev_handled_height()", verbose);

            let sql_height = format!("(select max(height) - {} from blocks_microblocks)", depth);

            Ok(blocks_microblocks
                .select((blocks_microblocks::uid, blocks_microblocks::height))
                .filter(blocks_microblocks::height.eq(diesel::dsl::sql::<Integer>(&sql_height)))
                .order(blocks_microblocks::uid.asc())
                .first(self)
                .optional()?)
        }

        fn get_block_uid(&mut self, block_id: &str) -> Result<i64> {
            timer!("get_block_uid()", verbose);

            Ok(blocks_microblocks
                .select(blocks_microblocks::uid)
                .filter(blocks_microblocks::id.eq(block_id))
                .get_result(self)?)
        }

        fn get_key_block_uid(&mut self) -> Result<i64> {
            timer!("get_key_block_uid()", verbose);

            Ok(blocks_microblocks
                .select(diesel::dsl::sql::<BigInt>("max(uid)"))
                .filter(blocks_microblocks::time_stamp.is_not_null())
                .get_result(self)?)
        }

        fn get_total_block_id(&mut self) -> Result<Option<String>> {
            timer!("get_total_block_id()", verbose);

            Ok(blocks_microblocks
                .select(blocks_microblocks::id)
                .filter(blocks_microblocks::time_stamp.is_null())
                .order(blocks_microblocks::uid.desc())
                .first(self)
                .optional()?)
        }

        fn get_next_update_uid(&mut self) -> Result<i64> {
            timer!("get_next_update_uid()", verbose);

            Ok(data_entries_uid_seq
                .select(data_entries_uid_seq::last_value)
                .first(self)?)
        }

        fn insert_blocks_or_microblocks(&mut self, blocks: &[BlockMicroblock]) -> Result<Vec<i64>> {
            timer!("insert_blocks_or_microblocks()", verbose);

            Ok(diesel::insert_into(blocks_microblocks::table)
                .values(blocks)
                .returning(blocks_microblocks::uid)
                .get_results(self)?)
        }

        fn insert_transactions(&mut self, transactions: &[InsertableTransaction]) -> Result<()> {
            timer!("insert_transactions()", verbose);

            diesel::insert_into(transactions::table)
                .values(transactions)
                .on_conflict_do_nothing()
                .execute(self)?;

            Ok(())
        }

        fn insert_associated_addresses(&mut self, addrs: &[AssociatedAddress]) -> Result<()> {
            timer!("insert_associated_addresses()", verbose);

            diesel::insert_into(associated_addresses::table)
                .values(addrs)
                .on_conflict_do_nothing()
                .execute(self)?;

            Ok(())
        }

        fn insert_data_entries(&mut self, entries: &[DataEntry]) -> Result<()> {
            timer!("insert_data_entries()", verbose);

            // one data entry has 18 columns
            // pg cannot insert more than 65535
            // so the biggest chunk should be less than 3640
            let chunk_size = 3600;
            for chunk in entries.iter().chunks_from_iter(chunk_size) {
                diesel::insert_into(data_entries::table)
                    .values(chunk)
                    .execute(self)?;
            }

            Ok(())
        }

        fn close_superseded_by(&mut self, updates: &[DataEntryUpdate]) -> Result<()> {
            timer!("close_superseded_by()", verbose);

            let mut addresses = vec![];
            let mut keys = vec![];
            let mut superseded_bys = vec![];
            updates.iter().for_each(|u| {
                addresses.push(&u.address);
                keys.push(&u.key);
                superseded_bys.push(&u.superseded_by);
            });

            diesel::sql_query("UPDATE data_entries SET superseded_by = updates.superseded_by FROM (SELECT UNNEST($1) as address, UNNEST($2) as key, UNNEST($3) as superseded_by) as updates where data_entries.address = updates.address and data_entries.key = updates.key and data_entries.superseded_by = $4")
                .bind::<Array<VarChar>, _>(addresses)
                .bind::<Array<VarChar>, _>(keys)
                .bind::<Array<BigInt>, _>(superseded_bys)
                .bind::<BigInt, _>(MAX_UID)
                .execute(self)?;

            Ok(())
        }

        fn reopen_superseded_by(&mut self, current_superseded_by: &[i64]) -> Result<()> {
            timer!("reopen_superseded_by()", verbose);

            diesel::sql_query("UPDATE data_entries SET superseded_by = $1 FROM (SELECT UNNEST($2) AS superseded_by) AS current WHERE data_entries.superseded_by = current.superseded_by;")
                .bind::<BigInt, _>(MAX_UID)
                .bind::<Array<BigInt>, _>(current_superseded_by)
                .execute(self)?;

            Ok(())
        }

        fn set_next_update_uid(&mut self, new_uid: i64) -> Result<()> {
            timer!("set_next_update_uid()", verbose);

            Ok(diesel::sql_query(format!(
                "select setval('data_entries_uid_seq', {}, false);", // 3rd param - is called; in case of true, value'll be incremented before returning
                new_uid
            ))
            .execute(self)
            .map(|_| ())?)
        }

        fn change_block_id(&mut self, block_uid: &i64, new_block_id: &str) -> Result<()> {
            timer!("change_block_id()", verbose);

            Ok(diesel::update(blocks_microblocks::table)
                .set(blocks_microblocks::id.eq(new_block_id))
                .filter(blocks_microblocks::uid.eq(block_uid))
                .execute(self)
                .map(|_| ())?)
        }

        fn update_transactions_block_references(&mut self, block_uid: &i64) -> Result<()> {
            timer!("update_transactions_block_references()", verbose);

            diesel::update(transactions::table)
                .set(transactions::block_uid.eq(block_uid))
                .filter(transactions::block_uid.gt(block_uid))
                .execute(self)?;

            Ok(())
        }

        fn delete_microblocks(&mut self) -> Result<()> {
            timer!("delete_microblocks()", verbose);

            Ok(diesel::delete(blocks_microblocks::table)
                .filter(blocks_microblocks::time_stamp.is_null())
                .execute(self)
                .map(|_| ())?)
        }

        fn rollback_blocks_microblocks(&mut self, block_uid: &i64) -> Result<()> {
            timer!("rollback_blocks_microblocks()", verbose);

            Ok(diesel::delete(blocks_microblocks::table)
                .filter(blocks_microblocks::uid.gt(block_uid))
                .execute(self)
                .map(|_| ())?)
        }

        fn rollback_data_entries(&mut self, block_uid: &i64) -> Result<Vec<DeletedDataEntry>> {
            timer!("rollback_data_entries()", verbose);

            Ok(diesel::delete(data_entries::table)
                .filter(data_entries::block_uid.gt(block_uid))
                .returning((data_entries::address, data_entries::key, data_entries::uid))
                .get_results(self)
                .map(|des| {
                    des.into_iter()
                        .map(|(de_address, de_key, de_uid)| DeletedDataEntry {
                            address: de_address,
                            key: de_key,
                            uid: de_uid,
                        })
                        .collect::<Vec<_>>()
                })?)
        }

        fn update_data_entries_block_references(&mut self, block_uid: &i64) -> Result<()> {
            timer!("update_data_entries_block_references()", verbose);

            diesel::update(data_entries::table)
                .set(data_entries::block_uid.eq(block_uid))
                .filter(data_entries::block_uid.gt(block_uid))
                .execute(self)?;

            Ok(())
        }

        fn close_lease_superseded_by(&mut self, updates: &[LeasingBalanceUpdate]) -> Result<()> {
            timer!("close_lease_superseded_by()", verbose);

            let mut addresses = vec![];
            let mut superseded_bys = vec![];
            updates.iter().for_each(|u| {
                addresses.push(&u.address);
                superseded_bys.push(&u.superseded_by);
            });

            diesel::sql_query("UPDATE leasing_balances SET superseded_by = updates.superseded_by FROM (SELECT UNNEST($1) as address, UNNEST($2) as superseded_by) as updates where leasing_balances.address = updates.address and leasing_balances.superseded_by = $3")
                .bind::<Array<VarChar>, _>(addresses)
                .bind::<Array<BigInt>, _>(superseded_bys)
                .bind::<BigInt, _>(MAX_UID)
                .execute(self)?;

            Ok(())
        }

        fn reopen_lease_superseded_by(&mut self, current_superseded_by: &[i64]) -> Result<()> {
            timer!("reopen_lease_superseded_by()", verbose);

            diesel::sql_query("UPDATE leasing_balances SET superseded_by = $1 FROM (SELECT UNNEST($2) AS superseded_by) AS current WHERE leasing_balances.superseded_by = current.superseded_by;")
                .bind::<BigInt, _>(MAX_UID)
                .bind::<Array<BigInt>, _>(current_superseded_by)
                .execute(self)?;

            Ok(())
        }

        fn insert_leasing_balances(&mut self, entries: &[LeasingBalance]) -> Result<()> {
            timer!("insert_leasing_balances()", verbose);

            // one leasing balance entry has 6 columns
            // pg cannot insert more than 65535
            // so the biggest chunk should be less than 10922
            let chunk_size = 10000;
            for chunk in entries.iter().chunks_from_iter(chunk_size) {
                diesel::insert_into(leasing_balances::table)
                    .values(chunk)
                    .execute(self)?;
            }

            Ok(())
        }

        fn set_next_lease_update_uid(&mut self, new_uid: i64) -> Result<()> {
            timer!("set_next_lease_update_uid()", verbose);

            Ok(diesel::sql_query(format!(
                "select setval('leasing_balances_uid_seq', {}, false);", // 3rd param - is called; in case of true, value'll be incremented before returning
                new_uid
            ))
            .execute(self)
            .map(|_| ())?)
        }

        fn rollback_leasing_balances(
            &mut self,
            block_uid: &i64,
        ) -> Result<Vec<DeletedLeasingBalance>> {
            timer!("rollback_leasing_balances()", verbose);

            Ok(diesel::delete(leasing_balances::table)
                .filter(leasing_balances::block_uid.gt(block_uid))
                .returning((leasing_balances::address, leasing_balances::uid))
                .get_results(self)
                .map(|des| {
                    des.into_iter()
                        .map(|(de_address, de_uid)| DeletedLeasingBalance {
                            address: de_address,
                            uid: de_uid,
                        })
                        .collect::<Vec<_>>()
                })?)
        }

        fn update_leasing_balances_block_references(&mut self, block_uid: &i64) -> Result<()> {
            timer!("update_leasing_balances_block_references()", verbose);

            diesel::update(leasing_balances::table)
                .set(leasing_balances::block_uid.eq(block_uid))
                .filter(leasing_balances::block_uid.gt(block_uid))
                .execute(self)?;

            Ok(())
        }

        fn get_next_lease_update_uid(&mut self) -> Result<i64> {
            timer!("get_next_lease_update_uid()", verbose);

            Ok(leasing_balances_uid_seq
                .select(leasing_balances_uid_seq::last_value)
                .first(self)?)
        }
    }
}
