use super::{
    AssociatedAddress, BlockMicroblock, DataEntryUpdate, DeletedDataEntry, DeletedLeasingBalance,
    InsertableDataEntry, InsertableLeasingBalance, LeasingBalanceUpdate, PrevHandledHeight,
    Transaction, TransactionType, TransactionsRepo, TransactionsRepoPool,
};
use crate::error::Result;
use crate::schema::blocks_microblocks::dsl::*;
use crate::schema::data_entries_uid_seq;
use crate::schema::data_entries_uid_seq::dsl::*;
use crate::schema::leasing_balances_uid_seq;
use crate::schema::leasing_balances_uid_seq::dsl::*;
use crate::schema::{
    associated_addresses, blocks_microblocks, data_entries, leasing_balances, transactions,
};
use crate::{db::PgPool, utils::ToChunks};
use diesel::sql_types::{Array, BigInt, VarChar};
use diesel::{prelude::*, r2d2::ConnectionManager};
use r2d2::PooledConnection;

const MAX_UID: i64 = std::i64::MAX - 1;

#[derive(Clone)]
pub struct TransactionsRepoPoolImpl {
    pool: PgPool,
}

impl TransactionsRepoPoolImpl {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn get_conn(&self) -> Result<PooledConnection<ConnectionManager<PgConnection>>> {
        Ok(self.pool.get()?)
    }
}

impl TransactionsRepoPool for TransactionsRepoPoolImpl {
    fn transaction(&self, f: impl FnOnce(&dyn TransactionsRepo) -> Result<()>) -> Result<()> {
        let conn = self.get_conn()?;
        f(&conn)
    }
}

impl TransactionsRepo for TransactionsRepoPoolImpl {
    fn get_prev_handled_height(&self) -> Result<Option<PrevHandledHeight>> {
        self.get_conn()?.get_prev_handled_height()
    }

    fn get_block_uid(&self, block_id: &str) -> Result<i64> {
        self.get_conn()?.get_block_uid(block_id)
    }

    fn get_key_block_uid(&self) -> Result<i64> {
        self.get_conn()?.get_key_block_uid()
    }

    fn get_total_block_id(&self) -> Result<Option<String>> {
        self.get_conn()?.get_total_block_id()
    }

    fn get_next_update_uid(&self) -> Result<i64> {
        self.get_conn()?.get_next_update_uid()
    }

    fn insert_blocks_or_microblocks(&self, blocks: &[BlockMicroblock]) -> Result<Vec<i64>> {
        self.get_conn()?.insert_blocks_or_microblocks(blocks)
    }

    fn insert_transactions(&self, transactions: &[Transaction]) -> Result<()> {
        self.get_conn()?.insert_transactions(transactions)
    }

    fn insert_associated_addresses(
        &self,
        associated_addresses: &[AssociatedAddress],
    ) -> Result<()> {
        self.get_conn()?
            .insert_associated_addresses(associated_addresses)
    }

    fn insert_data_entries(&self, entries: &[InsertableDataEntry]) -> Result<()> {
        self.get_conn()?.insert_data_entries(entries)
    }

    fn close_superseded_by(&self, updates: &[DataEntryUpdate]) -> Result<()> {
        self.get_conn()?.close_superseded_by(updates)
    }

    fn reopen_superseded_by(&self, current_superseded_by: &[i64]) -> Result<()> {
        self.get_conn()?.reopen_superseded_by(current_superseded_by)
    }

    fn set_next_update_uid(&self, new_uid: i64) -> Result<()> {
        self.get_conn()?.set_next_update_uid(new_uid)
    }

    fn change_block_id(&self, block_uid: &i64, new_block_id: &str) -> Result<()> {
        self.get_conn()?.change_block_id(block_uid, new_block_id)
    }

    fn update_transactions_block_references(&self, block_uid: &i64) -> Result<()> {
        self.get_conn()?
            .update_transactions_block_references(block_uid)
    }

    fn delete_microblocks(&self) -> Result<()> {
        self.get_conn()?.delete_microblocks()
    }

    fn rollback_blocks_microblocks(&self, block_uid: &i64) -> Result<()> {
        self.get_conn()?.rollback_blocks_microblocks(block_uid)
    }

    fn rollback_data_entries(&self, block_uid: &i64) -> Result<Vec<DeletedDataEntry>> {
        self.get_conn()?.rollback_data_entries(block_uid)
    }

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

    fn last_data_entry(&self, address: String, key: String) -> Result<Option<InsertableDataEntry>> {
        self.get_conn()?.last_data_entry(address, key)
    }

    fn update_data_entries_block_references(&self, block_uid: &i64) -> Result<()> {
        self.get_conn()?
            .update_data_entries_block_references(block_uid)
    }

    fn get_next_lease_update_uid(&self) -> Result<i64> {
        self.get_conn()?.get_next_lease_update_uid()
    }

    fn insert_leasing_balances(&self, entries: &[InsertableLeasingBalance]) -> Result<()> {
        self.get_conn()?.insert_leasing_balances(entries)
    }

    fn set_next_lease_update_uid(&self, new_uid: i64) -> Result<()> {
        self.get_conn()?.set_next_lease_update_uid(new_uid)
    }

    fn close_lease_superseded_by(&self, updates: &[LeasingBalanceUpdate]) -> Result<()> {
        self.get_conn()?.close_lease_superseded_by(updates)
    }

    fn reopen_lease_superseded_by(&self, current_superseded_by: &[i64]) -> Result<()> {
        self.get_conn()?
            .reopen_lease_superseded_by(current_superseded_by)
    }

    fn rollback_leasing_balances(&self, block_uid: &i64) -> Result<Vec<DeletedLeasingBalance>> {
        self.get_conn()?.rollback_leasing_balances(block_uid)
    }

    fn update_leasing_balances_block_references(&self, block_uid: &i64) -> Result<()> {
        self.get_conn()?
            .update_leasing_balances_block_references(block_uid)
    }

    fn last_leasing_balance(&self, address: String) -> Result<Option<InsertableLeasingBalance>> {
        self.get_conn()?.last_leasing_balance(address)
    }
}

impl TransactionsRepo for PooledConnection<ConnectionManager<PgConnection>> {
    fn get_prev_handled_height(&self) -> Result<Option<PrevHandledHeight>> {
        Ok(blocks_microblocks
            .select((blocks_microblocks::uid, blocks_microblocks::height))
            .filter(
                blocks_microblocks::height.eq(diesel::expression::sql_literal::sql(
                    "(select max(height) - 1 from blocks_microblocks)",
                )),
            )
            .order(blocks_microblocks::uid.asc())
            .first(self)
            .optional()?)
    }

    fn get_block_uid(&self, block_id: &str) -> Result<i64> {
        Ok(blocks_microblocks
            .select(blocks_microblocks::uid)
            .filter(blocks_microblocks::id.eq(block_id))
            .get_result(self)?)
    }

    fn get_key_block_uid(&self) -> Result<i64> {
        Ok(blocks_microblocks
            .select(diesel::expression::sql_literal::sql("max(uid)"))
            .filter(blocks_microblocks::time_stamp.is_not_null())
            .get_result(self)?)
    }

    fn get_total_block_id(&self) -> Result<Option<String>> {
        Ok(blocks_microblocks
            .select(blocks_microblocks::id)
            .filter(blocks_microblocks::time_stamp.is_null())
            .order(blocks_microblocks::uid.desc())
            .first(self)
            .optional()?)
    }

    fn get_next_update_uid(&self) -> Result<i64> {
        Ok(data_entries_uid_seq
            .select(data_entries_uid_seq::last_value)
            .first(self)?)
    }

    fn insert_blocks_or_microblocks(&self, blocks: &[BlockMicroblock]) -> Result<Vec<i64>> {
        Ok(diesel::insert_into(blocks_microblocks::table)
            .values(blocks)
            .returning(blocks_microblocks::uid)
            .get_results(self)?)
    }

    fn insert_transactions(&self, transactions: &[Transaction]) -> Result<()> {
        diesel::insert_into(transactions::table)
            .values(transactions)
            .on_conflict_do_nothing()
            .execute(self)?;
        Ok(())
    }

    fn insert_associated_addresses(
        &self,
        associated_addresses: &[AssociatedAddress],
    ) -> Result<()> {
        diesel::insert_into(associated_addresses::table)
            .values(associated_addresses)
            .on_conflict_do_nothing()
            .execute(self)?;
        Ok(())
    }

    fn insert_data_entries(&self, entries: &[InsertableDataEntry]) -> Result<()> {
        // one data entry has 10 columns
        // pg cannot insert more then 65535
        // so the biggest chunk should be less then 6553
        let chunk_size = 6500;
        for chunk in entries.iter().chunks_from_iter(chunk_size) {
            diesel::insert_into(data_entries::table)
                .values(chunk)
                .execute(self)?;
        }
        Ok(())
    }

    fn close_superseded_by(&self, updates: &[DataEntryUpdate]) -> Result<()> {
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

    fn reopen_superseded_by(&self, current_superseded_by: &[i64]) -> Result<()> {
        diesel::sql_query("UPDATE data_entries SET superseded_by = $1 FROM (SELECT UNNEST($2) AS superseded_by) AS current WHERE data_entries.superseded_by = current.superseded_by;")
            .bind::<BigInt, _>(MAX_UID)
            .bind::<Array<BigInt>, _>(current_superseded_by)
            .execute(self)?;

        Ok(())
    }

    fn set_next_update_uid(&self, new_uid: i64) -> Result<()> {
        Ok(diesel::sql_query(format!(
            "select setval('data_entries_uid_seq', {}, false);", // 3rd param - is called; in case of true, value'll be incremented before returning
            new_uid
        ))
        .execute(self)
        .map(|_| ())?)
    }

    fn change_block_id(&self, block_uid: &i64, new_block_id: &str) -> Result<()> {
        Ok(diesel::update(blocks_microblocks::table)
            .set(blocks_microblocks::id.eq(new_block_id))
            .filter(blocks_microblocks::uid.eq(block_uid))
            .execute(self)
            .map(|_| ())?)
    }

    fn update_transactions_block_references(&self, block_uid: &i64) -> Result<()> {
        diesel::update(transactions::table)
            .set(transactions::block_uid.eq(block_uid))
            .filter(transactions::block_uid.gt(block_uid))
            .execute(self)?;
        Ok(())
    }

    fn delete_microblocks(&self) -> Result<()> {
        Ok(diesel::delete(blocks_microblocks::table)
            .filter(blocks_microblocks::time_stamp.is_null())
            .execute(self)
            .map(|_| ())?)
    }

    fn rollback_blocks_microblocks(&self, block_uid: &i64) -> Result<()> {
        Ok(diesel::delete(blocks_microblocks::table)
            .filter(blocks_microblocks::uid.gt(block_uid))
            .execute(self)
            .map(|_| ())?)
    }

    fn rollback_data_entries(&self, block_uid: &i64) -> Result<Vec<DeletedDataEntry>> {
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

    fn last_transaction_by_address(&self, address: String) -> Result<Option<Transaction>> {
        Ok(associated_addresses::table
            .inner_join(transactions::table)
            .filter(associated_addresses::address.eq(address))
            .select(transactions::all_columns.nullable())
            .order(transactions::block_uid.desc())
            .first::<Option<Transaction>>(self)
            .optional()?
            .flatten())
    }

    fn last_transaction_by_address_and_type(
        &self,
        address: String,
        transaction_type: TransactionType,
    ) -> Result<Option<Transaction>> {
        Ok(associated_addresses::table
            .inner_join(transactions::table)
            .filter(associated_addresses::address.eq(address))
            .filter(transactions::tx_type.eq(transaction_type))
            .select(transactions::all_columns.nullable())
            .order(transactions::block_uid.desc())
            .first::<Option<Transaction>>(self)
            .optional()?
            .flatten())
    }

    fn last_exchange_transaction(
        &self,
        amount_asset: String,
        price_asset: String,
    ) -> Result<Option<Transaction>> {
        Ok(transactions::table
            .filter(transactions::exchange_amount_asset.eq(amount_asset))
            .filter(transactions::exchange_price_asset.eq(price_asset))
            .select(transactions::all_columns.nullable())
            .order(transactions::block_uid.desc())
            .first::<Option<Transaction>>(self)
            .optional()?
            .flatten())
    }

    fn last_data_entry(&self, address: String, key: String) -> Result<Option<InsertableDataEntry>> {
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
            .order(data_entries::block_uid.desc())
            .first::<Option<InsertableDataEntry>>(self)
            .optional()?
            .flatten())
    }

    fn update_data_entries_block_references(&self, block_uid: &i64) -> Result<()> {
        diesel::update(data_entries::table)
            .set(data_entries::block_uid.eq(block_uid))
            .filter(data_entries::block_uid.gt(block_uid))
            .execute(self)?;
        Ok(())
    }

    fn get_next_lease_update_uid(&self) -> Result<i64> {
        Ok(leasing_balances_uid_seq
            .select(leasing_balances_uid_seq::last_value)
            .first(self)?)
    }

    fn insert_leasing_balances(&self, entries: &[InsertableLeasingBalance]) -> Result<()> {
        // one data entry has 6 columns
        // pg cannot insert more then 65535
        // so the biggest chunk should be less then 10922
        let chunk_size = 10000;
        for chunk in entries.iter().chunks_from_iter(chunk_size) {
            diesel::insert_into(leasing_balances::table)
                .values(chunk)
                .execute(self)?;
        }
        Ok(())
    }

    fn close_lease_superseded_by(&self, updates: &[LeasingBalanceUpdate]) -> Result<()> {
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

    fn reopen_lease_superseded_by(&self, current_superseded_by: &[i64]) -> Result<()> {
        diesel::sql_query("UPDATE leasing_balances SET superseded_by = $1 FROM (SELECT UNNEST($2) AS superseded_by) AS current WHERE leasing_balances.superseded_by = current.superseded_by;")
            .bind::<BigInt, _>(MAX_UID)
            .bind::<Array<BigInt>, _>(current_superseded_by)
            .execute(self)?;

        Ok(())
    }

    fn set_next_lease_update_uid(&self, new_uid: i64) -> Result<()> {
        Ok(diesel::sql_query(format!(
            "select setval('leasing_balances_uid_seq', {}, false);", // 3rd param - is called; in case of true, value'll be incremented before returning
            new_uid
        ))
        .execute(self)
        .map(|_| ())?)
    }

    fn rollback_leasing_balances(&self, block_uid: &i64) -> Result<Vec<DeletedLeasingBalance>> {
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

    fn update_leasing_balances_block_references(&self, block_uid: &i64) -> Result<()> {
        diesel::update(leasing_balances::table)
            .set(leasing_balances::block_uid.eq(block_uid))
            .filter(leasing_balances::block_uid.gt(block_uid))
            .execute(self)?;
        Ok(())
    }

    fn last_leasing_balance(&self, address: String) -> Result<Option<InsertableLeasingBalance>> {
        Ok(leasing_balances::table
            .filter(leasing_balances::address.eq(address))
            .select(leasing_balances::all_columns.nullable())
            .order(leasing_balances::block_uid.desc())
            .first::<Option<InsertableLeasingBalance>>(self)
            .optional()?
            .flatten())
    }
}
