use super::{BlockMicroblock, PrevHandledHeight, TransactionsRepo};
use crate::error::Result;
use crate::schema::blocks_microblocks;
use crate::schema::blocks_microblocks::dsl::*;
use crate::schema::transactions;
use diesel::prelude::*;
use diesel::PgConnection;

// const MAX_UID: i64 = std::i64::MAX - 1;

pub struct TransactionsRepoImpl {
    conn: PgConnection,
}

impl TransactionsRepoImpl {
    pub fn new(conn: PgConnection) -> TransactionsRepoImpl {
        TransactionsRepoImpl { conn }
    }
}

impl TransactionsRepo for TransactionsRepoImpl {
    fn transaction(&self, f: impl FnOnce() -> Result<()>) -> Result<()> {
        self.conn.transaction(|| f())
    }

    fn get_prev_handled_height(&self) -> Result<Option<PrevHandledHeight>> {
        Ok(blocks_microblocks
            .select((blocks_microblocks::uid, blocks_microblocks::height))
            .filter(
                blocks_microblocks::height.eq(diesel::expression::sql_literal::sql(
                    "(select max(height) - 1 from blocks_microblocks)",
                )),
            )
            .order(blocks_microblocks::uid.asc())
            .first(&self.conn)
            .optional()?)
    }

    fn get_block_uid(&self, block_id: &str) -> Result<i64> {
        Ok(blocks_microblocks
            .select(blocks_microblocks::uid)
            .filter(blocks_microblocks::id.eq(block_id))
            .get_result(&self.conn)?)
    }

    fn get_key_block_uid(&self) -> Result<i64> {
        Ok(blocks_microblocks
            .select(diesel::expression::sql_literal::sql("max(uid)"))
            .filter(blocks_microblocks::time_stamp.is_not_null())
            .get_result(&self.conn)?)
    }

    fn get_total_block_id(&self) -> Result<Option<String>> {
        Ok(blocks_microblocks
            .select(blocks_microblocks::id)
            .filter(blocks_microblocks::time_stamp.is_null())
            .order(blocks_microblocks::uid.desc())
            .first(&self.conn)
            .optional()?)
    }

    // fn get_next_update_uid(&self) -> Result<i64> {
    //     Ok(data_entries_uid_seq
    //         .select(data_entries_uid_seq::last_value)
    //         .first(&self.conn)?)
    // }

    fn insert_blocks_or_microblocks(&self, blocks: &Vec<BlockMicroblock>) -> Result<Vec<i64>> {
        Ok(diesel::insert_into(blocks_microblocks::table)
            .values(blocks)
            .returning(blocks_microblocks::uid)
            .get_results(&self.conn)?)
    }

    // fn insert_data_entries(&self, entries: &Vec<InsertableDataEntry>) -> Result<()> {
    //     // one data entry has 29 columns
    //     // pg cannot insert more then 65535
    //     // so the biggest chunk should be less then 2259
    //     let chunk_size = 2000;
    //     entries
    //         .to_owned()
    //         .chunks(chunk_size)
    //         .into_iter()
    //         .try_fold((), |_, chunk| {
    //             diesel::insert_into(data_entries::table)
    //                 .values(chunk)
    //                 .execute(&self.conn)
    //                 .map(|_| ())
    //                 .map_err(|err| Error::new(AppError::DbError(err)))
    //         })
    // }

    // fn close_superseded_by(&self, updates: &Vec<DataEntryUpdate>) -> Result<()> {
    //     let mut addresses = vec![];
    //     let mut keys = vec![];
    //     let mut superseded_bys = vec![];
    //     updates.iter().for_each(|u| {
    //         addresses.push(&u.address);
    //         keys.push(&u.key);
    //         superseded_bys.push(&u.superseded_by);
    //     });

    //     diesel::sql_query("UPDATE data_entries SET superseded_by = updates.superseded_by FROM (SELECT UNNEST($1) as address, UNNEST($2) as key, UNNEST($3) as superseded_by) as updates where data_entries.address = updates.address and data_entries.key = updates.key and data_entries.superseded_by = $4")
    //             .bind::<Array<VarChar>, _>(addresses)
    //             .bind::<Array<VarChar>, _>(keys)
    //             .bind::<Array<BigInt>, _>(superseded_bys)
    //             .bind::<BigInt, _>(MAX_UID)
    //         .execute(&self.conn)
    //         .map(|_| ())
    //         .map_err(|err| Error::new(AppError::DbError(err)))
    // }

    // fn reopen_superseded_by(&self, current_superseded_by: &Vec<i64>) -> Result<()> {
    //     diesel::sql_query("UPDATE data_entries SET superseded_by = $1 FROM (SELECT UNNEST($2) AS superseded_by) AS current WHERE data_entries.superseded_by = current.superseded_by;")
    //         .bind::<BigInt, _>(MAX_UID)
    //         .bind::<Array<BigInt>, _>(current_superseded_by)
    //         .execute(&self.conn)
    //         .map(|_| ())
    //         .map_err(|err| Error::new(AppError::DbError(err)))
    // }

    // fn set_next_update_uid(&self, new_uid: i64) -> Result<()> {
    //     Ok(diesel::sql_query(format!(
    //         "select setval('data_entries_uid_seq', {}, false);", // 3rd param - is called; in case of true, value'll be incremented before returning
    //         new_uid
    //     ))
    //     .execute(&self.conn)
    //     .map(|_| ())?)
    // }

    fn change_block_id(&self, block_uid: &i64, new_block_id: &str) -> Result<()> {
        Ok(diesel::update(blocks_microblocks::table)
            .set(blocks_microblocks::id.eq(new_block_id))
            .filter(blocks_microblocks::uid.eq(block_uid))
            .execute(&self.conn)
            .map(|_| ())?)
    }

    fn update_transactions_block_references(&self, block_uid: &i64) -> Result<()> {
        Ok(diesel::update(transactions::table)
            .set(transactions::block_uid.eq(block_uid))
            .filter(transactions::block_uid.gt(block_uid))
            .execute(&self.conn)
            .map(|_| ())?)
    }

    fn delete_microblocks(&self) -> Result<()> {
        Ok(diesel::delete(blocks_microblocks::table)
            .filter(blocks_microblocks::time_stamp.is_null())
            .execute(&self.conn)
            .map(|_| ())?)
    }

    fn rollback_blocks_microblocks(&self, block_uid: &i64) -> Result<()> {
        Ok(diesel::delete(blocks_microblocks::table)
            .filter(blocks_microblocks::uid.gt(block_uid))
            .execute(&self.conn)
            .map(|_| ())?)
    }

    // fn rollback_data_entries(&self, block_uid: &i64) -> Result<Vec<DeletedDataEntry>> {
    //     diesel::delete(data_entries::table)
    //         .filter(data_entries::block_uid.gt(block_uid))
    //         .returning((data_entries::address, data_entries::key, data_entries::uid))
    //         .get_results(&self.conn)
    //         .map(|des| {
    //             des.into_iter()
    //                 .map(|(de_address, de_key, de_uid)| DeletedDataEntry {
    //                     address: de_address,
    //                     key: de_key,
    //                     uid: de_uid,
    //                 })
    //                 .collect()
    //         })
    //         .map_err(|err| Error::new(AppError::DbError(err)))
    // }
}
