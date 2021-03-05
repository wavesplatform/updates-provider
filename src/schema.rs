table! {
    blocks_microblocks (id) {
        uid -> BigInt,
        id -> Varchar,
        height -> Int4,
        time_stamp -> Nullable<BigInt>,
    }
}

table! {
    transactions (id) {
        block_uid -> BigInt,
        id -> Varchar,
    }
}

table! {
    recipients (uid) {
        uid -> BigInt,
        address -> Varchar,
        transaction_id -> Varchar,
    }
}
