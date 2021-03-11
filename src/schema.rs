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
        tx_type -> SmallInt,
    }
}

table! {
    associated_addresses (address, transaction_id) {
        address -> Varchar,
        transaction_id -> Varchar,
    }
}
