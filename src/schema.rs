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
        id -> Varchar,
        block_uid -> BigInt,
        tx_type -> SmallInt,
        body -> Nullable<Jsonb>,
    }
}

table! {
    associated_addresses (transaction_id, address) {
        address -> Varchar,
        transaction_id -> Varchar,
    }
}

table! {
    exchanges (transaction_id) {
        transaction_id -> Varchar,
        amount_asset -> Varchar,
        price_asset -> Varchar,
    }
}

joinable!(associated_addresses -> transactions (transaction_id));
allow_tables_to_appear_in_same_query!(blocks_microblocks, transactions, associated_addresses);
joinable!(exchanges -> transactions (transaction_id));
allow_tables_to_appear_in_same_query!(transactions, exchanges);
