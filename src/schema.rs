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
        exchange_amount_asset -> Nullable<Varchar>,
        exchange_price_asset -> Nullable<Varchar>,
    }
}

table! {
    associated_addresses (transaction_id, address) {
        address -> Varchar,
        transaction_id -> Varchar,
    }
}

table! {
    data_entries (superseded_by, address, key) {
        block_uid -> BigInt,
        transaction_id -> Varchar,
        uid -> BigInt,
        superseded_by -> BigInt,
        address -> Varchar,
        key -> Varchar,
        value_binary -> Nullable<Binary>,
        value_bool -> Nullable<Bool>,
        value_integer -> Nullable<BigInt>,
        value_string -> Nullable<Varchar>,
    }
}

table! {
    data_entries_uid_seq (last_value) {
        last_value -> BigInt,
    }
}

joinable!(associated_addresses -> transactions (transaction_id));

allow_tables_to_appear_in_same_query!(
    blocks_microblocks,
    transactions,
    associated_addresses,
    data_entries
);
