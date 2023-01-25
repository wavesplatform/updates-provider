table! {
    blocks_microblocks (id) {
        uid -> BigInt,
        id -> Varchar,
        ref_id -> Nullable<Varchar>,
        height -> Int4,
        time_stamp -> Nullable<BigInt>,
    }
}

table! {
    transactions (id) {
        uid -> BigInt,
        block_uid -> BigInt,
        id -> Varchar,
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
        key_frag_0 -> Nullable<Varchar>,
        key_frag_1 -> Nullable<Varchar>,
        key_frag_2 -> Nullable<Varchar>,
        key_frag_3 -> Nullable<Varchar>,
        key_frag_4 -> Nullable<Varchar>,
        key_frag_5 -> Nullable<Varchar>,
        key_frag_6 -> Nullable<Varchar>,
        key_frag_7 -> Nullable<Varchar>,
    }
}

table! {
    leasing_balances (superseded_by, address) {
        block_uid -> BigInt,
        // transaction_id -> Varchar,
        uid -> BigInt,
        superseded_by -> BigInt,
        address -> Varchar,
        balance_in -> BigInt,
        balance_out -> BigInt,
    }
}

table! {
    data_entries_uid_seq (last_value) {
        last_value -> BigInt,
    }
}

table! {
    leasing_balances_uid_seq (last_value) {
        last_value -> BigInt,
    }
}

joinable!(associated_addresses -> transactions (transaction_id));

allow_tables_to_appear_in_same_query!(
    blocks_microblocks,
    transactions,
    associated_addresses,
    data_entries,
    leasing_balances
);
