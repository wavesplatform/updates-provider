-- Your SQL goes here
CREATE TABLE IF NOT EXISTS blocks_microblocks (
    uid BIGINT GENERATED BY DEFAULT AS IDENTITY
        CONSTRAINT blocks_microblocks_uid_key
            UNIQUE,
    id VARCHAR NOT NULL
        CONSTRAINT blocks_microblocks_pkey
            PRIMARY KEY,
    height INTEGER NOT NULL,
    time_stamp BIGINT
);

CREATE INDEX IF NOT EXISTS blocks_microblocks_id_idx
    ON blocks_microblocks (id);

CREATE INDEX IF NOT EXISTS blocks_microblocks_time_stamp_uid_idx
    ON blocks_microblocks (time_stamp DESC, uid DESC);

CREATE TABLE IF NOT EXISTS transactions (
    block_uid BIGINT NOT NULL
        CONSTRAINT transactions_block_uid_fkey
            REFERENCES blocks_microblocks (uid)
                ON DELETE CASCADE,
    id VARCHAR CONSTRAINT transactions_id_key PRIMARY KEY,
    tx_type SMALLINT NOT NULL,
    body JSONB
);

CREATE INDEX IF NOT EXISTS transactions_block_uid_idx ON transactions (block_uid);

CREATE TABLE IF NOT EXISTS associated_addresses (
    address VARCHAR,
    transaction_id VARCHAR NOT NULL
        REFERENCES transactions (id)
            ON DELETE CASCADE,
    CONSTRAINT associated_addresses_pkey
        PRIMARY KEY (transaction_id, address)
);

CREATE INDEX IF NOT EXISTS associated_addresses_address_idx ON associated_addresses (address);
