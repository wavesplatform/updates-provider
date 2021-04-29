-- Your SQL goes here
CREATE TABLE IF NOT EXISTS data_entries (
  block_uid BIGINT NOT NULL
    CONSTRAINT data_entries_block_uid_fkey
      REFERENCES blocks_microblocks (uid)
        ON DELETE CASCADE,
  uid BIGINT GENERATED BY DEFAULT AS IDENTITY
    CONSTRAINT data_entries_uid_key
      UNIQUE,
  superseded_by BIGINT NOT NULL,
  address VARCHAR NOT NULL,
  key VARCHAR NOT NULL,
  transaction_id VARCHAR,
  value_binary TEXT,
  value_bool BOOLEAN,
  value_integer BIGINT,
  value_string VARCHAR,
  CONSTRAINT data_entries_pkey
    PRIMARY KEY (superseded_by, address, key)
);

CREATE INDEX IF NOT EXISTS data_entries_block_uid_idx ON data_entries (block_uid);
CREATE INDEX IF NOT EXISTS data_entries_key_idx ON data_entries(key);
CREATE INDEX IF NOT EXISTS data_entries_value_integer_idx ON data_entries(value_integer);
CREATE INDEX IF NOT EXISTS data_entries_md5_value_string_idx ON data_entries(md5(value_string));
CREATE INDEX IF NOT EXISTS data_entries_expr_ids ON data_entries((1)) WHERE value_bool IS NOT NULL;
CREATE INDEX IF NOT EXISTS data_entries_md5_value_binary_ids ON data_entries(md5(value_binary));