-- Your SQL goes here
CREATE TABLE IF NOT EXISTS leasing_balances (
  block_uid BIGINT NOT NULL
    CONSTRAINT leasing_balance_block_uid_fkey
      REFERENCES blocks_microblocks (uid)
        ON DELETE CASCADE,
  uid BIGINT GENERATED BY DEFAULT AS IDENTITY
    CONSTRAINT leasing_balance_uid_key
      UNIQUE,
  superseded_by BIGINT NOT NULL,
  address VARCHAR NOT NULL,
  balance_in BIGINT,
  balance_out BIGINT,
  CONSTRAINT leasing_balance_pkey
    PRIMARY KEY (superseded_by, address)
);

CREATE INDEX IF NOT EXISTS leasing_balances_block_uid_idx ON leasing_balances(block_uid);
CREATE INDEX IF NOT EXISTS leasing_balances_address_idx ON leasing_balances(address);