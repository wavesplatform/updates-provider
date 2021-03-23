-- Your SQL goes here
CREATE TABLE IF NOT EXISTS exchanges (
    transaction_id VARCHAR NOT NULL REFERENCES transactions (id)
        ON DELETE CASCADE PRIMARY KEY,
    amount_asset VARCHAR,
    price_asset VARCHAR
);

CREATE INDEX IF NOT EXISTS exchanges_amount_asset_idx ON exchanges (amount_asset);
CREATE INDEX IF NOT EXISTS exchanges_price_asset_idx ON exchanges (price_asset);
