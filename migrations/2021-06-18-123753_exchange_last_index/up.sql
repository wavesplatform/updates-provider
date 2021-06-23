-- Your SQL goes here
CREATE INDEX IF NOT EXISTS transactions_exchange_last_idx ON transactions (exchange_price_asset, exchange_amount_asset, block_uid)