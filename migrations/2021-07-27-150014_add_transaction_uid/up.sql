ALTER TABLE transactions ADD COLUMN uid BIGINT GENERATED BY DEFAULT AS IDENTITY;

ALTER TABLE transactions ADD CONSTRAINT transactions_uid_ukey UNIQUE (uid);

DROP INDEX transactions_exchange_last_idx;

CREATE INDEX transactions_exchange_last_idx ON transactions (exchange_price_asset, exchange_amount_asset, uid);
