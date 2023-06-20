create index if not exists "blocks_microblocks_time_stamp_uid" on blocks_microblocks(time_stamp) include(uid);
create index if not exists "ex_transactions_block_uid_amount_asset_price_asset_part" on transactions(exchange_amount_asset, exchange_price_asset, block_uid) where tx_type =7;
