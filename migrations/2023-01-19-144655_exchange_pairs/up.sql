create index if not exists "blocks_microblocks_time_stamp_uid" on blocks_microblocks(time_stamp) include(uid);
create index if not exists "ex_transactions_block_uid_amount_asset_price_asset_part" on transactions(block_uid, exchange_amount_asset, exchange_price_asset) where tx_type =7;
alter table blocks_microblocks add column ref_id TEXT;