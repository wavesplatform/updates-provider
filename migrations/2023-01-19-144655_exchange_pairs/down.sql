drop index if exists ex_transactions_block_uid_amount_asset_price_asset_part;
drop index if exists blocks_microblocks_time_stamp_uid;
alter table blocks_microblocks  drop column ref_id;