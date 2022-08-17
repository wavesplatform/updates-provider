use self::{item::TestItem, repo::TestProviderRepo};
use super::Provider;
use crate::db::BlockchainUpdate;
use crate::providers::{
    watchlist::{tests::repo::TestResourcesRepo, WatchListUpdate},
    UpdatesProvider,
};
use crate::resources::ResourcesRepo;
use crate::waves::{BlockMicroblockAppend, DataEntry, Fragments, ValueDataEntry};
use std::{convert::TryFrom, sync::Arc, time::Duration};
use wavesexchange_topic::Topic;

mod repo {
    use crate::db::{repo_provider::ProviderRepo, DataEntry, LeasingBalance};
    pub use crate::providers::watchlist::tests::item::TestItem;
    use crate::waves::transactions::{Transaction, TransactionType};
    use async_trait::async_trait;
    use itertools::Itertools;
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };
    use wavesexchange_topic::StateSingle;

    #[derive(Clone)]
    pub struct TestProviderRepo(Arc<Mutex<HashMap<TestItem, String>>>);

    impl Default for TestProviderRepo {
        fn default() -> Self {
            TestProviderRepo(Arc::new(Mutex::new(HashMap::new())))
        }
    }

    #[async_trait]
    impl ProviderRepo for TestProviderRepo {
        async fn last_transaction_by_address(
            &self,
            _address: String,
        ) -> crate::error::Result<Option<Transaction>> {
            unimplemented!()
        }

        async fn last_transaction_by_address_and_type(
            &self,
            _address: String,
            _transaction_type: TransactionType,
        ) -> crate::error::Result<Option<Transaction>> {
            unimplemented!()
        }

        async fn last_exchange_transaction(
            &self,
            _amount_asset: String,
            _price_asset: String,
        ) -> crate::error::Result<Option<Transaction>> {
            unimplemented!()
        }

        async fn last_leasing_balance(
            &self,
            _address: String,
        ) -> crate::error::Result<Option<LeasingBalance>> {
            unimplemented!()
        }

        async fn last_data_entry(
            &self,
            _address: String,
            _key: String,
        ) -> crate::error::Result<Option<DataEntry>> {
            unimplemented!()
        }

        async fn find_matching_data_keys(
            &self,
            _addresses: Vec<String>,
            _key_patterns: Vec<String>,
        ) -> crate::error::Result<Vec<StateSingle>> {
            unimplemented!()
        }
    }

    impl TestProviderRepo {
        pub fn has_value(&self, key: &TestItem) -> bool {
            let data = self.0.lock().unwrap();
            data.contains_key(key)
        }

        pub fn get_value(&self, key: &TestItem) -> String {
            let data = self.0.lock().unwrap();
            data.get(key).cloned().unwrap_or_else(|| "NONE".to_string())
        }

        pub fn put_value(&self, key: TestItem, value: impl ToString) {
            let mut data = self.0.lock().unwrap();
            data.insert(key, value.to_string());
        }

        pub fn del_value(&self, key: &TestItem) {
            let mut data = self.0.lock().unwrap();
            data.remove(key);
        }

        pub fn get_keys_with_prefix(&self, key_prefix: &str) -> Vec<String> {
            let data = self.0.lock().unwrap();
            data.keys()
                .filter(|&k| k.0.starts_with(key_prefix))
                .map(|k| k.0.to_owned())
                .collect_vec()
        }

        pub fn reset(&self) {
            let mut data = self.0.lock().unwrap();
            data.clear();
        }
    }
}

mod item {
    use super::super::{DataFromBlock, Item, LastValue};
    use super::repo::TestProviderRepo;
    pub use crate::providers::watchlist::tests::item::TestItem;
    use crate::waves::{BlockMicroblockAppend, ValueDataEntry};
    use async_trait::async_trait;

    impl Item<TestProviderRepo> for TestItem {}

    impl DataFromBlock for TestItem {
        fn data_from_block(block: &BlockMicroblockAppend) -> Vec<(String, Self)> {
            block
                .data_entries
                .iter()
                .map(|de| {
                    let address = de.address.as_str();
                    let key = de.key.as_str();
                    let value = match &de.value {
                        ValueDataEntry::Null => "",
                        ValueDataEntry::String(s) => s.as_str(),
                        _ => panic!("broken test"),
                    };
                    let topic = format!("topic://state/{}/{}", address, key);
                    let topic = Box::leak(Box::new(topic)).as_str();
                    let data = TestItem(topic);
                    (value.to_string(), data)
                })
                .collect()
        }
    }

    #[async_trait]
    impl LastValue<TestProviderRepo> for TestItem {
        async fn last_value(self, repo: &TestProviderRepo) -> crate::error::Result<String> {
            if self.0.ends_with('*') {
                let key_prefix = self.matched_subtopic_prefix();
                let mut matching_topics = repo.get_keys_with_prefix(&key_prefix);
                matching_topics.sort();
                Ok(serde_json::to_string(&matching_topics)?)
            } else {
                Ok(repo.get_value(&self))
            }
        }
    }

    // Check that mocks itself are working properly

    #[test]
    fn test_data_from_block() {
        use crate::waves::{BlockMicroblockAppend, DataEntry, Fragments, ValueDataEntry};
        let address = "foo";
        let key = "bar";
        let value = "baz";
        let entry = DataEntry {
            address: address.to_string(),
            key: key.to_string(),
            transaction_id: "0".to_string(),
            value: ValueDataEntry::String(value.to_string()),
            fragments: Fragments {
                key: vec![],
                value: vec![],
            },
        };
        let block = BlockMicroblockAppend {
            id: "0".to_string(),
            time_stamp: None,
            height: 0,
            transactions: vec![],
            data_entries: vec![entry],
            leasing_balances: vec![],
        };
        let data = TestItem::data_from_block(&block);
        assert_eq!(data.len(), 1);
        let (data_value, data_item) = &data[0];
        assert_eq!(data_value, "baz");
        assert_eq!(data_item, &TestItem("topic://state/foo/bar"));
    }

    #[tokio::test]
    async fn test_last_value() -> anyhow::Result<()> {
        let db = TestProviderRepo::default();

        let single_topic = "topic://state/foo/bar";
        let multi_topic = "topic://state?address__in[]=foo&key__match_any[]=bar*";

        assert_eq!(db.get_value(&TestItem(single_topic)), "NONE");
        let item = TestItem(single_topic);
        let last = item.last_value(&db).await?;
        assert_eq!(last, "NONE");
        let item = TestItem(multi_topic);
        let last = item.last_value(&db).await?;
        assert_eq!(last, "[]");

        db.put_value(TestItem(single_topic), "value");
        let item = TestItem(multi_topic);
        let last = item.last_value(&db).await?;
        assert_eq!(last, r#"["topic://state/foo/bar"]"#);

        Ok(())
    }
}

#[tokio::test]
async fn test_updates_provider() -> anyhow::Result<()> {
    // Poor man's thread synchronization
    let sync = || async { tokio::time::sleep(Duration::from_millis(10)).await };

    // Setup
    let res_repo = Arc::new(TestResourcesRepo::default());
    let db_repo = TestProviderRepo::default();
    let keep_alive = Duration::from_nanos(1);
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let provider =
        Provider::<TestItem, _, _>::new(res_repo.clone(), keep_alive, db_repo.clone(), rx);
    let watchlist = provider.watchlist();
    let sender = provider.fetch_updates().await?;
    let subscribe = |topic: &'static str| {
        let sender = sender.clone();
        let update = WatchListUpdate::Updated {
            item: TestItem(topic),
        };
        async move {
            let res = sender.send(update).await;
            assert!(res.is_ok());
        }
    };
    let unsubscribe = |topic: &'static str| {
        let sender = sender.clone();
        let update = WatchListUpdate::Removed {
            item: TestItem(topic),
        };
        async move {
            let res = sender.send(update).await;
            assert!(res.is_ok());
        }
    };
    let push_update = |key: &str, value: Option<&str>| {
        let tx = tx.clone();
        let update = DataEntry {
            address: "address".to_string(),
            key: key.to_string(),
            transaction_id: "0".to_string(),
            value: match value {
                None => ValueDataEntry::Null,
                Some(v) => ValueDataEntry::String(v.to_string()),
            },
            fragments: Fragments {
                key: vec![],
                value: vec![],
            },
        };
        let update = BlockchainUpdate::Block(BlockMicroblockAppend {
            id: "0".to_string(),
            time_stamp: None,
            height: 0,
            transactions: vec![],
            data_entries: vec![update],
            leasing_balances: vec![],
        });
        let update = Arc::new(vec![update]);
        async move {
            let res = tx.send(update).await;
            assert!(res.is_ok());
        }
    };
    let force_watchlist_cleanup = || {
        let watchlist = watchlist.clone();
        async move {
            let mut wl = watchlist.write().await;
            wl.delete_old().await;
        }
    };

    // Subscribe to non-existing key
    assert!(!db_repo.has_value(&TestItem("topic://state/address/foo")));
    assert_eq!(
        res_repo
            .get(&Topic::try_from("topic://state/address/foo")?)
            .await?,
        None
    );
    subscribe("topic://state/address/foo").await;
    sync().await;
    assert_eq!(
        res_repo
            .get(&Topic::try_from("topic://state/address/foo")?)
            .await?,
        Some("NONE".to_string())
    );

    // Subscribe to existing key (in database but not in redis)
    db_repo.put_value(TestItem("topic://state/address/bar"), "bar_value");
    assert!(db_repo.has_value(&TestItem("topic://state/address/bar")));
    assert_eq!(
        res_repo
            .get(&Topic::try_from("topic://state/address/bar")?)
            .await?,
        None
    );
    subscribe("topic://state/address/bar").await;
    sync().await;
    assert_eq!(
        res_repo
            .get(&Topic::try_from("topic://state/address/bar")?)
            .await?,
        Some("bar_value".to_string())
    );

    // Push updates on both topics
    push_update("foo", Some("updated_foo")).await;
    push_update("bar", Some("updated_bar")).await;
    sync().await;
    assert_eq!(
        res_repo
            .get(&Topic::try_from("topic://state/address/foo")?)
            .await?,
        Some("updated_foo".to_string())
    );
    assert_eq!(
        res_repo
            .get(&Topic::try_from("topic://state/address/bar")?)
            .await?,
        Some("updated_bar".to_string())
    );

    // Cleanup
    db_repo.del_value(&TestItem("topic://state/address/foo"));
    db_repo.del_value(&TestItem("topic://state/address/bar"));

    // Subscribe to multitopic, no data exists
    assert_eq!(
        res_repo
            .get(&Topic::try_from(
                "topic://state?address__in[]=address&key__match_any[]=multi*"
            )?)
            .await?,
        None
    );
    assert!(db_repo
        .get_keys_with_prefix("topic://state/address/multi")
        .is_empty());
    subscribe("topic://state?address__in[]=address&key__match_any[]=multi*").await;
    sync().await;
    assert_eq!(
        res_repo
            .get(&Topic::try_from(
                "topic://state?address__in[]=address&key__match_any[]=multi*"
            )?)
            .await?,
        Some("[]".to_string())
    );

    // Now emit a blockchain update so that the above multitopic will get a subtopic
    assert!(!db_repo.has_value(&TestItem("topic://state/address/multi1")));
    assert_eq!(
        res_repo
            .get(&Topic::try_from("topic://state/address/multi1")?)
            .await?,
        None
    );
    push_update("multi1", Some("value_1")).await;
    sync().await;
    assert_eq!(
        res_repo
            .get(&Topic::try_from("topic://state/address/multi1")?)
            .await?,
        Some("value_1".to_string())
    );
    assert_eq!(
        res_repo
            .get(&Topic::try_from(
                "topic://state?address__in[]=address&key__match_any[]=multi*"
            )?)
            .await?,
        Some(r#"["topic://state/address/multi1"]"#.to_string())
    );

    // Emit another blockchain update so that the above multitopic will get second subtopic
    assert!(!db_repo.has_value(&TestItem("topic://state/address/multi2")));
    assert_eq!(
        res_repo
            .get(&Topic::try_from("topic://state/address/multi2")?)
            .await?,
        None
    );
    push_update("multi2", Some("value_2")).await;
    sync().await;
    assert_eq!(
        res_repo
            .get(&Topic::try_from("topic://state/address/multi2")?)
            .await?,
        Some("value_2".to_string())
    );
    assert_eq!(
        res_repo
            .get(&Topic::try_from(
                "topic://state?address__in[]=address&key__match_any[]=multi*"
            )?)
            .await?,
        Some(r#"["topic://state/address/multi1","topic://state/address/multi2"]"#.to_string())
    );

    // Emit third subtopic and delete the previous one
    // Deleted topic should still remain under multitopic so that clients can receive 'deleted' event
    assert!(!db_repo.has_value(&TestItem("topic://state/address/multi3")));
    assert_eq!(
        res_repo
            .get(&Topic::try_from("topic://state/address/multi3")?)
            .await?,
        None
    );
    push_update("multi2", None).await;
    push_update("multi3", Some("value_3")).await;
    sync().await;
    assert_eq!(
        res_repo
            .get(&Topic::try_from("topic://state/address/multi3")?)
            .await?,
        Some("value_3".to_string())
    );
    assert_eq!(
        res_repo.get(&Topic::try_from("topic://state?address__in[]=address&key__match_any[]=multi*")?).await?,
        Some(r#"["topic://state/address/multi1","topic://state/address/multi2","topic://state/address/multi3"]"#.to_string())
    );

    // Unsubscribe from multitopic
    unsubscribe("topic://state?address__in[]=address&key__match_any[]=multi*").await;
    sync().await;
    force_watchlist_cleanup().await;
    assert_eq!(
        res_repo
            .get(&Topic::try_from(
                "topic://state?address__in[]=address&key__match_any[]=multi*"
            )?)
            .await?,
        None
    );

    // Update should be ignored
    push_update("multi1", Some("value_1_updated")).await;
    sync().await;
    assert_eq!(
        res_repo
            .get(&Topic::try_from("topic://state/address/multi1")?)
            .await?,
        None
    );
    assert_eq!(
        res_repo
            .get(&Topic::try_from("topic://state/address/multi2")?)
            .await?,
        None
    );
    assert_eq!(
        res_repo
            .get(&Topic::try_from("topic://state/address/multi3")?)
            .await?,
        None
    );

    // Subscribe to multitopic, data exists in db
    res_repo.reset();
    db_repo.reset();
    db_repo.put_value(TestItem("topic://state/address/multi1"), "value_1");
    db_repo.put_value(TestItem("topic://state/address/multi2"), "value_2");
    subscribe("topic://state?address__in[]=address&key__match_any[]=multi*").await;
    sync().await;
    assert_eq!(
        res_repo
            .get(&Topic::try_from(
                "topic://state?address__in[]=address&key__match_any[]=multi*"
            )?)
            .await?,
        Some(r#"["topic://state/address/multi1","topic://state/address/multi2"]"#.to_string())
    );

    // Subscribe to multitopic, data exists in db & redis
    assert_eq!(
        res_repo
            .get(&Topic::try_from("topic://state/address/multi1")?)
            .await?,
        Some("value_1".to_string())
    );
    assert_eq!(
        res_repo
            .get(&Topic::try_from("topic://state/address/multi2")?)
            .await?,
        Some("value_2".to_string())
    );
    subscribe("topic://state?address__in[]=address&key__match_any[]=m*").await;
    sync().await;
    assert_eq!(
        res_repo
            .get(&Topic::try_from(
                "topic://state?address__in[]=address&key__match_any[]=m*"
            )?)
            .await?,
        Some(r#"["topic://state/address/multi1","topic://state/address/multi2"]"#.to_string())
    );

    Ok(())
}
