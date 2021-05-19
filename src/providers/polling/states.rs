use super::super::watchlist_process;
use super::requester::Requester;
use super::{TSResourcesRepoImpl, TSUpdatesProviderLastValues};
use crate::error::Error;
use crate::models::State;
use async_trait::async_trait;
use futures::stream::{self, StreamExt, TryStreamExt};
use reqwest::{Client, ClientBuilder};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use wavesexchange_log::error;

#[derive(Debug)]
pub struct Config {
    pub base_url: String,
    pub polling_delay: Duration,
    pub delete_timeout: Duration,
    pub batch_size: usize,
    pub concurrent_requests_count: usize,
}

pub struct StateRequester {
    base_url: String,
    http_client: Client,
    batch_size: usize,
    concurrent_requests_count: usize,
}

impl StateRequester {
    pub fn new(
        base_url: impl AsRef<str>,
        batch_size: usize,
        concurrent_requests_count: usize,
    ) -> Self {
        Self {
            batch_size,
            concurrent_requests_count,
            base_url: base_url.as_ref().to_owned(),
            http_client: ClientBuilder::new()
                .timeout(std::time::Duration::from_secs(30))
                .connect_timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap(),
        }
    }

    pub async fn get(&self, address_key_pairs: &Vec<State>) -> Result<String, Error> {
        let req = Request {
            address_key_pairs: address_key_pairs.clone(),
        };
        let body = serde_json::to_string(&req).unwrap();
        let url = reqwest::Url::parse(format!("{}/entries", self.base_url).as_ref())?;
        let res = self.http_client.post(url).body(body).send().await?;
        let status = res.status();
        let text = res.text().await.map_err(|e| Error::from(e))?;
        if status.is_success() {
            Ok(text.to_owned())
        } else {
            error!("error while fetching states data");
            Err(Error::ResourceFetchingError(
                "error fetching states /entries".to_owned(),
            ))
        }
    }
}

#[async_trait]
impl Requester<State> for StateRequester {
    async fn process<'a, I: Iterator<Item = &'a State> + Send + Sync>(
        &self,
        items: I,
        resources_repo: &TSResourcesRepoImpl,
        last_values: &TSUpdatesProviderLastValues,
    ) -> Result<(), Error> {
        let states = self.group_states(items);
        stream::iter(states)
            .map(Ok)
            .try_for_each_concurrent(
                self.concurrent_requests_count,
                |address_key_pairs| async move {
                    let text = self.get(&address_key_pairs).await?;
                    let response = serde_json::from_str::<Response>(&text)?;
                    if address_key_pairs.len() != response.entries.len() {
                        let err = "error fetching states, invalid response".into();
                        return Err(Error::ResourceFetchingError(err));
                    };
                    for (state, value) in address_key_pairs.iter().zip(response.entries.iter()) {
                        let current_value = serde_json::to_string(value)?;
                        watchlist_process(state, current_value, resources_repo, last_values)
                            .await?;
                    }
                    Ok(())
                },
            )
            .await?;
        Ok(())
    }
}

impl StateRequester {
    fn group_states<'a>(&self, items: impl Iterator<Item = &'a State>) -> Vec<Vec<State>> {
        items.fold(vec![], |mut acc, state| {
            if let Some(last) = acc.last_mut() {
                if last.len() == self.batch_size {
                    acc.push(vec![state.to_owned()])
                } else {
                    last.push(state.to_owned())
                }
            } else {
                acc.push(vec![state.to_owned()])
            }
            acc
        })
    }
}

#[derive(Debug, Serialize)]
pub struct Request {
    pub address_key_pairs: Vec<State>,
}

#[derive(Debug, Deserialize)]
pub struct Response {
    pub entries: Vec<Option<serde_json::Value>>,
}
