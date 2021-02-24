use crate::error::Error;
use crate::providers::watchlist::{MaybeFromTopic, WatchListItem};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use url::Url;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Topic {
    Config(ConfigFile),
    State(State),
    TestResource(TestResource),
    BlockchainHeight,
}

impl TryFrom<&str> for Topic {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let url = Url::parse(s)?;

        match url.scheme() {
            "topic" => match url.host_str() {
                Some("config") => {
                    let config_file = ConfigFile::try_from(url.path())?;
                    Ok(Topic::Config(config_file))
                }
                Some("state") => {
                    let state = State::try_from(url.path())?;
                    Ok(Topic::State(state))
                }
                Some("test.resource") => {
                    let ps = TestResource::try_from(&url)?;
                    Ok(Topic::TestResource(ps))
                }
                Some("blockchain_height") => Ok(Topic::BlockchainHeight),
                _ => Err(Error::InvalidTopic(s.to_owned())),
            },
            _ => Err(Error::InvalidTopic(s.to_owned())),
        }
    }
}

#[test]
fn string_to_topic() {
    let s = "topic://config/asd/qwe";
    if let Topic::Config(config) = Topic::try_from(s).unwrap() {
        assert_eq!(config.path, "/asd/qwe".to_string());
    } else {
        panic!("not config")
    }
    let s = "topic://state/asd/qwe";
    if let Topic::State(state) = Topic::try_from(s).unwrap() {
        assert_eq!(state.address, "asd".to_string());
        assert_eq!(state.key, "qwe".to_string());
    } else {
        panic!("not state")
    }
    let s = "topic://test.resource/asd/qwe?a=b";
    if let Topic::TestResource(test_resource) = Topic::try_from(s).unwrap() {
        assert_eq!(test_resource.path, "/asd/qwe".to_string());
        assert_eq!(test_resource.query, Some("a=b".to_string()));
    } else {
        panic!("not test_resource")
    }
    let s = "topic://blockchain_height";
    if let Topic::BlockchainHeight = Topic::try_from(s).unwrap() {
    } else {
        panic!("not blockchain_height")
    }
}

impl ToString for Topic {
    fn to_string(&self) -> String {
        let mut url = Url::parse("topic://").unwrap();
        match self {
            Topic::Config(cf) => {
                url.set_host(Some("config")).unwrap();
                url.set_path(&cf.path);
                url.as_str().to_owned()
            }
            Topic::State(state) => {
                url.set_host(Some("state")).unwrap();
                url.set_path(state.to_string().as_str());
                url.as_str().to_owned()
            }
            Topic::TestResource(ps) => {
                url.set_host(Some("test.resource")).unwrap();
                url.set_path(&ps.path);
                if let Some(query) = ps.query.clone() {
                    url.set_query(Some(query.as_str()));
                }
                url.as_str().to_owned()
            }
            Topic::BlockchainHeight => {
                url.set_host(Some("blockchain_height")).unwrap();
                url.as_str().to_owned()
            }
        }
    }
}

#[test]
fn topic_to_string_test() {
    let t = Topic::Config(ConfigFile {
        path: "asd/qwe".to_string(),
    });
    assert_eq!(t.to_string(), "topic://config/asd/qwe".to_string());
    let t = Topic::State(State {
        address: "asd".to_string(),
        key: "qwe".to_string(),
    });
    assert_eq!(t.to_string(), "topic://state/asd/qwe".to_string());
    let t = Topic::TestResource(TestResource {
        path: "asd/qwe".to_string(),
        query: Some("a=b".to_string()),
    });
    assert_eq!(
        t.to_string(),
        "topic://test.resource/asd/qwe?a=b".to_string()
    );
    let t = Topic::BlockchainHeight;
    assert_eq!(t.to_string(), "topic://blockchain_height".to_string());
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ConfigFile {
    path: String,
}

impl ToString for ConfigFile {
    fn to_string(&self) -> String {
        self.path.clone()
    }
}

impl TryFrom<&str> for ConfigFile {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let p = std::path::Path::new(s)
            .to_str()
            .ok_or(Error::InvalidConfigPath(s.to_owned()))?;

        Ok(ConfigFile { path: p.to_owned() })
    }
}

impl From<ConfigFile> for Topic {
    fn from(config_file: ConfigFile) -> Self {
        Self::Config(config_file)
    }
}

impl MaybeFromTopic for ConfigFile {
    fn maybe_item(topic: Topic) -> Option<Self> {
        if let Topic::Config(config_file) = topic {
            return Some(config_file);
        }
        return None;
    }
}

impl WatchListItem for ConfigFile {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct State {
    address: String,
    key: String,
}

impl ToString for State {
    fn to_string(&self) -> String {
        format!("{}/{}", self.address, self.key)
    }
}

impl TryFrom<&str> for State {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let parts = s
            .trim_start_matches("/")
            .split("/")
            .take(2)
            .collect::<Vec<_>>();
        if parts.len() == 2 {
            let address = parts[0].to_string();
            let key = parts[1].to_string();
            Ok(State { address, key })
        } else {
            Err(Error::InvalidStatePath(s.to_owned()))
        }
    }
}

impl From<State> for Topic {
    fn from(state: State) -> Self {
        Self::State(state)
    }
}

impl MaybeFromTopic for State {
    fn maybe_item(topic: Topic) -> Option<Self> {
        if let Topic::State(state) = topic {
            return Some(state);
        }
        return None;
    }
}

impl WatchListItem for State {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TestResource {
    pub path: String,
    pub query: Option<String>,
}

impl ToString for TestResource {
    fn to_string(&self) -> String {
        let mut s = self.path.clone();
        if let Some(query) = self.query.clone() {
            s = format!("{}?{}", s, query).to_string();
        }
        s
    }
}

impl TryFrom<&url::Url> for TestResource {
    type Error = Error;

    fn try_from(u: &url::Url) -> Result<Self, Self::Error> {
        Ok(Self {
            path: u.path().to_string(),
            query: u.query().map(|q| q.to_owned()),
        })
    }
}

impl From<TestResource> for Topic {
    fn from(test_resource: TestResource) -> Self {
        Self::TestResource(test_resource)
    }
}

impl MaybeFromTopic for TestResource {
    fn maybe_item(topic: Topic) -> Option<Self> {
        if let Topic::TestResource(test_resource) = topic {
            return Some(test_resource);
        }
        return None;
    }
}

impl WatchListItem for TestResource {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BlockchainHeight {}

impl ToString for BlockchainHeight {
    fn to_string(&self) -> String {
        "".to_string()
    }
}

impl TryFrom<&url::Url> for BlockchainHeight {
    type Error = Error;

    fn try_from(_u: &url::Url) -> Result<Self, Self::Error> {
        Ok(Self {})
    }
}

impl From<BlockchainHeight> for Topic {
    fn from(_blockchain_height: BlockchainHeight) -> Self {
        Self::BlockchainHeight
    }
}

impl MaybeFromTopic for BlockchainHeight {
    fn maybe_item(topic: Topic) -> Option<Self> {
        if let Topic::BlockchainHeight = topic {
            return Some(Self {});
        }
        return None;
    }
}

impl WatchListItem for BlockchainHeight {}
