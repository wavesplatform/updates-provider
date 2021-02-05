use crate::error::Error;
use crate::providers::watchlist::{MaybeToTopic, WatchListItem};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use url::Url;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Topic {
    Config(ConfigFile),
    State(State),
    TestResource(TestResource),
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
                _ => Err(Error::InvalidTopic(s.to_owned())),
            },
            _ => Err(Error::InvalidTopic(s.to_owned())),
        }
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
        }
    }
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

impl MaybeToTopic for ConfigFile {
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

impl MaybeToTopic for State {
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

impl MaybeToTopic for TestResource {
    fn maybe_item(topic: Topic) -> Option<Self> {
        if let Topic::TestResource(test_resource) = topic {
            return Some(test_resource);
        }
        return None;
    }
}

impl WatchListItem for TestResource {}
