use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use url::Url;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Topic {
    Config(ConfigFile),
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
                },
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
            },
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
