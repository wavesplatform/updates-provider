use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Resource {
    Config(ConfigFile),
    Entry(StateEntry),
}

impl TryFrom<&str> for Resource {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let parts: Vec<&str> = s.splitn(2, ":").collect();

        if parts.len() < 2 {
            return Err(Error::InvalidResource(s.to_owned()));
        }

        let resource = parts.first().unwrap().to_owned();
        let metadata = parts.last().unwrap().to_owned();

        match resource {
            "config" => {
                let config_file = ConfigFile::try_from(metadata)?;
                Ok(Resource::Config(config_file))
            }
            "entry" => {
                let state_entry = StateEntry::try_from(metadata)?;
                Ok(Resource::Entry(state_entry))
            }
            _ => Err(Error::InvalidResource(s.to_owned())),
        }
    }
}

impl ToString for Resource {
    fn to_string(&self) -> String {
        match self {
            Resource::Config(cf) => format!("config:{}", cf.to_string()),
            Resource::Entry(se) => format!("entry:{}", se.to_string()),
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StateEntry {
    address: String,
    key: String,
}

impl ToString for StateEntry {
    fn to_string(&self) -> String {
        format!("{}:{}", self.address, self.key)
    }
}

impl TryFrom<&str> for StateEntry {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let parts: Vec<&str> = s.split(":").collect();
        if parts.len() < 2 {
            return Err(Error::InvalidStateEntry(s.to_owned()));
        }

        let address = parts.first().unwrap().to_owned();
        let key = parts.last().unwrap().to_owned();

        Ok(StateEntry {
            address: address.to_owned(),
            key: key.to_owned(),
        })
    }
}
