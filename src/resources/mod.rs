pub mod repo;

use crate::error::Error;
use wavesexchange_topic::Topic;

pub trait ResourcesRepo {
    fn get(&self, resource: &Topic) -> Result<Option<String>, Error>;

    fn set(&self, resource: Topic, value: String) -> Result<(), Error>;

    fn del(&self, resource: Topic) -> Result<(), Error>;

    fn push(&self, resource: Topic, value: String) -> Result<(), Error>;

    fn set_and_push(&self, resource: Topic, value: String) -> Result<(), Error> {
        self.set(resource.clone(), value.clone())?;
        self.push(resource, value)?;
        Ok(())
    }
}
