pub mod repo;

use crate::error::Error;
use crate::models::Topic;

pub trait ResourcesRepo {
    fn get(&self, resource: &Topic) -> Result<Option<String>, Error>;

    fn set(&self, resource: Topic, value: String) -> Result<(), Error>;
}
