pub mod repo;

use crate::errors::Error;
use crate::models::Resource;

pub trait ResourcesRepo {
    fn get(&self, resource: &Resource) -> Result<Option<String>, Error>;

    fn set(&self, resource: Resource, value: String) -> Result<(), Error>;
}
