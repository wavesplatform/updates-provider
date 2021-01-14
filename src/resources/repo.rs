use super::ResourcesRepo;
use crate::errors::Error;
use crate::models::Resource;
use r2d2::Pool;
use r2d2_redis::redis::Commands;
use r2d2_redis::RedisConnectionManager;

pub struct ResourcesRepoImpl {
    pool: Pool<RedisConnectionManager>,
}

impl ResourcesRepoImpl {
    pub fn new(pool: Pool<RedisConnectionManager>) -> ResourcesRepoImpl {
        ResourcesRepoImpl { pool }
    }
}

impl ResourcesRepo for ResourcesRepoImpl {
    fn get(&self, resource: &Resource) -> Result<Option<String>, Error> {
        let mut con = self.pool.get()?;

        con.get(resource.to_string())
            .map_err(|err| Error::from(err))
    }

    fn set(&self, resource: Resource, value: String) -> Result<(), Error> {
        let mut con = self.pool.get()?;
        con.set(resource.to_string(), value)
            .map_err(|err| Error::from(err))
    }
}
