use super::ResourcesRepo;
use crate::error::Error;
use crate::models::Topic;
use r2d2::Pool;
use r2d2_redis::redis::Commands;
use r2d2_redis::RedisConnectionManager;

#[derive(Debug)]
pub struct ResourcesRepoImpl {
    pool: Pool<RedisConnectionManager>,
}

impl ResourcesRepoImpl {
    pub fn new(pool: Pool<RedisConnectionManager>) -> ResourcesRepoImpl {
        ResourcesRepoImpl { pool }
    }
}

impl ResourcesRepo for ResourcesRepoImpl {
    fn get(&self, resource: &Topic) -> Result<Option<String>, Error> {
        let mut con = self.pool.get()?;

        con.get(resource.to_string())
            .map_err(|err| Error::from(err))
    }

    fn set(&self, resource: Topic, value: String) -> Result<(), Error> {
        let mut con = self.pool.get()?;
        con.set(resource.to_string(), value)
            .map_err(|err| Error::from(err))
    }

    fn del(&self, resource: Topic) -> Result<(), Error> {
        let mut con = self.pool.get()?;
        con.del(resource.to_string())
            .map_err(|err| Error::from(err))
    }

    fn push(&self, resource: Topic, value: String) -> Result<(), Error> {
        let mut con = self.pool.get()?;
        con.publish(resource.to_string(), value)?;
        Ok(())
    }
}
