use super::ResourcesRepo;
use crate::error::Error;
use r2d2::Pool;
use r2d2_redis::redis::Commands;
use r2d2_redis::RedisConnectionManager;
use wavesexchange_log::debug;
use wavesexchange_topic::Topic;

#[derive(Debug)]
pub struct ResourcesRepoRedis {
    pool: Pool<RedisConnectionManager>,
}

impl ResourcesRepoRedis {
    pub fn new(pool: Pool<RedisConnectionManager>) -> ResourcesRepoRedis {
        ResourcesRepoRedis { pool }
    }
}

impl ResourcesRepo for ResourcesRepoRedis {
    fn get(&self, resource: &Topic) -> Result<Option<String>, Error> {
        let mut con = self.pool.get()?;
        let key = String::from(resource.to_owned());
        let result = con.get(key)?;
        Ok(result)
    }

    fn set(&self, resource: Topic, value: String) -> Result<(), Error> {
        let mut con = self.pool.get()?;
        let key = String::from(resource);
        debug!("[REDIS] set '{}' = '{}'", key, value);
        con.set(key, value)?;
        Ok(())
    }

    fn del(&self, resource: Topic) -> Result<(), Error> {
        let mut con = self.pool.get()?;
        let key = String::from(resource);
        debug!("[REDIS] del '{}'", key);
        con.del(key)?;
        Ok(())
    }

    fn push(&self, resource: Topic, value: String) -> Result<(), Error> {
        let mut con = self.pool.get()?;
        let key = String::from(resource);
        con.publish(key, value)?;
        Ok(())
    }
}
