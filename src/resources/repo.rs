use super::ResourcesRepo;
use crate::error::Error;
use async_trait::async_trait;
use bb8::Pool;
use bb8_redis::redis::AsyncCommands;
use bb8_redis::RedisConnectionManager;
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

#[async_trait]
impl ResourcesRepo for ResourcesRepoRedis {
    async fn get(&self, resource: &Topic) -> Result<Option<String>, Error> {
        let mut con = self.pool.get().await?;
        let key = String::from(resource.to_owned());
        let result = con.get(key).await?;
        Ok(result)
    }

    async fn set(&self, resource: Topic, value: String) -> Result<(), Error> {
        let mut con = self.pool.get().await?;
        let key = String::from(resource);
        debug!("[REDIS] set '{}' = '{}'", key, value);
        con.set(key, value).await?;
        Ok(())
    }

    async fn del(&self, resource: Topic) -> Result<(), Error> {
        let mut con = self.pool.get().await?;
        let key = String::from(resource);
        debug!("[REDIS] del '{}'", key);
        con.del(key).await?;
        Ok(())
    }

    async fn push(&self, resource: Topic, value: String) -> Result<(), Error> {
        let mut con = self.pool.get().await?;
        let key = String::from(resource);
        con.publish(key, value).await?;
        Ok(())
    }
}
