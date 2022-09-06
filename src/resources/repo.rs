use super::ResourcesRepo;
use crate::error::Error;
use crate::redis::{AsyncCommands, RedisPoolWithStats};
use async_trait::async_trait;
use wavesexchange_log::debug;
use wx_topic::Topic;

pub struct ResourcesRepoRedis {
    pool: RedisPoolWithStats,
}

impl ResourcesRepoRedis {
    pub fn new(pool: RedisPoolWithStats) -> ResourcesRepoRedis {
        ResourcesRepoRedis { pool }
    }
}

#[async_trait]
impl ResourcesRepo for ResourcesRepoRedis {
    async fn get(&self, resource: &Topic) -> Result<Option<String>, Error> {
        let mut con = self.pool.get().await?;
        let key = resource.to_string();
        let result = con.get(key).await?;
        Ok(result)
    }

    async fn set(&self, resource: &Topic, value: String) -> Result<(), Error> {
        let mut con = self.pool.get().await?;
        let key = resource.to_string();
        debug!("[REDIS] set '{}' = '{}'", key, value);
        con.set(key, value).await?;
        Ok(())
    }

    async fn del(&self, resource: &Topic) -> Result<(), Error> {
        let mut con = self.pool.get().await?;
        let key = resource.to_string();
        debug!("[REDIS] del '{}'", key);
        con.del(key).await?;
        Ok(())
    }

    async fn push(&self, resource: &Topic, value: String) -> Result<(), Error> {
        let mut con = self.pool.get().await?;
        let key = resource.to_string();
        con.publish(key, value).await?;
        Ok(())
    }

    async fn set_and_push(&self, resource: &Topic, value: String) -> Result<(), Error> {
        let mut con = self.pool.get().await?;
        let key = resource.to_string();
        debug!("[REDIS] set+publish '{}' = '{}'", key, value);
        con.set(key.clone(), value.clone()).await?;
        con.publish(key, value).await?;
        Ok(())
    }
}
