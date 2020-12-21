use super::ResourcesRepo;
use crate::error::Error;
use crate::models::Resource;
use async_trait::async_trait;
use bb8_redis::redis::AsyncCommands;
use bb8_redis::RedisConnectionManager;

pub struct ResourcesRepoImpl {
    pool: bb8::Pool<RedisConnectionManager>,
}

impl ResourcesRepoImpl {
    pub fn new(pool: bb8::Pool<RedisConnectionManager>) -> ResourcesRepoImpl {
        ResourcesRepoImpl { pool }
    }
}

#[async_trait]
impl ResourcesRepo for ResourcesRepoImpl {
    async fn get(&self, resource: &Resource) -> Result<Option<String>, Error> {
        let mut con = self.pool.get().await?;

        con.get(resource.to_string())
            .await
            .map_err(|err| Error::from(err))
    }

    async fn set(&self, resource: Resource, value: String) -> Result<(), Error> {
        let mut con = self.pool.get().await?;
        con.set(resource.to_string(), value)
            .await
            .map_err(|err| Error::from(err))
    }
}
