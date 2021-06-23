use crate::{config::PostgresConfig, error::Error};
use diesel::pg::PgConnection;
use diesel::r2d2::{ConnectionManager, Pool};
use std::time::Duration;

pub type PgPool = Pool<ConnectionManager<PgConnection>>;

pub fn pool(config: &PostgresConfig) -> Result<PgPool, Error> {
    let db_url = format!(
        "postgres://{}:{}@{}:{}/{}",
        config.user, config.password, config.host, config.port, config.database
    );

    let manager = ConnectionManager::<PgConnection>::new(db_url);
    Ok(Pool::builder()
        .min_idle(Some(2))
        .max_size(config.pool_size as u32)
        .idle_timeout(Some(Duration::from_secs(5 * 60)))
        .build(manager)?)
}
