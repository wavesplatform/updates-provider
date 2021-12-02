use std::time::Duration;

use diesel::pg::PgConnection;
use diesel::r2d2::{ConnectionManager, Pool};
use r2d2::PooledConnection;

use crate::{config::PostgresConfig, error::Error};

pub type PgPool = Pool<ConnectionManager<PgConnection>>;
pub type PooledPgConnection = PooledConnection<ConnectionManager<PgConnection>>;

pub fn new(config: &PostgresConfig) -> Result<PgPool, Error> {
    let db_url = format!(
        "postgres://{}:{}@{}:{}/{}",
        config.user, config.password, config.host, config.port, config.database
    );

    let manager = ConnectionManager::<PgConnection>::new(db_url);
    Ok(Pool::builder()
        .min_idle(Some(config.pool_min_size as u32))
        .max_size(config.pool_size as u32)
        .idle_timeout(Some(Duration::from_secs(
            config.pool_idle_timeout_minutes as u64 * 60,
        )))
        .build(manager)?)
}
