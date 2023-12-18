use prometheus::IntGauge;

use deadpool_diesel::postgres::{Connection, InteractError, Manager, Pool, Runtime};

use crate::{config::PostgresConfig, error::Error};

pub use deadpool::managed::BuildError;

pub type PgPool = Pool;
pub type PooledPgConnection = Connection;
pub type PgPoolCreateError = BuildError<deadpool_diesel::Error>;
pub type PgPoolRuntimeError = deadpool::managed::PoolError<deadpool_diesel::Error>;

pub fn new(config: &PostgresConfig, gauge: IntGauge) -> Result<PgPoolWithStats, Error> {
    let db_url = config.database_url();

    let manager = Manager::new(db_url, Runtime::Tokio1);

    let pool = Pool::builder(manager)
        .max_size(config.pool_size as usize)
        .build()?;

    Ok(PgPoolWithStats::new(pool, gauge))
}

#[derive(Debug)]
pub struct PgPoolSyncCallError(std::sync::Mutex<InteractError>);

impl From<InteractError> for PgPoolSyncCallError {
    fn from(err: InteractError) -> Self {
        PgPoolSyncCallError(std::sync::Mutex::new(err))
    }
}

impl std::fmt::Display for PgPoolSyncCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.lock().unwrap())
    }
}

impl std::error::Error for PgPoolSyncCallError {}

impl From<InteractError> for crate::Error {
    fn from(err: InteractError) -> Self {
        crate::Error::PgPoolSyncCallError(err.into())
    }
}

#[derive(Clone)]
pub struct PgPoolWithStats {
    pool: PgPool,
    gauge: IntGauge,
}

impl PgPoolWithStats {
    pub fn new(pool: PgPool, gauge: IntGauge) -> Self {
        PgPoolWithStats { pool, gauge }
    }

    pub async fn get(&self) -> Result<PooledPgConnection, PgPoolRuntimeError> {
        let res = self.pool.get().await;
        self.gauge.set(self.pool.status().available as i64);
        res
    }
}
