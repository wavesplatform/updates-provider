use crate::config::PostgresConfig;
use crate::error::Error;
use diesel::{Connection, PgConnection};

// todo max connections
pub fn new(config: &PostgresConfig) -> Result<PgConnection, Error> {
    let db_url = format!(
        "postgres://{}:{}@{}:{}/{}",
        config.user, config.password, config.host, config.port, config.database
    );
    let conn = PgConnection::establish(&db_url)?;
    Ok(conn)
}
