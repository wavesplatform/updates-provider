use waves_protobuf_schemas::{tonic, waves::transaction::Data};

pub type Result<T> = std::result::Result<T, Error>;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("ConfigLoadError: {0}")]
    ConfigLoadError(#[from] envy::Error),
    #[error("JoinError: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("PgPoolCreateError: {0}")]
    PgPoolCreateError(#[from] crate::db::pool::PgPoolCreateError),
    #[error("PgPoolRuntimeError: {0}")]
    PgPoolRuntimeError(#[from] crate::db::pool::PgPoolRuntimeError),
    #[error("PgPoolSyncCallError: {0}")]
    PgPoolSyncCallError(#[from] crate::db::pool::PgPoolSyncCallError),
    #[error("DbError: {0}")]
    DbError(#[from] diesel::result::Error),
    #[error("RedisError: {0}")]
    RedisError(#[from] crate::redis::RedisError),
    #[error("RedisPoolCreateError: {0}")]
    RedisPoolCreateError(#[from] crate::redis::RedisPoolCreateError),
    #[error("RedisPoolInitError: {0} - {1}")]
    RedisPoolInitError(&'static str, crate::redis::RedisPoolError),
    #[error("RedisPoolCreateError: {0}")]
    RedisPoolRuntimeError(#[from] crate::redis::RedisPoolError),
    #[error("SerdeJsonError: {0}")]
    SerdeJsonError(#[from] serde_json::Error),
    #[error("SendError: {0}")]
    SendError(String),
    #[error("ReqwestError: {0}")]
    ReqwestError(#[from] reqwest::Error),
    #[error("UrlParseError: {0}")]
    UrlParseError(#[from] url::ParseError),
    #[error("ResourceFetchingError: {0}")]
    ResourceFetchingError(String),
    #[error("InvalidResource: {0}")]
    InvalidTopic(String),
    #[error("InvalidConfigPath: {0}")]
    InvalidConfigPath(String),
    #[error("InvalidStatePath: {0}")]
    InvalidStatePath(String),
    #[error("GRPCConnectionError: {0}")]
    GRPCConnectionError(#[from] tonic::transport::Error),
    #[error("GRPCUriError: {0}")]
    GRPCUriError(String),
    #[error("GRPCError: {0}")]
    GRPCError(#[from] tonic::Status),
    #[error("SendErrorBlockchainUpdated: {0}")]
    SendErrorBlockchainUpdated(String),
    #[error("InvalidTransactionType: {0}")]
    InvalidTransactionType(String),
    #[error("InvalidTransactionQuery: {0}")]
    InvalidTransactionQuery(ErrorQuery),
    #[error("GRPCBodyError: {0}")]
    GRPCBodyError(String),
    #[error("InvalidDBTransactionType: {0}")]
    InvalidDBTransactionType(String),
    #[error("InvalidExchangeData: {0:?}")]
    InvalidExchangeData(Data),
    #[error("InvalidOrderType: {0}")]
    InvalidOrderType(i32),
    #[error("InvalidOrderVersion: {0}")]
    InvalidOrderVersion(i32),
    #[error("SendErrorVecBlockchainUpdate")]
    SendErrorVecBlockchainUpdate,
    #[error("InvalidLeasingPath: {0}")]
    InvalidLeasingPath(String),
    #[error("AssetServiceClientError: {0}")]
    AssetServiceClientError(String),
}

#[derive(Debug)]
pub struct ErrorQuery(pub Option<String>);

impl std::fmt::Display for ErrorQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0.as_ref() {
            None => write!(f, "None"),
            Some(s) => write!(f, "{}", s.to_owned()),
        }
    }
}
