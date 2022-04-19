use waves_protobuf_schemas::waves::transaction::Data;

pub type Result<T> = std::result::Result<T, Error>;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("ConfigLoadError: {0}")]
    ConfigLoadError(#[from] envy::Error),
    #[error("JoinError: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("PoolError: {0}")]
    PoolError(#[from] diesel::r2d2::PoolError),
    #[error("RedisError: {0}")]
    RedisError(#[from] bb8_redis::redis::RedisError),
    #[error("RedisPoolRunError: {0}")]
    RedisPoolRunError(#[from] bb8::RunError<bb8_redis::redis::RedisError>),
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
    #[error("PostgresConnectionError: {0}")]
    PostgresConnectionError(#[from] diesel::ConnectionError),
    #[error("DbError: {0}")]
    DbError(#[from] diesel::result::Error),
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
