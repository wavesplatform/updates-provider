#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("ConfigLoadError: {0}")]
    ConfigLoadError(#[from] envy::Error),
    #[error("JoinError: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("PoolError: {0}")]
    PoolError(#[from] r2d2::Error),
    #[error("RedisPoolError: {0}")]
    RedisPoolError(#[from] r2d2_redis::Error),
    #[error("RedisError: {0}")]
    RedisError(#[from] r2d2_redis::redis::RedisError),
    #[error("SerdeJsonError: {0}")]
    SerdeJsonError(#[from] serde_json::Error),
    #[error("SendError: {0}")]
    SendError(
        #[from] tokio::sync::mpsc::error::SendError<crate::subscriptions::SubscriptionUpdate>,
    ),
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
}
