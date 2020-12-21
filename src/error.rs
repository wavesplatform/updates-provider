#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("ConfigLoadError: {0}")]
    ConfigLoadError(#[from] envy::Error),
    #[error("JoinError: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("RedisPoolError: {0}")]
    RedisPoolError(#[from] bb8_redis::redis::RedisError),
    #[error("RunRedisError: {0}")]
    RunRedisError(#[from] bb8::RunError<bb8_redis::redis::RedisError>),
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
    InvalidResource(String),
    #[error("InvalidConfigPath: {0}")]
    InvalidConfigPath(String),
    #[error("InvalidStateEntry: {0}")]
    InvalidStateEntry(String),
}
