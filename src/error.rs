//! Custom error types for DataFusion NATS integration.

use datafusion::error::DataFusionError;
use thiserror::Error;

/// Custom error type for DataFusion NATS operations.
#[derive(Error, Debug)]
pub enum DatafusionNatsError {
    /// NATS client error.
    #[error("NATS client error: {0}")]
    NatsError(#[from] async_nats::Error),

    /// DataFusion error.
    #[error("DataFusion error: {0}")]
    DataFusionError(#[from] DataFusionError),

    /// Arrow error.
    #[error("Arrow error: {0}")]
    ArrowError(#[from] datafusion::arrow::error::ArrowError),

    /// General error.
    #[error("General error: {0}")]
    General(String),

    /// Config error.
    #[error("Config error: {0}")]
    Config(String),


    /// Format error.
    #[error("Format error: {0}")]
    Format(String),

    /// JSON serialization/deserialization error.
    #[error("JSON error: {0}")]
    SerdeJsonError(#[from] serde_json::Error),

    /// UTF-8 conversion error.
    #[error("UTF-8 conversion error: {0}")]
    FromUtf8Error(#[from] std::string::FromUtf8Error),

    /// System time error.
    #[error("System time error: {0}")]
    SystemTimeError(#[from] std::time::SystemTimeError),
}

/// A specialized `Result` type for DataFusion NATS operations.
pub type Result<T> = std::result::Result<T, DatafusionNatsError>;
