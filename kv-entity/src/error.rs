#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("TiKV error: {0}")]
    TikvError(#[from] tikv_client::Error),
    #[error("Prost error: {0}")]
    SerializationError(#[from] prost::EncodeError),
    #[error("Prost error: {0}")]
    DeserializationError(#[from] prost::DecodeError),
    #[error("Invalid entity id: {0}")]
    InvalidEntityId(String),
    #[error("Invalid utf8: {0}")]
    InvalidUtf8(std::string::FromUtf8Error),
    #[error("Invalid u64: {0}")]
    InvalidU64(std::num::ParseIntError),
    #[error("Not found")]
    NotFound,
}
