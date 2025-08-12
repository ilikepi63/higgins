use higgins_codec::errors::HigginsCodecError;
use prost::EncodeError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum HigginsClientError {
    #[error("Incorrect Response received. Expect: {0}, Received: {1}")]
    IncorrectResponseReceived(String, String),
    #[error("Higgins Codec Error")]
    CodecError(#[from] HigginsCodecError),
    #[error("Encoding Error")]
    EncodeError(#[from] EncodeError),
    #[error("Missing Payload")]
    MissingPayload,
}
