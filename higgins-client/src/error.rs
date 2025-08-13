use higgins_codec::{errors::HigginsCodecError, message::Type};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum HigginsClientError {
    #[error("Incorrect Response received. Expect: {0}, Received: {1}")]
    IncorrectResponseReceived(String, String),
    #[error("Higgins Codec Error")]
    CodecError(#[from] HigginsCodecError),
}
