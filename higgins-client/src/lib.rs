use bytes::BytesMut;
use higgins_codec::frame::Frame;
use higgins_codec::{Message, ProduceRequest, message::Type};
use higgins_codec::{ProduceResponse, TakeRecordsRequest};
use prost::Message as _;

pub(crate) mod configuration;
pub(crate) mod functions;
pub(crate) mod ping;
pub(crate) mod query;
pub(crate) mod subscription;
pub(crate) mod produce;
pub mod error;

mod client;

pub use client::Client;
