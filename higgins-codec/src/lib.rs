mod codec {
    include!(concat!(env!("OUT_DIR"), "/higgins.rs"));
}

pub use codec::*; // TODO: everything visible in codec here?
pub mod errors;
pub mod frame;

#[cfg(test)]
mod test {

    use prost::Message as _;

    use crate::message::Type;

    use super::*;

    fn test_serde() -> Result<(), errors::HigginsCodecError> {
        let mut buf = Vec::new();

        let ping = Ping::default();

        let message = Message {
            r#type: Type::Ping as i32,
            ping: Some(ping),
            ..Default::default()
        };

        buf.reserve(message.encoded_len());

        message.encode(&mut buf)?;

        let decode = Message::decode(buf.as_ref())?;

        assert_eq!(decode.r#type, Type::Ping as i32);
        assert!(decode.ping.is_some());
        Ok(())
    }

    #[test]
    fn can_serde_correctly() {
        // Send a ping command to the server;

        #[allow(clippy::expect_used)]
        test_serde().expect("Failed serde test.");
    }
}
