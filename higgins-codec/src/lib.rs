mod codec {
    include!(concat!(env!("OUT_DIR"), "/higgins.rs"));
}

pub use codec::*; // TODO: everything visible in codec here? 

#[cfg(test)]
mod test {

    use prost::Message as _;

    use crate::message::Type;

    use super::*;

    #[test]
    fn can_serde_correctly() {
        // Send a ping command to the server;

        let mut buf = Vec::new();

        let ping = Ping::default();

        let message = Message {
            r#type: Type::Ping as i32,
            ping: Some(ping),
            ..Default::default()
        };

        buf.reserve(message.encoded_len());

        message.encode(&mut buf).unwrap();

        let decode = Message::decode(buf.as_ref()).unwrap();

        assert_eq!(decode.r#type, Type::Ping as i32);
        assert!(decode.ping.is_some());
    }
}
