use bytes::BytesMut;
use higgins_codec::CreateSubscriptionRequest;
use higgins_codec::{Message, message::Type};
use prost::Message as _;

pub fn create_subscription<T: std::io::Read + std::io::Write>(
    stream: &[u8],
    socket: &mut T,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let create_subscription = CreateSubscriptionRequest {
        offset: None,
        offset_type: 0,
        timestamp: None,
        stream_name: stream.to_vec(),
    };

    let mut write_buf = BytesMut::new();
    let mut read_buf = BytesMut::zeroed(1024);

    Message {
        r#type: Type::Createsubscriptionrequest as i32,
        create_subscription_request: Some(create_subscription),
        ..Default::default()
    }
    .encode(&mut write_buf)?;

    let _result = socket.write_all(&write_buf)?;

    let n = socket.read(&mut read_buf).unwrap();

    assert_ne!(n, 0);

    let slice = &read_buf[0..n];

    let message = Message::decode(slice).unwrap();

    let sub_id = match Type::try_from(message.r#type).unwrap() {
        Type::Createsubscriptionresponse => {
            let sub_id = message
                .create_subscription_response
                .unwrap()
                .subscription_id;

            tracing::info!("Got the sub_id: {:#?}", sub_id);

            sub_id.unwrap()
        }
        _ => panic!("Received incorrect response from server for Create Subscription request."),
    };

    Ok(sub_id)
}
