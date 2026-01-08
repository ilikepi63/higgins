use bytes::BytesMut;
use higgins_codec::CreateSubscriptionRequest;
use higgins_codec::frame::Frame;
use higgins_codec::{Message, message::Type};
use prost::Message as _;

#[allow(unused)]
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

    let frame = Frame::new(write_buf.to_vec());

    frame.try_write(socket).unwrap();

    let frame = Frame::try_read(socket).unwrap();

    let slice = frame.inner();

    let message = Message::decode(slice).unwrap();

    let sub_id = match Type::try_from(message.r#type).unwrap() {
        Type::Createsubscriptionresponse => {
            let sub_id = message
                .create_subscription_response
                .unwrap()
                .subscription_id;

            sub_id.unwrap()
        }
        _ => panic!("Received incorrect response from server for Create Subscription request."),
    };

    Ok(sub_id)
}
