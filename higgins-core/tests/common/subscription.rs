use bytes::BytesMut;
use higgins_codec::CreateSubscriptionRequest;
use higgins_codec::frame::Frame;
use higgins_codec::{Message, message::Type, TakeRecordsRequest};
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


pub fn take_from_subscription<T: std::io::Read + std::io::Write>(
    n: u64,
    sub_id: &[u8],
    stream_name: &[u8],
    socket: &mut T,
) -> Result<(), Box<dyn std::error::Error>>{
    tracing::info!("Writing the TakeRecordsRequest..");
    let take_request = TakeRecordsRequest {
        n,
        subscription_id: sub_id.to_vec(),
        stream_name: stream_name.to_vec(),
    };

    let mut write_buf = BytesMut::new();
    let mut read_buf = BytesMut::zeroed(8048);

    Message {
        r#type: Type::Takerecordsrequest as i32,
        take_records_request: Some(take_request),
        ..Default::default()
    }
    .encode(&mut write_buf)
    .unwrap();

    socket.write_all(&write_buf).unwrap();

    tracing::info!("Wrote the take records request.");

    Ok(())
}
