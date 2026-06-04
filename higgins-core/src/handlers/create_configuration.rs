use std::sync::Arc;

use bytes::BytesMut;
use higgins_codec::{
    CreateConfigurationRequest, CreateConfigurationResponse, Message, message::Type,
};
use prost::Message as _;
use tokio::sync::RwLock;

use crate::broker::Broker;
use tokio::sync::mpsc::Sender;

pub async fn handle_create_configuration(
    broker: Arc<RwLock<Broker>>,
    message: Message,
    writer_tx: Sender<BytesMut>,
) {
    tracing::info!("We're trying to get the lock.");

    let mut broker = broker.write().await;

    tracing::info!("Applying configuration..");

    if let Some(CreateConfigurationRequest { data }) = message.create_configuration_request {
        tracing::trace!("Making a config");

        let result = broker.apply_configuration(&data).await;

        tracing::trace!("Returned {:#?} from configuratin update.", result);

        if let Err(err) = result {
            let create_configuration_response = CreateConfigurationResponse {
                errors: vec![err.to_string()],
            };

            let mut result = BytesMut::new();

            Message {
                r#type: Type::Createconfigurationresponse as i32,
                create_configuration_response: Some(create_configuration_response),
                ..Default::default()
            }
            .encode(&mut result)
            .unwrap();

            let _ = writer_tx.send(result).await;
        } else {
            let create_configuration_response = CreateConfigurationResponse { errors: vec![] };

            tracing::info!("Responding with: {:#?}", create_configuration_response);

            let mut result = BytesMut::new();

            Message {
                correlation_id: message.correlation_id,
                r#type: Type::Createconfigurationresponse as i32,
                create_configuration_response: Some(create_configuration_response),
                ..Default::default()
            }
            .encode(&mut result)
            .unwrap();

            let result = writer_tx.send(result).await;
            tracing::info!("Result from writing: {:#?}", result);
        }
    } else {
        let create_configuration_response = CreateConfigurationResponse {
                errors: vec!["Malformed request for creating configuration. Please include CreateConfigurationRequest in body.".into()]
            };

        let mut result = BytesMut::new();

        Message {
            correlation_id: message.correlation_id,
            r#type: Type::Createconfigurationresponse as i32,
            create_configuration_response: Some(create_configuration_response),
            ..Default::default()
        }
        .encode(&mut result)
        .unwrap();

        tracing::info!("Responding with: {:#?}", result.clone().to_vec());

        writer_tx.send(result).await.unwrap();
    }
}
