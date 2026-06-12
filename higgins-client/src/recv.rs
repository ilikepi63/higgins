use super::{Client, error::HigginsClientError};
use higgins_codec::{
    AcknowledgeSubscriptionOffsetsResponse, CreateConfigurationResponse,
    CreateSubscriptionResponse, DeleteConfigurationResponse, GetCurrentTopographyResponse,
    GetIndexResponse, GetSubscriptionResponse, Message, MetadataResponse, Pong, ProduceResponse,
    TakeRecordsResponse, UploadModuleResponse, frame::Frame, message::Type,
};
use prost::Message as _;

#[derive(Debug)]
pub struct ResponseMetadata {
    pub correlation_id: Option<u64>,
}

#[derive(Debug)]
pub struct Response {
    pub metadata: ResponseMetadata,
    pub body: ResponseBody,
}

#[derive(Debug)]
pub enum ResponseBody {
    CreateConfiguration(CreateConfigurationResponse),
    CreateSubscription(CreateSubscriptionResponse),
    DeleteConfiguration(DeleteConfigurationResponse),
    GetIndex(GetIndexResponse),
    Metadata(MetadataResponse),
    Pong(Pong),
    Produce(ProduceResponse),
    TakeRecords(TakeRecordsResponse),
    GetCurrentTopography(GetCurrentTopographyResponse),
    UploadModule(UploadModuleResponse),
    GetSubscription(GetSubscriptionResponse),
    Acknowledge(AcknowledgeSubscriptionOffsetsResponse),
}

impl TryFrom<Message> for ResponseBody {
    type Error = HigginsClientError;

    fn try_from(value: Message) -> Result<Self, Self::Error> {
        match Type::try_from(value.r#type).map_err(|err| {
            tracing::error!("Error when trying to convert enum value: {:#?}", err);
            HigginsClientError::UnexpectedMessageReceived(value.r#type)
        })? {
            Type::Createconfigurationresponse => Ok(ResponseBody::CreateConfiguration(
                value
                    .create_configuration_response
                    .ok_or(HigginsClientError::MissingPayload)?,
            )),
            Type::Createsubscriptionresponse => Ok(ResponseBody::CreateSubscription(
                value
                    .create_subscription_response
                    .ok_or(HigginsClientError::MissingPayload)?,
            )),
            Type::Deleteconfigurationresponse => Ok(ResponseBody::DeleteConfiguration(
                value
                    .delete_configuration_response
                    .ok_or(HigginsClientError::MissingPayload)?,
            )),
            Type::Getindexresponse => Ok(ResponseBody::GetIndex(
                value
                    .get_index_response
                    .ok_or(HigginsClientError::MissingPayload)?,
            )),
            Type::Metadataresponse => Ok(ResponseBody::Metadata(
                value
                    .metadata_response
                    .ok_or(HigginsClientError::MissingPayload)?,
            )),
            Type::Pong => Ok(ResponseBody::Pong(
                value.pong.ok_or(HigginsClientError::MissingPayload)?,
            )),
            Type::Produceresponse => Ok(ResponseBody::Produce(
                value
                    .produce_response
                    .ok_or(HigginsClientError::MissingPayload)?,
            )),
            Type::Takerecordsresponse => Ok(ResponseBody::TakeRecords(
                value
                    .take_records_response
                    .ok_or(HigginsClientError::MissingPayload)?,
            )),
            Type::Uploadmoduleresponse => Ok(ResponseBody::UploadModule(
                value
                    .upload_module_response
                    .ok_or(HigginsClientError::MissingPayload)?,
            )),
            Type::Getcurrenttopographyresponse => Ok(ResponseBody::GetCurrentTopography(
                value
                    .get_current_topography_response
                    .ok_or(HigginsClientError::MissingPayload)?,
            )),
            Type::Getsubscriptionresponse => Ok(ResponseBody::GetSubscription(
                value
                    .get_subscription_response
                    .ok_or(HigginsClientError::MissingPayload)?,
            )),
            Type::Acknowledgeresponse => Ok(ResponseBody::Acknowledge(
                value
                    .acknowledge_response
                    .ok_or(HigginsClientError::MissingPayload)?,
            )),
            _ => Err(HigginsClientError::UnexpectedMessageReceived(value.r#type)),
        }
    }
}

impl Client {
    /// Awaits on the socket, listening for any specific
    /// responses that may be coming.
    pub async fn recv(
        &mut self,
        timeout: Option<std::time::Duration>,
    ) -> Result<Response, HigginsClientError> {
        let frame = match timeout {
            Some(duration) => {
                tokio::time::timeout(duration, Frame::try_read_async(&mut self.0)).await??
            }
            None => Frame::try_read_async(&mut self.0).await?,
        };

        let slice = frame.inner();

        let message = Message::decode(slice)?;

        if let Some(id) = message.correlation_id {
            self.2.remove(id);
        }

        Ok(Response {
            metadata: ResponseMetadata {
                correlation_id: message.correlation_id,
            },
            body: ResponseBody::try_from(message)?,
        })
    }
}
