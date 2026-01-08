use super::{Client, error::HigginsClientError};
use higgins_codec::{
    CreateConfigurationResponse, CreateSubscriptionResponse, DeleteConfigurationResponse,
    GetIndexResponse, Message, MetadataResponse, Pong, ProduceResponse, TakeRecordsResponse,
    UploadModuleResponse, frame::Frame, message::Type,
};
use prost::Message as _;

pub enum Response {
    CreateConfiguration(CreateConfigurationResponse),
    CreateSubscription(CreateSubscriptionResponse),
    DeleteConfiguration(DeleteConfigurationResponse),
    GetIndex(GetIndexResponse),
    Metadata(MetadataResponse),
    Pong(Pong),
    Produce(ProduceResponse),
    TakeRecords(TakeRecordsResponse),
    UploadModule(UploadModuleResponse),
}

impl TryFrom<Message> for Response {
    type Error = HigginsClientError;

    fn try_from(value: Message) -> Result<Self, Self::Error> {
        match Type::try_from(value.r#type).map_err(|err| {
            tracing::error!("Error when trying to convert enum value: {:#?}", err);
            HigginsClientError::UnexpectedMessageReceived(value.r#type)
        })? {
            Type::Createconfigurationresponse => Ok(Response::CreateConfiguration(
                value.create_configuration_response.unwrap(),
            )),
            Type::Createsubscriptionresponse => Ok(Response::CreateSubscription(
                value.create_subscription_response.unwrap(),
            )),
            Type::Deleteconfigurationresponse => Ok(Response::DeleteConfiguration(
                value.delete_configuration_response.unwrap(),
            )),
            Type::Getindexresponse => Ok(Response::GetIndex(value.get_index_response.unwrap())),
            Type::Metadataresponse => Ok(Response::Metadata(value.metadata_response.unwrap())),
            Type::Pong => Ok(Response::Pong(value.pong.unwrap())),
            Type::Produceresponse => Ok(Response::Produce(value.produce_response.unwrap())),
            Type::Takerecordsresponse => {
                Ok(Response::TakeRecords(value.take_records_response.unwrap()))
            }
            Type::Uploadmoduleresponse => Ok(Response::UploadModule(
                value.upload_module_response.unwrap(),
            )),
            _ => Err(HigginsClientError::UnexpectedMessageReceived(value.r#type)),
        }
    }
}

impl Client {
    /// Awaits on the socket, listening for any specific
    /// responses that may be coming.
    pub async fn recv(&mut self) -> Result<Response, HigginsClientError> {
        let frame = Frame::try_read_async(&mut self.0).await?;

        let slice = frame.inner();

        let message = Message::decode(slice).unwrap();

        Response::try_from(message)
    }
}
