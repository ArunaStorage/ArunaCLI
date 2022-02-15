use scienceobjectsdb_rust_api::sciobjectsdb::sciobjsdb::api::{
    notification::services::v1::update_notification_service_client,
    storage::services::v1::{
        dataset_objects_service_client, dataset_service_client, object_load_service_client,
        project_service_client::{self},
    },
};

use tonic::codegen::InterceptedService;
use tonic::metadata::AsciiMetadataKey;
use tonic::metadata::AsciiMetadataValue;
use tonic::transport::Channel;

const API_TOKEN_ENTRY_KEY: &str = "API_TOKEN";

#[derive(Clone)]
pub struct Client {
    pub project_service: project_service_client::ProjectServiceClient<
        InterceptedService<Channel, ClientInterceptor>,
    >,
    pub dataset_service: dataset_service_client::DatasetServiceClient<
        InterceptedService<Channel, ClientInterceptor>,
    >,
    pub dataset_object_service: dataset_objects_service_client::DatasetObjectsServiceClient<
        InterceptedService<Channel, ClientInterceptor>,
    >,
    pub object_load_service: object_load_service_client::ObjectLoadServiceClient<
        InterceptedService<Channel, ClientInterceptor>,
    >,
    pub notification_service: update_notification_service_client::UpdateNotificationServiceClient<
        InterceptedService<Channel, ClientInterceptor>,
    >,
}

#[derive(Clone)]
pub struct ClientInterceptor {
    api_token: String,
}

impl Client {
    pub async fn new(channel: Channel, api_token: String) -> Self {
        let interceptor = ClientInterceptor {
            api_token: api_token,
        };

        let client = Client{
            project_service: project_service_client::ProjectServiceClient::with_interceptor(channel.clone(), interceptor.clone()),
            dataset_service: dataset_service_client::DatasetServiceClient::with_interceptor(channel.clone(), interceptor.clone()),
            dataset_object_service: dataset_objects_service_client::DatasetObjectsServiceClient::with_interceptor(channel.clone(), interceptor.clone()),
            object_load_service: object_load_service_client::ObjectLoadServiceClient::with_interceptor(channel.clone(), interceptor.clone()),
            notification_service: update_notification_service_client::UpdateNotificationServiceClient::with_interceptor(channel, interceptor.clone()),
        };

        return client;
    }
}

impl tonic::service::Interceptor for ClientInterceptor {
    fn call(&mut self, request: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let mut mut_req: tonic::Request<()> = request;
        let metadata = mut_req.metadata_mut();
        metadata.append(
            AsciiMetadataKey::from_bytes(API_TOKEN_ENTRY_KEY.as_bytes()).unwrap(),
            AsciiMetadataValue::from_str(self.api_token.as_str()).unwrap(),
        );

        return Ok(mut_req);
    }
}
