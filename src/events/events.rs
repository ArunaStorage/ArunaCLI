use futures_util::StreamExt;

use crate::client::client;
use crate::util;

use scienceobjectsdb_rust_api::sciobjectsdbapi::services::v1::notification_stream_request;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::v1::NotificationStreamRequest;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::v1::StreamFromDate;

pub struct Events {
    client: client::Client,
}

impl Events {
    pub fn new(client: client::Client) -> Self {
        return Events { client: client };
    }

    pub async fn stream_events(&mut self, request: util::cli::Stream) {
        let stream_from_date = StreamFromDate {
            timestamp: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
        };

        let proto_resource = match request.resource {
            util::cli::Resource::Project => {
                notification_stream_request::EventResources::ProjectResource
            }
            util::cli::Resource::Dataset => {
                notification_stream_request::EventResources::DatasetResource
            }
            util::cli::Resource::DatasetVersion => {
                notification_stream_request::EventResources::DatasetVersionResource
            }
            util::cli::Resource::ObjectGroup => {
                notification_stream_request::EventResources::ObjectGroupResource
            }
        };

        let request = NotificationStreamRequest {
            stream_type: Some(notification_stream_request::StreamType::StreamFromDate(
                stream_from_date,
            )),
            resource: proto_resource as i32,
            resource_id: request.id,
            include_subresource: true,
        };

        let mut stream = self
            .client
            .notification_service
            .notification_stream(request)
            .await
            .unwrap()
            .into_inner();
        loop {
            let event = stream.next().await.unwrap().unwrap();
            println!("{:#?}", event)
        }
    }
}
