use futures_util::StreamExt;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::v1::CreateEventStreamingGroupRequest;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::v1::NotficationStreamAck;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::v1::NotificationStreamGroupRequest;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::v1::NotificationStreamInit;

use scienceobjectsdb_rust_api::sciobjectsdbapi::services::v1::StreamAll;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::v1::create_event_streaming_group_request::StreamType;

use scienceobjectsdb_rust_api::sciobjectsdbapi::services::v1::create_event_streaming_group_request;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::v1::notification_stream_group_request::StreamAction::Init;
use scienceobjectsdb_rust_api::sciobjectsdbapi::services::v1::notification_stream_group_request::StreamAction::Ack;

use tonic::Request;

use crate::client::client;
use crate::util;

pub struct Events {
    client: client::Client,
}

impl Events {
    pub fn new(client: client::Client) -> Self {
        return Events { client: client };
    }

    pub async fn create_stream_consumer(&mut self, request: util::cli::CreateStreamConsumer) {
        let proto_resource = match request.resource {
            util::cli::Resource::Project => {
                create_event_streaming_group_request::EventResources::ProjectResource
            }
            util::cli::Resource::Dataset => {
                create_event_streaming_group_request::EventResources::DatasetResource
            }
            util::cli::Resource::DatasetVersion => {
                create_event_streaming_group_request::EventResources::DatasetVersionResource
            }
            util::cli::Resource::ObjectGroup => {
                create_event_streaming_group_request::EventResources::ObjectGroupResource
            }
        };

        let request = CreateEventStreamingGroupRequest {
            resource_id: request.id,
            resource: proto_resource.into(),
            stream_type: Some(StreamType::StreamAll(StreamAll {})),
            include_subresource: true,
            ..Default::default()
        };

        let response = self
            .client
            .notification_service
            .create_event_streaming_group(request)
            .await
            .unwrap()
            .into_inner();

        println!("ConsumerGroup ID: {:#?}", response.stream_group_id)
    }

    pub async fn stream_events(&mut self, request: util::cli::Stream) {
        let (send, recv) = async_channel::bounded(3);

        let outbound = async_stream::stream! {
            let init_request = NotificationStreamGroupRequest {
                close: false,
                stream_action: Some(Init(NotificationStreamInit {
                    stream_group_id: request.consumer_group_id,
                })),
            };

            yield init_request;

            loop {
                let ack_chunk_id = recv.recv().await.unwrap();
                let ack_request = NotificationStreamGroupRequest {
                    close: false,
                    stream_action: Some(Ack(NotficationStreamAck {
                        ack_chunk_id: vec![ack_chunk_id],
                    })),
                };

                yield ack_request
            }
        };

        let response = self
            .client
            .notification_service
            .notification_stream_group(Request::new(outbound))
            .await
            .unwrap();
        let mut notification_stream = response.into_inner();
        while let Some(notification_result) = notification_stream.next().await {
            let notification = notification_result.unwrap();
            for event in notification.notification {
                println!("{:?}", event)
            }

            send.send(notification.ack_chunk_id).await.unwrap();
        }
    }
}
