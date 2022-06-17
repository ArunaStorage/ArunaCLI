use crate::{client::client, util::cli::UpdateRequest};

use scienceobjectsdb_rust_api::sciobjectsdb::sciobjsdb::api::storage::services::v1::{
    AddObjectRequest, CreateObjectGroupRevisionRequest, DeleteObjectRequest,
    UpdateObjectGroupRequest, UpdateObjectsRequests,
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub struct Update {
    client: client::Client,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UpdateObjectGroup {
    dataset_id: String,
    objectgroup_id: String,
    objects_ids: Vec<String>,
}

impl Update {
    pub fn new(client: client::Client) -> Self {
        return Update { client: client };
    }

    pub async fn update(&mut self, request: UpdateRequest) {
        match request.operation {
            crate::util::cli::UpdateResource::Delete => self.delete_objects(request).await,
            crate::util::cli::UpdateResource::Add => self.add_objects(request).await,
        }
    }

    async fn delete_objects(&mut self, request: UpdateRequest) {
        let delete_objects_config: UpdateObjectGroup =
            self.read_request_file(request.path.clone()).await;

        let objects: Vec<DeleteObjectRequest> = delete_objects_config
            .objects_ids
            .into_iter()
            .map(|object_id| DeleteObjectRequest { id: object_id })
            .collect();

        let delete_request = UpdateObjectsRequests {
            delete_objects: objects,
            ..Default::default()
        };

        let object_group_revision_request = CreateObjectGroupRevisionRequest {
            update_objects: Some(delete_request),
            object_group_id: delete_objects_config.objectgroup_id.clone(),
            ..Default::default()
        };

        let request = UpdateObjectGroupRequest {
            id: delete_objects_config.objectgroup_id.clone(),
            create_revision_request: Some(object_group_revision_request),
        };

        self.client
            .dataset_object_service
            .update_object_group(request)
            .await
            .unwrap();
    }

    async fn add_objects(&mut self, request: UpdateRequest) {
        let add_objects_config: UpdateObjectGroup =
            self.read_request_file(request.path.clone()).await;

        let objects: Vec<AddObjectRequest> = add_objects_config
            .objects_ids
            .into_iter()
            .map(|object_id| AddObjectRequest { id: object_id })
            .collect();

        let add_request = UpdateObjectsRequests {
            add_objects: objects,
            ..Default::default()
        };

        let object_group_revision_request = CreateObjectGroupRevisionRequest {
            update_objects: Some(add_request),
            ..Default::default()
        };

        let request = UpdateObjectGroupRequest {
            id: add_objects_config.objectgroup_id,
            create_revision_request: Some(object_group_revision_request),
        };

        self.client
            .dataset_object_service
            .update_object_group(request)
            .await
            .unwrap();
    }

    pub async fn read_request_file<Z: DeserializeOwned>(&self, file_path: String) -> Z {
        let data = tokio::fs::read_to_string(file_path).await.unwrap();
        let create_request: Z = serde_yaml::from_str(data.as_str()).unwrap();

        return create_request;
    }
}
