use scienceobjectsdb_rust_api::sciobjectsdb::sciobjsdb::api::storage::services::v1::{
    GetDatasetRequest, GetDatasetVersionRequest, GetObjectGroupRequest, GetProjectRequest,
};

use crate::client::client;
use crate::util::cli;

pub struct Describe {
    client: client::Client,
}

impl Describe {
    pub fn new(client: client::Client) -> Self {
        return Describe { client: client };
    }

    pub async fn describe(&mut self, request: cli::Describe) {
        match request.resource {
            cli::Resource::Project => {
                let project = self
                    .client
                    .project_service
                    .get_project(GetProjectRequest { id: request.id })
                    .await
                    .unwrap()
                    .into_inner();

                println!("{:#?}", project)
            }
            cli::Resource::Dataset => {
                let dataset = self
                    .client
                    .dataset_service
                    .get_dataset(GetDatasetRequest { id: request.id })
                    .await
                    .unwrap()
                    .into_inner();

                println!("{:#?}", dataset)
            }
            cli::Resource::DatasetVersion => {
                let dataset_version = self
                    .client
                    .dataset_service
                    .get_dataset_version(GetDatasetVersionRequest { id: request.id })
                    .await
                    .unwrap()
                    .into_inner();

                println!("{:#?}", dataset_version)
            }
            cli::Resource::ObjectGroup => {
                let object_group = self
                    .client
                    .dataset_object_service
                    .get_object_group(GetObjectGroupRequest {
                        id: request.id,
                        // Not sure how to deal with that:
                        // user maybe needs to specifiy which version but
                        // describing can also be verbose
                        pagination: None,
                    })
                    .await
                    .unwrap()
                    .into_inner();

                println!("{:#?}", object_group)
            }
        }
    }
}
