use crate::client::client;
use crate::util::cli;

use scienceobjectsdb_rust_api::sciobjectsdb::sciobjsdb::api::storage::services::v1::{
    GetDatasetObjectGroupsRequest, GetDatasetVersionObjectGroupsRequest, GetDatasetVersionsRequest,
    GetProjectDatasetsRequest,
};

pub struct LS {
    client: client::Client,
}

impl LS {
    pub fn new(client: client::Client) -> Self {
        return LS { client: client };
    }

    pub async fn ls(&mut self, request: cli::Ls) {
        match request.resource {
            cli::LsResource::ProjectDatasets => self.ls_project_dataset(request).await,
            cli::LsResource::DatasetObjectGroups => self.ls_dataset_object_groups(request).await,
            cli::LsResource::DatasetVersions => self.ls_dataset_versions(request).await,
            cli::LsResource::DatasetVersionObjectGroups => {
                self.ls_dataset_version_object_groups(request).await
            }
        }
    }

    async fn ls_project_dataset(&mut self, request: cli::Ls) {
        let datasets = self
            .client
            .project_service
            .get_project_datasets(GetProjectDatasetsRequest { id: request.id })
            .await
            .unwrap()
            .into_inner();

        println!("{:#?}", datasets.datasets)
    }

    async fn ls_dataset_object_groups(&mut self, request: cli::Ls) {
        let object_groups = self
            .client
            .dataset_service
            .get_dataset_object_groups(GetDatasetObjectGroupsRequest {
                id: request.id,
                page_request: None,
            })
            .await
            .unwrap()
            .into_inner();

        println!("{:#?}", object_groups.object_groups)
    }

    async fn ls_dataset_versions(&mut self, request: cli::Ls) {
        let dataset_versions = self
            .client
            .dataset_service
            .get_dataset_versions(GetDatasetVersionsRequest { id: request.id })
            .await
            .unwrap()
            .into_inner();

        println!("{:#?}", dataset_versions.dataset_versions)
    }

    async fn ls_dataset_version_object_groups(&mut self, request: cli::Ls) {
        let object_groups = self
            .client
            .dataset_service
            .get_dataset_version_object_groups(GetDatasetVersionObjectGroupsRequest {
                id: request.id,
                page_request: None,
            })
            .await
            .unwrap()
            .into_inner();

        println!("{:#?}", object_groups.object_group)
    }
}
