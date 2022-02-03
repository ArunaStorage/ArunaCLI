use std::{error, path::Path};

use crate::{client::client, util::cli};

use super::download_path_handler::DownloadPathHandler;
use async_channel::bounded;
use futures::{
    future::{try_join, try_join_all},
    StreamExt,
};
use scienceobjectsdb_rust_api::sciobjectsdbapi::{
    models::v1::{Object, PageRequest},
    services::v1::{
        CreateDownloadLinkRequest, GetDatasetObjectGroupsRequest, GetObjectGroupRequest,
        GetProjectDatasetsRequest,
    },
};

use tokio::io::AsyncWriteExt;

const DATASET_OBJECT_GROUP_PAGE_SIZE: u64 = 500;
const OBJECT_GROUP_DOWNLOAD_REQUEST_QUEUE_SIZE: usize = 500;

type Result<T> = std::result::Result<T, Box<dyn error::Error>>;

pub struct DownloadHandler {}

#[derive(Clone)]
pub struct ObjectDownloadMessage {
    pub object: Object,
    pub object_group_name: String,
}

impl DownloadHandler {
    pub async fn download<T: DownloadPathHandler>(request: cli::Load, client: client::Client) {
        let (sender, recv) = bounded(OBJECT_GROUP_DOWNLOAD_REQUEST_QUEUE_SIZE);
        let basepath = request.path.clone();
        let path = Path::new(basepath.as_str());

        let worker_future = match request.path_style {
            cli::DownloadPathStyle::Canonical => {
                DownloadHandler::start_download_worker::<T>(10, path, client.clone(), recv)
            }
            cli::DownloadPathStyle::Flat => {
                DownloadHandler::start_download_worker::<T>(10, path, client.clone(), recv)
            }
        };

        let resource_future = DownloadHandler::handle_resources(request, client.clone(), sender);

        try_join(worker_future, resource_future).await.unwrap();
    }

    async fn handle_resources(
        request: cli::Load,
        client: client::Client,
        sender: async_channel::Sender<ObjectDownloadMessage>,
    ) -> Result<()> {
        match request.resource {
            cli::Resource::Project => {
                DownloadHandler::download_project(client, request.id, sender).await?
            }
            cli::Resource::Dataset => {
                DownloadHandler::download_dataset(client, request.id, sender).await?
            }
            cli::Resource::DatasetVersion => todo!(),
            cli::Resource::ObjectGroup => {
                DownloadHandler::download_object_group(client, request.id, sender).await?
            }
        };

        Ok(())
    }

    async fn start_download_worker<T: DownloadPathHandler>(
        workercount: usize,
        basepath: &Path,
        client: client::Client,
        recv: async_channel::Receiver<ObjectDownloadMessage>,
    ) -> Result<()> {
        let mut worker = Vec::new();

        for _ in 0..workercount {
            worker.push(Self::download_object_loop::<T>(
                basepath,
                client.clone(),
                recv.clone(),
            ));
        }

        try_join_all(worker).await?;

        Ok(())
    }

    async fn download_project(
        mut client: client::Client,
        project_id: String,
        sender: async_channel::Sender<ObjectDownloadMessage>,
    ) -> Result<()> {
        let project_datasets = client
            .project_service
            .get_project_datasets(GetProjectDatasetsRequest { id: project_id })
            .await?
            .into_inner();

        for dataset in project_datasets.dataset {
            DownloadHandler::download_dataset(client.clone(), dataset.id, sender.clone()).await?;
        }

        Ok(())
    }

    async fn download_dataset(
        mut client: client::Client,
        dataset_id: String,
        sender: async_channel::Sender<ObjectDownloadMessage>,
    ) -> Result<()> {
        let mut prev_last_uuid = "".to_string();

        loop {
            let object_groups = client
                .dataset_service
                .get_dataset_object_groups(GetDatasetObjectGroupsRequest {
                    id: dataset_id.clone(),
                    page_request: Some(PageRequest {
                        page_size: DATASET_OBJECT_GROUP_PAGE_SIZE,
                        last_uuid: prev_last_uuid.clone(),
                    }),
                })
                .await?
                .into_inner();

            let object_group_count = object_groups.object_groups.len() as u64;
            for object_group in object_groups.object_groups {
                for object in object_group.objects {
                    prev_last_uuid = object.id.clone();
                    let msg = ObjectDownloadMessage {
                        object: object,
                        object_group_name: object_group.name.clone(),
                    };

                    sender.send(msg).await?;
                }
            }

            if object_group_count != DATASET_OBJECT_GROUP_PAGE_SIZE {
                break;
            }
        }

        Ok(())
    }

    async fn download_object_group(
        mut client: client::Client,
        object_group_id: String,
        sender: async_channel::Sender<ObjectDownloadMessage>,
    ) -> Result<()> {
        let object_group_response = client
            .dataset_object_service
            .get_object_group(GetObjectGroupRequest {
                id: object_group_id,
            })
            .await?
            .into_inner();
        let object_group = object_group_response.object_group.unwrap();

        for object in object_group.objects {
            let msg = ObjectDownloadMessage {
                object: object,
                object_group_name: object_group.name.clone(),
            };
            sender.send(msg).await?;
        }

        return Ok(());
    }

    async fn download_object_loop<T: DownloadPathHandler>(
        basepath: &Path,
        mut client: client::Client,
        mut recv: async_channel::Receiver<ObjectDownloadMessage>,
    ) -> Result<()> {
        while let Some(object_msg) = recv.next().await {
            let object_link = client
                .object_load_service
                .create_download_link(CreateDownloadLinkRequest {
                    id: object_msg.object.id,
                    ..Default::default()
                })
                .await?
                .into_inner();

            let object = &object_link.object.unwrap();
            let object_group_path =
                T::create_object_group_path(basepath, object, object_msg.object_group_name);

            tokio::fs::create_dir_all(&object_group_path).await?;
            let full_file_path = T::create_file_path(&object_group_path, object);
            let mut file = tokio::fs::File::create(full_file_path).await?;

            let mut get_response = reqwest::get(object_link.download_link).await?;
            while let Some(chunk) = get_response.chunk().await? {
                file.write(&chunk).await?;
            }

            file.flush().await?;
        }

        return Ok(());
    }
}
