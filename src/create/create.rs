use std::{collections::HashMap, path::Path};

use crate::{client::client, util::cli::CreateRequest};

use reqwest::Body;
use tokio_util::codec::{BytesCodec, FramedRead};

use tokio::io::AsyncReadExt;

use scienceobjectsdb_rust_api::sciobjectsdb::sciobjsdb::api::storage::{
    models,
    services::v1::{
        AddObjectRequest, CompleteMultipartUploadRequest, CompletedParts, CreateDatasetRequest,
        CreateObjectGroupRequest, CreateObjectGroupRevisionRequest, CreateObjectRequest,
        CreateUploadLinkRequest, GetMultipartUploadLinkRequest, ReleaseDatasetVersionRequest,
        StartMultipartUploadRequest, UpdateObjectsRequests,
    },
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

const UPLOAD_BUFFER_SIZE: usize = 5 * 1024 * 1024;

pub struct Create {
    client: client::Client,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateDataset {
    name: String,
    project_id: String,
    description: String,
    labels: Vec<Label>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateDatasetVersion {
    name: String,
    dataset_id: String,
    description: String,
    labels: Vec<Label>,
    objects_ids: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateObjectGroup {
    name: String,
    dataset_id: String,
    description: String,
    labels: Vec<Label>,
    objects_ids: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateObjectBatch {
    objects: Vec<CreateObject>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateObject {
    dataset_id: String,
    path: String,
    content_len: i64,
    filename: String,
    filetype: String,
    labels: Vec<Label>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Label {
    key: String,
    value: String,
}

impl Create {
    pub fn new(client: client::Client) -> Self {
        return Create { client: client };
    }

    pub async fn create(&mut self, request: CreateRequest) {
        match request.resource {
            crate::util::cli::CreateResource::Dataset => self.create_dataset(request).await,
            crate::util::cli::CreateResource::DatasetVersion => {
                self.create_dataset_version(request).await
            }
            crate::util::cli::CreateResource::ObjectGroup => {
                self.create_object_group(request).await
            }
            crate::util::cli::CreateResource::Object => self.create_objects(request).await,
        }
    }

    async fn create_dataset(&mut self, cli_request: CreateRequest) {
        let request: CreateDataset = self.read_request_file(cli_request.path).await;
        let labels = request
            .labels
            .into_iter()
            .map(|x| x.to_proto_label())
            .collect();

        let dataset = CreateDatasetRequest {
            name: request.name,
            description: request.description,
            labels: labels,
            project_id: request.project_id,
            ..Default::default()
        };

        self.client
            .dataset_service
            .create_dataset(dataset)
            .await
            .unwrap();
    }

    async fn create_dataset_version(&mut self, request: CreateRequest) {
        let dataset_version_request: CreateDatasetVersion =
            self.read_request_file(request.path).await;
        let labels = dataset_version_request
            .labels
            .into_iter()
            .map(|x| x.to_proto_label())
            .collect();

        let create_dataset_version_request = ReleaseDatasetVersionRequest {
            name: dataset_version_request.name,
            dataset_id: dataset_version_request.dataset_id,
            description: dataset_version_request.description,
            labels: labels,
            object_group_revision_ids: dataset_version_request.objects_ids,
            ..Default::default()
        };

        self.client
            .dataset_service
            .release_dataset_version(create_dataset_version_request)
            .await
            .unwrap();
    }

    async fn create_object_group(&mut self, request: CreateRequest) {
        let create_object_group_config: CreateObjectGroup =
            self.read_request_file(request.path.clone()).await;
        let labels = create_object_group_config
            .labels
            .into_iter()
            .map(|x| x.to_proto_label())
            .collect();

        let create_revision_request = match create_object_group_config.objects_ids {
            Some(object_ids) => {
                let ids: Vec<AddObjectRequest> = object_ids
                    .into_iter()
                    .map(|x| AddObjectRequest { id: x })
                    .collect();
                CreateObjectGroupRevisionRequest {
                    description: create_object_group_config.description,
                    include_object_link: false,
                    labels: labels,
                    name: create_object_group_config.name,
                    update_objects: Some(UpdateObjectsRequests {
                        add_objects: ids,
                        ..Default::default()
                    }),
                    ..Default::default()
                }
            }
            None => CreateObjectGroupRevisionRequest {
                description: create_object_group_config.description,
                include_object_link: false,
                labels: labels,
                name: create_object_group_config.name,
                update_objects: None,
                ..Default::default()
            },
        };
        let create_object_group_request = CreateObjectGroupRequest {
            dataset_id: create_object_group_config.dataset_id,
            create_revision_request: Some(create_revision_request),
        };

        self.client
            .dataset_object_service
            .create_object_group(create_object_group_request)
            .await
            .unwrap()
            .into_inner();
    }

    async fn create_objects(&mut self, request: CreateRequest) {
        let create_object_batch_config: CreateObjectBatch =
            self.read_request_file(request.path.clone()).await;
        let mut object_map = HashMap::new();

        for object in &create_object_batch_config.objects {
            let create_object_request = self.create_object_from_file(object).await;
            object_map.insert(object.path.to_owned(), create_object_request);
        }

        for (path, request) in object_map {
            let create_objects_response = self
                .client
                .dataset_object_service
                .create_object(request)
                .await
                .unwrap()
                .into_inner();
            let as_path = Path::new(path.as_str());
            let file = tokio::fs::File::open(as_path).await.unwrap();

            let size = file.metadata().await.unwrap().len() as usize;

            if size < UPLOAD_BUFFER_SIZE {
                self.upload_file(path.clone(), create_objects_response.id)
                    .await
            } else {
                self.upload_file_multipart(path.clone(), create_objects_response.id)
                    .await;
            }
        }
    }

    async fn upload_file_multipart(&mut self, path: String, object_id: String) {
        self.client
            .object_load_service
            .start_multipart_upload(StartMultipartUploadRequest {
                id: object_id.clone(),
            })
            .await
            .unwrap()
            .into_inner();

        let path = Path::new(path.as_str());
        let mut file = tokio::fs::File::open(path).await.unwrap();
        let mut remaining_bytes: usize = file.metadata().await.unwrap().len() as usize;

        let mut upload_part_counter: i64 = 0;
        let mut etags: Vec<CompletedParts> = Vec::new();

        loop {
            upload_part_counter = upload_part_counter + 1;

            let mut buffer_size = UPLOAD_BUFFER_SIZE;

            if remaining_bytes < UPLOAD_BUFFER_SIZE {
                buffer_size = remaining_bytes;
            }

            let mut data_buf = vec![0u8; buffer_size];
            file.read_buf(&mut data_buf).await.unwrap();

            let upload_link = self
                .client
                .object_load_service
                .get_multipart_upload_link(GetMultipartUploadLinkRequest {
                    object_id: object_id.clone(),
                    upload_part: upload_part_counter,
                })
                .await
                .unwrap()
                .into_inner();

            let etag = self.upload_part(upload_link.upload_link, data_buf).await;
            etags.push(CompletedParts {
                etag: etag,
                part: upload_part_counter,
            });

            remaining_bytes = remaining_bytes - buffer_size;

            if remaining_bytes == 0 {
                break;
            }
        }

        self.client
            .object_load_service
            .complete_multipart_upload(CompleteMultipartUploadRequest {
                object_id: object_id.clone(),
                parts: etags,
            })
            .await
            .unwrap();
    }

    async fn upload_file(&mut self, path: String, object_id: String) {
        let upload_link = self
            .client
            .object_load_service
            .create_upload_link(CreateUploadLinkRequest { id: object_id })
            .await
            .unwrap()
            .into_inner();

        let path = Path::new(path.as_str());
        let file = tokio::fs::File::open(path).await.unwrap();

        let client = reqwest::Client::new();

        let stream = FramedRead::new(file, BytesCodec::new());
        let body = Body::wrap_stream(stream);

        client
            .put(upload_link.upload_link)
            .body(body)
            .send()
            .await
            .unwrap();
    }

    async fn upload_part(&mut self, upload_link: String, data_buf: Vec<u8>) -> String {
        let client = reqwest::Client::new();
        let response = client.put(upload_link).body(data_buf).send().await.unwrap();
        let etag_raw = response.headers().get("ETag").unwrap().as_bytes();
        let etag = std::str::from_utf8(etag_raw).unwrap().to_string();

        return etag;
    }

    async fn create_object_from_file(&self, create_object: &CreateObject) -> CreateObjectRequest {
        let path = Path::new(create_object.path.as_str());
        let file = tokio::fs::File::open(path).await.unwrap();
        let filename = path.file_stem().unwrap().to_str().unwrap().to_string();
        let file_extension = match path.extension() {
            Some(value) => value.to_str().unwrap().to_string(),
            None => "".to_string(),
        };

        let labels = create_object
            .labels
            .iter()
            .map(|x| x.to_proto_label())
            .collect();
        let create_object_request = CreateObjectRequest {
            dataset_id: create_object.dataset_id.clone(),
            content_len: file.metadata().await.unwrap().len() as i64,
            filename: filename,
            filetype: file_extension,
            labels: labels,
            ..Default::default()
        };

        return create_object_request;
    }

    async fn read_request_file<Z: DeserializeOwned>(&self, file_path: String) -> Z {
        let data = tokio::fs::read_to_string(file_path).await.unwrap();
        let create_request: Z = serde_yaml::from_str(data.as_str()).unwrap();

        return create_request;
    }
}

impl Label {
    fn to_proto_label(&self) -> models::v1::Label {
        let label = models::v1::Label {
            key: self.key.clone(),
            value: self.value.clone(),
        };

        return label;
    }
}
