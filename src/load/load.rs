use std::path::Path;
use futures::future::{try_join_all};
use scienceobjectsdb_rust_api::sciobjectsdbapi::models::v1::{Object, ObjectGroup};
use tokio::io::AsyncWriteExt;
use crate::{client::client, util::cli};

pub struct Load {
    client: client::Client,
}

impl Load {
    pub async fn load(&self, request: cli::Load) {
        match request.resource {
            cli::Resource::Project => todo!(),
            cli::Resource::Dataset => todo!(),
            cli::Resource::DatasetVersion => todo!(),
            cli::Resource::ObjectGroup => todo!(),
        }
    }

    async fn load_object_groups(&self, basepath: &Path, object_groups: Vec<ObjectGroup>) {
        let object_load_futures = Vec::new();

        for object_group in object_groups {
            for object in object_group.objects {
            }
        }
    }

    async fn load_object_data(&self, url: String, target_file_path: &Path) {
        let mut file = tokio::fs::File::create(target_file_path).await.unwrap();
        let mut url_response = reqwest::get(url).await.unwrap();

        while let Some(chunk) = url_response.chunk().await.unwrap() {
            file.write_all(&chunk).await.unwrap();
        }

        file.flush().await.unwrap();
    }
}   