use std::path::Path;

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

    async fn load_object_group(&self, basepath: &Path) {
        
    }

    async fn load_object_group_data(&self, basepath: &Path, links: Vec<String>) {
    }
}