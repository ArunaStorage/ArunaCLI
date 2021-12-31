use std::path::{Path, PathBuf};

use scienceobjectsdb_rust_api::sciobjectsdbapi::models::v1::{Dataset, Object};

const DATASET_DATA_DIR_NAME: &str = "_data";
const DATASET_INDEX_DIR_NAME: &str = "_index";

pub trait DownloadPathHandler {
    fn create_object_group_path(
        base_path: &Path,
        msg: &Object,
        object_group_name: String,
    ) -> PathBuf;
    fn dataset_index_path(base_path: &Path, dataset: &Dataset) -> PathBuf;
    fn create_file_path(base_path: &Path, object: &Object) -> PathBuf;
}

#[derive(Debug, Clone)]
pub struct CanonicalDownloadPathHandler {}

impl DownloadPathHandler for CanonicalDownloadPathHandler {
    fn create_object_group_path(
        base_path: &Path,
        object: &Object,
        object_group_name: String,
    ) -> PathBuf {
        let finalpath = base_path
            .join(object.project_id.clone())
            .join(object.dataset_id.clone())
            .join(DATASET_DATA_DIR_NAME)
            .join(object_group_name);
        finalpath
    }

    fn create_file_path(base_path: &Path, object: &Object) -> PathBuf {
        let full_file_name = format!("{}.{}", object.filename.clone(), object.filetype.clone());
        base_path.join(full_file_name)
    }

    fn dataset_index_path(base_path: &Path, dataset: &Dataset) -> PathBuf {
        base_path
            .join(dataset.project_id.clone())
            .join(dataset.id.clone())
            .join(DATASET_INDEX_DIR_NAME)
    }
}

#[derive(Debug, Clone)]
pub struct FlatpathDownloadManager {}

impl DownloadPathHandler for FlatpathDownloadManager {
    fn create_object_group_path(
        base_path: &Path,
        _msg: &Object,
        object_group_name: String,
    ) -> PathBuf {
        base_path.join(object_group_name)
    }

    fn dataset_index_path(base_path: &Path, _dataset: &Dataset) -> PathBuf {
        base_path.join(DATASET_INDEX_DIR_NAME)
    }

    fn create_file_path(base_path: &Path, object: &Object) -> PathBuf {
        let full_file_name = format!("{}.{}", object.filename.clone(), object.filetype.clone());
        base_path.join(full_file_name)
    }
}
