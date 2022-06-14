use serde::{Deserialize, Serialize};
use serde_yaml;
use tokio::io::AsyncReadExt;

use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    pub api_key: String,
    pub endpoint: String,
}

const DEFAULT_CONF_PATH: [&str; 2] = [".sciobjsdb/config.yaml", ".config/sciobjsdb/config.yaml"];

impl Config {
    pub async fn new() -> Self {
        let dirs = directories::UserDirs::new().unwrap();
        for conf_path in DEFAULT_CONF_PATH {
            let search_path = Path::new(conf_path);
            let homedir = dirs.home_dir();
            let mut path_buf = PathBuf::new();
            path_buf.push(homedir);
            path_buf.push(search_path);
            let path = path_buf.as_path();
            if path.is_file() {
                let mut data = String::new();
                let mut conf_file = tokio::fs::File::open(path).await.unwrap();
                conf_file.read_to_string(&mut data).await.unwrap();
                let conf: Config = serde_yaml::from_str(data.as_str()).unwrap();

                return conf;
            }
        }

        panic!("could not find default config")
    }
    pub async fn specified_path(config_path: &str) -> Self {
        let config_path = Path::new(config_path);
        if config_path.is_file() {
            let mut data = String::new();
            let mut conf_file = tokio::fs::File::open(config_path).await.unwrap();
            conf_file.read_to_string(&mut data).await.unwrap();
            let conf: Config = serde_yaml::from_str(data.as_str()).unwrap();

            return conf;
        }

        panic!("could not find specified config {:?}", config_path)
    }
}
