#![feature(generators, generator_trait)]

mod client;
mod create;
mod describe;
mod download;
mod events;
mod ls;
mod update;
mod util;

use clap::{Parser, Subcommand};
use tonic::transport::ClientTlsConfig;
use util::config;

use crate::download::download_path_handler::CanonicalDownloadPathHandler;
use crate::download::download_path_handler::FlatpathDownloadManager;

#[derive(Parser)]
#[clap(subcommand_required = true, arg_required_else_help = true)]
#[clap(about, version, author)]
struct Cli {
    #[clap(short)]
    /// Path to the config file. Defaults to: .config/sciobjsdbcli/config.yaml or .sciobjsdb/config.yaml
    config: Option<String>,
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Lists all associated subresource of the given resource
    Ls(util::cli::Ls),
    /// Displays details of the given resource
    Describe(util::cli::Describe),
    /// The event stream of the given ressource
    EventStream(util::cli::Stream),
    /// Creates a consumer for the event group
    CreateStreamConsumer(util::cli::CreateStreamConsumer),
    /// Creates the given resource type from the given file
    Create(util::cli::CreateRequest),
    /// Loads a given resource to disk
    /// There are two possible directory structures {n}\
    /// 1. Canonical (Default)
    ///   The canonical structure is based on the internal structure of the stored data, so the structure will always be
    ///   /<project_id>/<dataset_id>/_data/<object_group_name>/<object_name>. Datasetversions will be stored under
    ///   /<project_id>/<dataset_id>/_datasetversion/<object_group_name>/<object_name>
    Load(util::cli::Load),
    /// Updates Objects inside the ObjectGroup
    Update(util::cli::UpdateRequest),
}

#[tokio::main]
async fn main() {
    console_subscriber::init();

    let cli = Cli::parse();

    let tls_config = ClientTlsConfig::new();

    let config = match cli.config {
        Some(config) => util::config::Config::specified_path(config.as_str()).await,
        None => util::config::Config::new().await,
    };
    let mut endpoint = tonic::transport::Channel::from_shared(config.endpoint).unwrap();
    endpoint = endpoint.tls_config(tls_config).unwrap();

    let channel = endpoint.connect().await.unwrap();
    let client = client::client::Client::new(channel, config.api_key.clone()).await;

    match cli.command {
        Commands::Ls(request) => {
            let mut ls = ls::ls::LS::new(client.clone());
            ls.ls(request).await;
        }
        Commands::Describe(request) => {
            let mut describe = describe::describe::Describe::new(client.clone());
            describe.describe(request).await;
        }
        Commands::EventStream(request) => {
            let mut stream = events::events::Events::new(client.clone());
            stream.stream_events(request).await;
        }
        Commands::Create(request) => {
            let mut create = create::create::Create::new(client.clone());
            create.create(request).await;
        }
        Commands::Load(request) => match request.path_style {
            util::cli::DownloadPathStyle::Canonical => {
                download::download_handler::DownloadHandler::download::<CanonicalDownloadPathHandler>(
                    request,
                    client.clone(),
                )
                .await
            }
            util::cli::DownloadPathStyle::Flat => {
                download::download_handler::DownloadHandler::download::<FlatpathDownloadManager>(
                    request,
                    client.clone(),
                )
                .await
            }
        },
        Commands::CreateStreamConsumer(request) => {
            let mut stream = events::events::Events::new(client.clone());
            stream.create_stream_consumer(request).await;
        },
        Commands::Update(request) => {
            let mut update = update::update::Update::new(client.clone());
            update.update(request).await;
        }
    };
}
