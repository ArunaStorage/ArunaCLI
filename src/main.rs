mod client;
mod create;
mod describe;
mod events;
mod load;
mod ls;
mod util;

use clap::{AppSettings, Parser, Subcommand};
use tonic::transport::ClientTlsConfig;

#[derive(Parser)]
#[clap(setting(AppSettings::SubcommandRequiredElseHelp))]
#[clap(about, version, author)]
struct Cli {
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
    /// Creates the given resource type from the given file
    Create(util::cli::CreateRequest),
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let tls_config = ClientTlsConfig::new();

    let config = util::config::Config::new().await;
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
    };
}
