use clap::{ArgEnum, Parser};

#[derive(Parser)]
pub struct Ls {
    /// The targeted resource
    #[clap(arg_enum)]
    pub resource: LsResource,
    /// The targeted resource parent id
    pub id: String,
}

#[derive(Parser)]
pub struct Describe {
    /// The resource type to describe
    #[clap(arg_enum, short = 'r')]
    pub resource: Resource,
    #[clap(short = 'i')]
    /// The id of the resource to describe
    pub id: String,
}

#[derive(Parser)]
pub struct CreateStreamConsumer {
    /// The resource type to describe
    #[clap(arg_enum, short = 'r')]
    pub resource: Resource,
    /// The id of the resource to describe
    #[clap(short = 'i')]
    pub id: String,
}

#[derive(Parser)]
pub struct Stream {
    /// The id of the resource to describe
    #[clap(short = 'i')]
    pub consumer_group_id: String,
}

#[derive(Parser)]
pub struct Load {
    /// The resource type to load
    #[clap(arg_enum, short = 'r')]
    pub resource: Resource,
    /// The id of the resource to load
    #[clap(short = 'i')]
    pub id: String,
    /// The base path of the loaded resource
    #[clap(short = 'p')]
    pub path: String,
    /// Download target path style
    #[clap(arg_enum, short = 's', default_value = "canonical")]
    pub path_style: DownloadPathStyle,
}

#[derive(Parser)]
pub struct CreateRequest {
    /// The resource type to create
    #[clap(arg_enum, short = 'r')]
    pub resource: CreateResource,
    /// The path to the resource specification file. Examples can be found
    /// under examples
    #[clap(short = 'p')]
    pub path: String,
}

#[derive(Parser)]
pub struct UpdateRequest {
    /// The operation to apply to the Object Group (currently only delete and add)
    #[clap(arg_enum, short = 'o')]
    pub operation: UpdateResource,
    /// The path to the resource specification file. Examples can be found
    /// under examples
    #[clap(short = 'p')]
    pub path: String,
}

#[derive(PartialEq, Debug, ArgEnum, Clone)]
pub enum UpdateResource {
    Delete,
    Add,
}

#[derive(PartialEq, Debug, ArgEnum, Clone)]
pub enum Resource {
    Project,
    Dataset,
    DatasetVersion,
    ObjectGroup,
}

#[derive(PartialEq, Debug, ArgEnum, Clone)]
pub enum CreateResource {
    Dataset,
    DatasetVersion,
    ObjectGroup,
    ObjectGroupFromFile,
    Object,
}

#[derive(PartialEq, Debug, ArgEnum, Clone)]
pub enum LsResource {
    ProjectDatasets,
    DatasetObjectGroups,
    DatasetVersions,
    DatasetVersionObjectGroups,
    DatasetObjects,
}

#[derive(PartialEq, Debug, ArgEnum, Clone)]
pub enum DownloadPathStyle {
    Canonical,
    Flat,
}
