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
pub struct Stream {
    /// The resource type to describe
    #[clap(arg_enum, short = 'r')]
    pub resource: Resource,
    /// The id of the resource to describe
    #[clap(short = 'i')]
    pub id: String,
}


// Loads a given resource to disk
// There are two possible directory structures
// 1. Canonical (Default)
//   The canonical structure is based on the internal structure of the stored data, so the structure will always be
//   /<project_id>/<dataset_id>/_data/<object_group_name>/<object_name>. Datasetversions will be stored under
//   /<project_id>/<dataset_id>/_datasetversion/<object_group_name>/<object_name>
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
}

#[derive(PartialEq, Debug, ArgEnum, Clone)]
pub enum LsResource {
    ProjectDatasets,
    DatasetObjectGroups,
    DatasetVersions,
    DatasetVersionObjectGroups,
}
