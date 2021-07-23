package subcmd

import (
	"fmt"
	"log"

	services "github.com/ScienceObjectsDB/go-api/api/services/v1"
)

//LS Generic list struct
type LS struct {
	*GrpcClients
}

func (ls *LS) ObjectGroupVersions(objectGroupID string) error {
	id := services.GetObjectGroupRevisionsRequest{
		Id: objectGroupID,
	}

	versions, err := ls.ObjectGroupClient.GetObjectGroupRevisions(ls.OutGoingContext(), &id)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	for _, version := range versions.GetObjectGroupRevision() {
		println(fmt.Sprintf("%v", version))
	}

	return nil
}

func (ls *LS) ProjectLS() error {
	projects, err := ls.ProjectClient.GetUserProjects(ls.OutGoingContext(), &services.GetUserProjectsRequest{})
	if err != nil {
		log.Println(err.Error())
		return err
	}

	for _, project := range projects.GetProjects() {
		println(fmt.Sprintf("%v", project))
	}

	return nil
}

//DatasetsLS Lists all datasets of a project
func (ls *LS) DatasetsLS(projectid string) error {
	id := services.GetProjectDatasetsRequest{
		Id: projectid,
	}

	datasets, err := ls.ProjectClient.GetProjectDatasets(ls.OutGoingContext(), &id)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	for _, dataset := range datasets.GetDataset() {
		println(fmt.Sprintf("%v", dataset))
	}

	return nil
}

func (ls *LS) DatasetObjectGroupsLS(datasetid string) error {
	id := services.GetDatasetObjectGroupsRequest{
		Id: datasetid,
	}

	objectgroups, err := ls.DatasetClient.GetDatasetObjectGroups(ls.OutGoingContext(), &id)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	for _, objectgroup := range objectgroups.GetObjectGroups() {
		println(fmt.Sprintf("%v - %v - %v - %v", objectgroup.Name, objectgroup.Id, objectgroup.Status, objectgroup.Labels))
	}

	return nil
}
