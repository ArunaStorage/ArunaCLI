package subcmd

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	models "github.com/ScienceObjectsDB/go-api/api/models/v1"
	services "github.com/ScienceObjectsDB/go-api/api/services/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
)

//Create Generic list struct
type CreateClient struct {
	*GrpcClients
}

func (create *CreateClient) Create(resource models.Resource, filepath string) {
	switch resource {
	case models.Resource_OBJEFT_GROUP_RESOURCE:
		create.createObjectGroup(filepath)
	case models.Resource_PROJECT_RESOURCE:
		create.createProject(filepath)
	case models.Resource_DATASET_RESOURCE:
		create.createDataset(filepath)
	}
}

func (create *CreateClient) createDataset(filepath string) error {
	datasetRequest := services.CreateDatasetRequest{}
	err := create.loadConfigWrapper(filepath, &datasetRequest)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	project, err := create.DatasetClient.CreateDataset(create.OutGoingContext(), &datasetRequest)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	println(fmt.Sprintf("%v", project))

	return nil
}

func (create *CreateClient) createProject(filepath string) error {
	projectRequest := services.CreateProjectRequest{}

	err := create.loadConfigWrapper(filepath, &projectRequest)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	project, err := create.ProjectClient.CreateProject(create.OutGoingContext(), &projectRequest)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	println(fmt.Sprintf("%v", project))

	return nil
}

func (create *CreateClient) createObjectGroup(filepath string) error {
	withRevisionGroupStruct := services.CreateObjectGroupRequest{}

	err := create.loadConfigWrapper(filepath, &withRevisionGroupStruct)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	group, err := create.ObjectGroupClient.CreateObjectGroup(create.OutGoingContext(), &withRevisionGroupStruct)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	println(fmt.Sprintf("%v", group))

	return nil
}

func (create *CreateClient) loadConfigWrapper(filepath string, target protoreflect.ProtoMessage) error {
	file, err := os.Open(filepath)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	data, err := ioutil.ReadAll(file)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	err = protojson.Unmarshal(data, target)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	return nil

}
