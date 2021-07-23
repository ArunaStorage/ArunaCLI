package subcmd

import (
	"log"

	models "github.com/ScienceObjectsDB/go-api/api/models/v1"
	services "github.com/ScienceObjectsDB/go-api/api/services/v1"
)

//Create Generic list struct
type UserHandling struct {
	*GrpcClients
}

func (userhandler *UserHandling) AddUser(userID string, projectID string, scope []models.Right) error {
	user := services.AddUserToProjectRequest{
		UserId:    userID,
		ProjectId: projectID,
		Scope:     scope,
	}

	_, err := userhandler.ProjectClient.AddUserToProject(userhandler.OutGoingContext(), &user)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	return nil
}
