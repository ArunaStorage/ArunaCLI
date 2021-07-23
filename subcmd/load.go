package subcmd

import (
	"fmt"
	"log"
	"net/http"
	"os"

	services "github.com/ScienceObjectsDB/go-api/api/services/v1"
)

//Load Generic Load struct
type Load struct {
	*GrpcClients
}

func (load *Load) Upload(filepath string, objectID string) error {
	file, err := os.Open(filepath)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	id := services.CreateUploadLinkRequest{
		Id: objectID,
	}

	uploadLink, err := load.ObjectLoadClient.CreateUploadLink(load.OutGoingContext(), &id)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	req, err := http.NewRequest("PUT", uploadLink.UploadLink, file)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	fileStats, err := file.Stat()
	if err != nil {
		log.Println(err.Error())
		return err
	}

	req.ContentLength = fileStats.Size()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	if resp.StatusCode != 200 {
		log.Println(fmt.Sprintf("Bad request reponse: %v", resp))
	}

	return nil
}
