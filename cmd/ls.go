package cmd

/*
Copyright © 2021 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

import (
	"fmt"
	"log"

	"github.com/ScienceObjectsDB/ScienceObjectsDBClient/subcmd"
	models "github.com/ScienceObjectsDB/go-api/api/models/v1"
	"github.com/spf13/cobra"
)

// lsCmd represents the ls command
var lsCmd = &cobra.Command{
	Use:   "ls",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,

	Run: RunLS,
}

func RunLS(cmd *cobra.Command, args []string) {
	clients, err := subcmd.NewClients(Token)
	if err != nil {
		log.Fatalln(err.Error())
	}

	lsExec := subcmd.LS{
		GrpcClients: clients,
	}

	resourceEnum := models.Resource_value[Resource]

	switch resourceEnum {
	case int32(models.Resource_DATASET_RESOURCE):
		lsExec.DatasetsLS(ResourceID)
	case int32(models.Resource_OBJEFT_GROUP_RESOURCE):
		lsExec.DatasetObjectGroupsLS(ResourceID)
	case int32(models.Resource_PROJECT_RESOURCE):
		lsExec.ProjectLS()
	case int32(models.Resource_OBJECT_GROUP_VERSION_RESOURCE):
		lsExec.ObjectGroupVersions(ResourceID)
	default:
		log.Fatalln(fmt.Sprintf("could not find resource %v", Resource))
	}
}

func init() {
	lsCmd.Flags().StringVarP(&ResourceID, "id", "i", "", "ID of the referenced resource")
	lsCmd.Flags().StringVarP(&Resource, "resource", "r", "", "Targeted resource")
	lsCmd.Flags().StringVarP(&Token, "token", "t", "", "Authentication token")

	rootCmd.AddCommand(lsCmd)
}
