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
	"log"

	"github.com/ScienceObjectsDB/ScienceObjectsDBClient/subcmd"
	models "github.com/ScienceObjectsDB/go-api/api/models/v1"
	"github.com/spf13/cobra"
)

// createCmd represents the create command
var createCmd = &cobra.Command{
	Use:   "create",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: RunCreate,
}

func RunCreate(cmd *cobra.Command, args []string) {
	clients, err := subcmd.NewClients(Token)
	if err != nil {
		log.Fatalln(err.Error())
	}

	createExec := subcmd.CreateClient{
		GrpcClients: clients,
	}

	Resource = Resource + "_RESOURCE"

	resourceEnum := models.Resource_value[Resource]

	createExec.Create(models.Resource(resourceEnum), RequestFilepath)
}

func init() {
	createCmd.Flags().StringVarP(&Resource, "resource", "r", "", "Targeted resource")
	createCmd.Flags().StringVarP(&Token, "token", "t", "", "Authentication token")
	createCmd.Flags().StringVarP(&RequestFilepath, "filepath", "f", "", "Authentication token")

	rootCmd.AddCommand(createCmd)
}
