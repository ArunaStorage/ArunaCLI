package subcmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"

	services "github.com/ScienceObjectsDB/go-api/api/services/v1"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

// TokenType Indicates the kind of token passed for authentication
type TokenType string

const (
	// AccessToken for Oauth2 authentication
	AccessToken TokenType = "AccessToken"
	// UserAPIToken for api token authentication
	UserAPIToken TokenType = "API_TOKEN"
)

//GrpcClients struct to hold the individual api clients
type GrpcClients struct {
	ProjectClient          services.ProjectServiceClient
	DatasetClient          services.DatasetServiceClient
	ObjectGroupClient      services.DatasetObjectsServiceClient
	ObjectLoadClient       services.ObjectLoadServiceClient
	GenericOutGoingContext context.Context
	Token                  string
}

// New Creates and initializes a new set of GRPCEndpointsClients
func NewClients(token string) (*GrpcClients, error) {
	if viper.IsSet("Auth.AccessToken") {
		token = viper.GetString("Auth.AccessToken")
	} else if viper.IsSet("Auth.UserToken") {
		token = viper.GetString("Auth.UserToken")
	}

	clients := GrpcClients{
		Token: token,
	}

	host := viper.GetString("Endpoint.Host")
	port := viper.GetInt("Endpoint.Port")

	ctx := context.Background()
	clients.GenericOutGoingContext = ctx

	var tlsConf tls.Config
	credentials := credentials.NewTLS(&tlsConf)
	dialOptions := grpc.WithTransportCredentials(credentials)

	if host == "127.0.0.1" {
		dialOptions = grpc.WithInsecure()
	}

	conn, err := grpc.Dial(fmt.Sprintf("%v:%v", host, port), dialOptions)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}
	clients.createDatasetClient(conn)
	clients.createProjectClient(conn)
	clients.createLoadClient(conn)
	clients.createObjectsClient(conn)

	return &clients, nil
}

//OutGoingContext Wrapper function for legacy reasons
func (clients *GrpcClients) OutGoingContext() context.Context {
	if viper.IsSet("Auth.AccessToken") {
		return clients.OutGoingContextFromToken(clients.Token, AccessToken)
	} else if viper.IsSet("Auth.UserToken") {
		return clients.OutGoingContextFromToken(clients.Token, UserAPIToken)
	}

	return context.TODO()
}

// OutGoingContextFromToken Creates the required outgoing context for a call
func (clients *GrpcClients) OutGoingContextFromToken(token string, tokentype TokenType) context.Context {
	mdMap := make(map[string]string)
	mdMap[string(tokentype)] = token
	tokenMetadata := metadata.New(mdMap)

	outgoingContext := metadata.NewOutgoingContext(context.TODO(), tokenMetadata)
	return outgoingContext
}
func (clients *GrpcClients) createObjectsClient(conn *grpc.ClientConn) {
	clients.ObjectGroupClient = services.NewDatasetObjectsServiceClient(conn)
}

func (clients *GrpcClients) createLoadClient(conn *grpc.ClientConn) {
	clients.ObjectLoadClient = services.NewObjectLoadServiceClient(conn)
}

func (clients *GrpcClients) createDatasetClient(conn *grpc.ClientConn) {
	clients.DatasetClient = services.NewDatasetServiceClient(conn)
}

func (clients *GrpcClients) createProjectClient(conn *grpc.ClientConn) {
	clients.ProjectClient = services.NewProjectServiceClient(conn)
}
