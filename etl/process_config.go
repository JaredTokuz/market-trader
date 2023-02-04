package etl

import (
	"net/http"

	"go.mongodb.org/mongo-driver/mongo"
)

type processConfig struct {
	database *mongo.Database
	apikey   string
	token    string
	workdoc  WorkDoc
}

type ProcessConfig interface {
	init()
	getHttpResponse() (*http.Response, error)
	transform() (interface{}, error)
	update() error
}
