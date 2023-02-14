package etl

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ApiCallService interface {
	Cache(etlconfig EtlConfig, doc HttpResponsesDocument) error
}

type apiCalls struct {
	apicalls *mongo.Collection
}

func NewApiCallService(mg *mongo.Database) ApiCallService {
	return &apiCalls{apicalls: mg.Collection(APICalls)}
}

type HttpResponsesDocument struct {
	Response  APIResponse `json:"response"  bson:"response"`
	EtlConfig EtlConfig   `json:"etlConfig"  bson:"etlConfig"`
}

type APIResponse struct {
	Body   interface{} `json:"body"  bson:"body"`
	Status int         `json:"status"  bson:"status"`
	// Request string      `json:"request"  bson:"request"`
}

func (q *apiCalls) Cache(etlConfig EtlConfig, document HttpResponsesDocument) error {
	_, err := q.apicalls.UpdateOne(context.TODO(),
		bson.M{"symbol": etlConfig.Symbol, "work": etlConfig.Work},
		bson.M{"$set": document},
		options.Update().SetUpsert(true))
	if err != nil {
		return err
	}
	return nil
}
