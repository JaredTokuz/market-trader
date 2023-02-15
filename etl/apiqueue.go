package etl

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ApiQueueService interface {
	Queue(cursor *mongo.Cursor, workName EtlJob) error
	Init() error
	Get() *EtlConfig
	UpdateStage(etlConfig EtlConfig) error
	Remove(etlConfig EtlConfig) error
}

type apiQueue struct {
	apiqueue *mongo.Collection
	logs     *mongo.Collection
}

func NewApiQueue(mg *mongo.Database) ApiQueueService {
	return &apiQueue{apiqueue: mg.Collection(ApiQueue), logs: mg.Collection(Logs)}
}

func (q *apiQueue) Queue(cursor *mongo.Cursor, workName EtlJob) error {
	var operations []mongo.WriteModel
	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(false)

	for cursor.Next(context.TODO()) {
		if len(operations) == 100 {
			log.Println("BulkWrite: ", len(operations), operations[0], time.Now().Format(time.RFC3339Nano))
			_, err := q.apiqueue.BulkWrite(context.TODO(), operations, &bulkOption)
			if err != nil {
				return err
			}
			operations = nil
		}
		var result SymbolDoc
		if err := cursor.Decode(&result); err != nil {
			return err
		}
		field := NewEtlConfig(result.Symbol, workName)

		operations = append(
			operations,
			mongo.NewUpdateOneModel().SetFilter(field).SetUpdate(bson.M{"$set": field}).SetUpsert(true))
	}

	if len(operations) != 0 {
		_, err := q.apiqueue.BulkWrite(context.TODO(), operations, &bulkOption)
		if err != nil {
			return err
		}
	}

	q.logs.InsertOne(context.TODO(), bson.M{
		"desc":     "http queue up complete",
		"workName": workName,
		"init_dt":  time.Now(),
	})

	return nil
}

func (q *apiQueue) Init() error {
	_, err := q.apiqueue.UpdateMany(context.TODO(), bson.D{}, bson.M{"$set": bson.M{"stage": Api}})
	if err != nil {
		return err
	}
	return nil
}

func (q *apiQueue) Get() *EtlConfig {
	var etlConfig EtlConfig
	err := q.apiqueue.FindOne(context.TODO(), bson.M{"stage": Api}).Decode(&etlConfig)
	if err != nil {
		return nil
	}
	return &etlConfig
}

func (q *apiQueue) UpdateStage(etlConfig EtlConfig) error {
	_, err := q.apiqueue.UpdateOne(context.TODO(),
		bson.M{"symbol": etlConfig.Symbol, "work": etlConfig.Work},
		bson.M{"$set": bson.M{"stage": Transform}})

	if err != nil {
		return err
	}
	return nil
}

func (q *apiQueue) Remove(etlConfig EtlConfig) error {
	_, err := q.apiqueue.DeleteOne(
		context.TODO(),
		bson.M{"symbol": etlConfig.Symbol, "work": etlConfig.Work})
	if err != nil {
		return err
	}
	return nil
}
