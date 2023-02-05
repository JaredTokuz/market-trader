package etl

import (
	"context"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoController struct {
	Macros   *mongo.Collection
	Medium   *mongo.Collection
	Short    *mongo.Collection
	Signals  *mongo.Collection
	ApiQueue ApiQueueService
	ApiCalls *mongo.Collection
	Logs     *mongo.Collection
}

type Task string

// Mongo Collection names OR Task Names
const (
	Undefined Task = "unknown"
	Macros         = "Macros"
	Medium         = "Medium"
	Short          = "Short"
	Signals        = "Signals"
)

// Other Mongo Collections
const (
	ApiQueue = "ApiQueue"
	APICalls = "APICalls"
	Logs     = "logs"
)

func Connect(mongoURI string) (*MongoController, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(mongoURI))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = client.Connect(ctx)
	db := client.Database(os.Getenv("DB_NAME"))

	if err != nil {
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	return &MongoController{
		Macros:   db.Collection(Macros),
		Medium:   db.Collection(Medium),
		Short:    db.Collection(Short),
		Signals:  db.Collection(Signals),
		ApiQueue: NewApiQueue(db),
		ApiCalls: db.Collection(APICalls),
		Logs:     db.Collection(Logs),
	}, nil
}

type StockDoc struct {
	ID     primitive.ObjectID `json:"_id,omitempty"  bson:"_id,omitempty"`
	Symbol string             `json:"symbol"  bson:"symbol"`
}

type SymbolWorkConfig struct {
	ID     primitive.ObjectID `bson:"_id,omitempty"`
	Symbol string             `bson:"symbol"`
	Work   Task               `bson:"work"`
}

/*
API QUEUE
|
|
|
|
*/
type ApiQueueService interface {
	Queue(cursor *mongo.Cursor, workName Task) error
	Remove(workConfig SymbolWorkConfig) error
	Get() *SymbolWorkConfig
}

type apiQueue struct {
	apiqueue *mongo.Collection
	logs     *mongo.Collection
}

func NewApiQueue(mg *mongo.Database) ApiQueueService {
	return &apiQueue{apiqueue: mg.Collection(ApiQueue), logs: mg.Collection(Logs)}
}

func (q *apiQueue) Queue(cursor *mongo.Cursor, workName Task) error {
	var operations []mongo.WriteModel
	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(false)

	for cursor.Next(context.TODO()) {
		if len(operations) == 100 {
			_, err := q.apiqueue.BulkWrite(context.TODO(), operations, &bulkOption)
			if err != nil {
				return err
			}
			operations = nil
		}
		var result StockDoc
		if err := cursor.Decode(&result); err != nil {
			return err
		}
		field := SymbolWorkConfig{Symbol: result.Symbol, Work: workName} //bson.M{"symbol": result.Symbol, "work": workName}

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

func (q *apiQueue) Remove(workConfig SymbolWorkConfig) error {
	_, err := q.apiqueue.DeleteOne(
		context.TODO(),
		bson.M{"symbol": workConfig.Symbol, "work": workConfig.Work})
	if err != nil {
		return err
	}
	return nil
}

func (q *apiQueue) Get() *SymbolWorkConfig {
	var workConfig SymbolWorkConfig
	result := q.apiqueue.FindOne(context.TODO(), bson.M{})
	if result.Err() != nil {
		return nil
	}
	err := result.Decode(&workConfig)
	if err != nil {
		return nil
	}
	return &workConfig
}
