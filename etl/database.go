package etl

import (
	"context"
	"os"
	"time"

	"github.com/jaredtokuz/market-trader/helpers"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoInstance struct {
	Macros       *mongo.Collection
	HttpQueue    *mongo.Collection
	HttpResponse *mongo.Collection
	Logs         *mongo.Collection
}

func Connect(mongoURI string) (*MongoInstance, error) {
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

	return &MongoInstance{
		Macros:       db.Collection("macros"),
		HttpQueue:    db.Collection("http-queue"),
		HttpResponse: db.Collection("http-response"),
		Logs:         db.Collection("logs"),
	}, nil
}

type MongoInstanceWorker interface {
	Queue(cursor *mongo.Cursor) error
}

type WithMongoId struct {
	ID primitive.ObjectID `bson:"_id,omitempty"`
}

func (mg *MongoInstance) Queue(cursor *mongo.Cursor, workName string) error {
	// TODO move Append logic here
	var operations []mongo.WriteModel
	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(false)

	for cursor.Next(context.TODO()) {
		if len(operations) == 100 {
			_, err := mg.HttpQueue.BulkWrite(context.TODO(), operations, &bulkOption)
			if err != nil {
				return err
			}
			operations = nil
		}
		var result WithMongoId
		if err := cursor.Decode(&result); err != nil {
			return err
		}
		field := bson.M{"id": result.ID, "work": workName}
		operations = helpers.AppendUpsertOne(operations, field, field)
	}

	if len(operations) != 0 {
		_, err := mg.HttpQueue.BulkWrite(context.TODO(), operations, &bulkOption)
		if err != nil {
			return err
		}
	}

	mg.Logs.InsertOne(context.TODO(), bson.M{
		"desc":     "http queue up complete",
		"workName": workName,
		"init_dt":  time.Now(),
	})

	return nil
}
