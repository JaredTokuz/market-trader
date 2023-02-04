package etl

// TODO refactor this

import (
	"context"
	"time"

	"github.com/jaredtokuz/market-trader/helpers"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type WorkDoc struct {
	ID     primitive.ObjectID `bson:"_id,omitempty"`
	Symbol string             `bson:"symbol"`
	Work   string             `bson:"work"`
}

const WORK = "work"
const AppendWork = "append work"

func Append(workName string, cursor *mongo.Cursor, db *mongo.Database) error {
	var operations []mongo.WriteModel
	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(false)

	for cursor.Next(context.TODO()) {
		if len(operations) == 100 {
			_, err := db.Collection(WORK).BulkWrite(context.TODO(), operations, &bulkOption)
			if err != nil {
				return err
			}
			operations = nil
		}
		var result WorkDoc
		if err := cursor.Decode(&result); err != nil {
			return err
		}
		field := bson.M{"symbol": result.Symbol, "work": workName}
		operations = helpers.AppendUpsertOne(operations, field, field)
	}

	if len(operations) != 0 {
		_, err := db.Collection(WORK).BulkWrite(context.TODO(), operations, &bulkOption)
		if err != nil {
			return err
		}
	}

	db.Collection("logs").InsertOne(context.TODO(), bson.M{
		"desc":     AppendWork,
		"workName": workName,
		"init_dt":  time.Now(),
	})

	return nil
}
