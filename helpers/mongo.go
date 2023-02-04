package helpers

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func AppendUpsertOne(operations []mongo.WriteModel, filter bson.M, update bson.M) []mongo.WriteModel {
	op := mongo.NewUpdateOneModel()
	op.SetFilter(filter)
	op.SetUpdate(bson.M{"$set": update})
	op.SetUpsert(true)
	return append(operations, op)
}
