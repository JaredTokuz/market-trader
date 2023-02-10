package cmd

import (
	"context"
	"log"
	"os"

	"github.com/jaredtokuz/market-trader/etl"
	"go.mongodb.org/mongo-driver/bson"
)

func main() {
	mongo, err := etl.NewMongoController(os.Getenv("MONGO_URI"), os.Getenv("MONGO_DB"))
	if err != nil {
		log.Fatal("Database connection failed")
	}

	cursor, err := mongo.Macros.Find(context.TODO(), bson.M{})
	if err != nil {
		log.Fatal("Issue in check daily avg volume", err)
	}
	// http response task
	err = mongo.ApiQueue.Queue(cursor, etl.Macros)
	if err != nil {
		log.Fatal("Work Queue up failed.")
	}
}
