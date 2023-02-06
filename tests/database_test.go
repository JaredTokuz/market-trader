package tests

import (
	"log"
	"os"
	"testing"

	"github.com/jaredtokuz/market-trader/etl"
)

func setController() *etl.MongoController {
	m, err := etl.NewMongoController(os.Getenv("MONGO_URI"))
	if err != nil {
		log.Fatal("Failed to connect to mongo controller ", err)

	}
	return m
}

func TestMongoController(t *testing.T) {
	_, err := etl.NewMongoController(os.Getenv("MONGO_URI"))
	if err != nil {
		t.Error("Failed to connect to Mongo Controller", err)
	}
}
