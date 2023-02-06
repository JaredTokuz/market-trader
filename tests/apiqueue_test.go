package tests

import (
	"context"
	"log"
	"testing"

	"github.com/jaredtokuz/market-trader/etl"
	"go.mongodb.org/mongo-driver/bson"
)

func queueMacros(job etl.EtlJob) {
	mc := setController()
	cursor, err := mc.Macros.Find(context.TODO(), bson.M{})
	if err != nil {
		log.Fatal("Issue in check daily avg volume", err)
	}

	err = mc.ApiQueue.Queue(cursor, job)
	if err != nil {
		log.Fatal("Work Queue up failed.")
	}
}

func initializeQueueData(data []interface{}, job etl.EtlJob) {
	mc := setController()

	_, err := mc.Macros.InsertMany(context.TODO(), data)
	if err != nil {
		log.Fatal("insert step failed")
	}

	queueMacros(job)
}

func TestApiQueue(t *testing.T) {
	mc := setController()
	data := []interface{}{
		etl.SymbolDoc{
			Symbol: "TSLA",
		},
		etl.SymbolDoc{
			Symbol: "MSFT",
		},
	}
	initializeQueueData(data, etl.Macros)

	var found *etl.EtlConfig
	found = mc.ApiQueue.Get()
	if found == nil {
		t.Error("Docs not added to queue")
	}
	for _, s := range data {
		mc.ApiQueue.Remove(s.(etl.EtlConfig))
	}
	found = mc.ApiQueue.Get()
	if found != nil {
		t.Error("Docs not removed from queue")
	}
}
