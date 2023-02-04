package main

import (
	"context"
	"log"

	"github.com/jaredtokuz/market-trader/cmd/setup"
	"github.com/jaredtokuz/market-trader/etl"
	"go.mongodb.org/mongo-driver/bson"
)

func main() {
	setup, err := setup.Setup()
	if err != nil {
		log.Fatal(err)
	}
	// filter := bson.M{"$and": []interface{}{
	// 	bson.M{"fundamental.vol10DayAvg": bson.M{"$gt": 500000 } },
	// 	bson.M{"fundamental.marketCap": bson.M{ "$gt" : 500 } },
	// }}
	cursor, err := setup.Mg.Stocks.Find(context.TODO(), bson.M{"fundamental.vol10DayAvg": bson.M{"$gt": 500000}})
	if err != nil {
		log.Fatal("Issue in check daily avg volume", err)
	}
	workName := "Day2Minute15"
	err = etl.Append(workName, cursor, setup.Mg.Db)
	if err != nil {
		log.Fatal("append work failed", err)
	}
	err = setup.Worker.InitWork()
	if err != nil {
		log.Fatal("init work failed", err)
	}
}
