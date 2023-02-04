package main

import (
	"context"
	"log"

	"github.com/jaredtokuz/market-trader/etl"
	"go.mongodb.org/mongo-driver/bson"
)

func main() {
	config, err := etl.Configuration()
	if err != nil {
		log.Fatal(err)
	}

	setup, err := setup.Setup()
	if err != nil {
		log.Fatal(err)
	}
	cursor, err := setup.Mg.Stocks.Find(context.TODO(), bson.D{})
	if err != nil {
		log.Fatal("Issue in check daily avg volume", err)
	}
	workName := "YearDaily"
	err = work.Append(workName, cursor, setup.Mg.Db)
	if err != nil {
		log.Fatal("append work failed", err)
	}
	err = setup.Worker.InitWork()
	if err != nil {
		log.Fatal("init work failed", err)
	}
}
