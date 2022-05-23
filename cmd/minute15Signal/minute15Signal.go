package main

import (
	"context"
	"log"

	"github.com/jaredtokuz/market-trader/cmd/setup"
	"github.com/jaredtokuz/market-trader/pkg/work"
	"go.mongodb.org/mongo-driver/bson"
)

func main() {
	setup, err := setup.Setup()
	if err != nil {
		log.Fatal(err)
	}
	cursor, err := setup.Mg.Stocks.Find(context.TODO(), bson.M{"signal": true })
	if err != nil {
		log.Fatal("Issue in check daily avg volume", err)
	}
	workName := "Minute15Signals"
	err = work.Append(workName, cursor, setup.Mg.Db)
	if err != nil {
		log.Fatal("append work failed", err)
	}
	err = setup.Worker.InitWork()
	if err != nil {
		log.Fatal("init work failed", err)
	}
}