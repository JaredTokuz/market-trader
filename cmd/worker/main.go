package main

import (
	"log"

	"github.com/jaredtokuz/market-trader/etl"
)

func main() {

	err := etl.InitWorker()

	if err != nil {
		log.Fatal("Issue in check daily avg volume", err)
	}
}
