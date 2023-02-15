package etl

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/jaredtokuz/market-trader/token"
)

func InitWorker() error {
	mg, err := NewMongoController(os.Getenv("MONGO_URI"), os.Getenv("DB_NAME"))
	if err != nil {
		log.Fatal(err)
		return err
	}
	tokenHandler := token.NewAccessTokenService(os.Getenv("TOKEN_PATH"))
	api_key := os.Getenv("API_KEY")

	var (
		workDoc *EtlConfig
		wg      sync.WaitGroup
	)
	tdApiService := NewTDApiService(mg, api_key, tokenHandler)
	mg.ApiQueue.Init() // sets all existing docs to stage api
	for {
		workDoc = mg.ApiQueue.Get()
		if workDoc == nil {
			// finished work
			break
		}

		success, err := tdApiService.Call(*workDoc)
		time.Sleep(1000)                  // change this to backoff retry
		mg.ApiQueue.UpdateStage(*workDoc) // update the stage to transform so apiqueue knows not to grab it again

		if err != nil {
			log.Println("TD Call Error: ", err.Error())
			mg.Logs.InsertOne(context.TODO(), bson.M{"msg": err.Error(), "category": "TD Call"})
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			err = TransformLoad(*mg, success)
			if err != nil {
				mg.Logs.InsertOne(context.TODO(), bson.M{"msg": err.Error(), "category": "TD TransformLoad"})
			}
		}()
	}
	wg.Wait()
	return nil
}
