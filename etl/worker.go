package etl

import (
	"context"
	"log"
	"os"
	"sync"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/jaredtokuz/market-trader/token"
)

type ETLService struct {
	mongo  MongoController
	apikey string
	token  string
}

func InitWorker() error {
	mg, err := Connect(os.Getenv("MONGO_URI"))
	if err != nil {
		log.Fatal(err)
		return err
	}
	tokenHandler := token.NewAccessTokenService(os.Getenv("TOKEN_PATH"))
	api_key := os.Getenv("API_KEY")

	var (
		workDoc *SymbolWorkConfig
		wg      *sync.WaitGroup
	)
	processConfig := NewProcessConfig(mg, api_key, tokenHandler.Fetch(), *workDoc)
	for {
		workDoc = mg.ApiQueue.Get()
		if workDoc == nil {
			// finished work
			break
		}

		processConfig.SetWorkConfig(*workDoc)

		var (
			etl ProcessETL
		)
		switch workDoc.Work {
		case Macros:
			etl = MacrosETL(processConfig)
		case Medium:
			etl = MedTermETL(processConfig)
		case Short:
			etl = ShortETL(processConfig)
		case Signals:
			etl = SignalsETL(processConfig)
		}
		success, err := etl.CallApi()
		if err != nil {
			mg.Logs.InsertOne(context.TODO(), bson.M{"msg": err.Error()})
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			etl.Transform(success)
		}()
	}
	wg.Wait()
	return nil
}
