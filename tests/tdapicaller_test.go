package tests

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jaredtokuz/market-trader/etl"
	"github.com/jaredtokuz/market-trader/token"
	"go.mongodb.org/mongo-driver/bson"
)

func setTDApiService() etl.TDApiService {
	mc := setController()
	td := etl.NewTDApiService(mc, os.Getenv("API_KEY"), token.NewAccessTokenService(os.Getenv("TOKEN_PATH")))
	return td
}

func getEtlJobs() []etl.EtlJob {
	return []etl.EtlJob{
		etl.Macros,
		etl.Medium,
		etl.Short,
		etl.Signals,
	}
}

func TestCall(t *testing.T) {
	td := setTDApiService()
	db := setDatabase()

	for _, j := range getEtlJobs() {
		c := etl.EtlConfig{
			Symbol: "TSLA",
			Work:   j,
		}
		_, err := td.Call(c)
		if err != nil {
			t.Error("Call failed")
		}
		time.Sleep(500)
		// check if logged
		found := db.Collection(etl.APICalls).FindOne(context.TODO(), bson.M{})
		if found != nil {
			t.Error("Not cached in logs")
		}
		var doc *etl.HttpResponsesDocument
		found.Decode(&doc)
		if doc.Response.Status != 200 {
			t.Error("Response status was not 200")
		}
		if doc.EtlConfig.Symbol != c.Symbol || doc.EtlConfig.Work != c.Work {
			t.Error("configs dont match")
		}
	}

}
