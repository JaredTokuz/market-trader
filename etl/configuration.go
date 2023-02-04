package etl

import (
	"log"
	"os"

	"github.com/jaredtokuz/market-trader/token"
)

type WorkConfig struct {
	Worker Worker
	Mg     *MongoInstance
}

func Configuration() (*WorkConfig, error) {
	// Connect to the database
	mg, err := Connect(os.Getenv("MONGO_URI"))
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	tokenHandler := token.NewAccessTokenService(os.Getenv("TOKEN_PATH"))
	api_key := os.Getenv("API_KEY")
	worker := NewWorker(mg.Db, api_key, tokenHandler)
	return &WorkConfig{
		Worker: worker,
		Mg:     mg,
	}, err
}

type WorkStart