package setup

import (
	"context"
	"log"
	"os"
	"path"
	"time"

	"github.com/jaredtokuz/market-trader/pkg/token"
	"github.com/jaredtokuz/market-trader/pkg/work"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type WorkSetup struct {
	Worker 	work.Worker
	Mg			*MongoInstance
}

func Setup() (*WorkSetup, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	err = godotenv.Load("/home/jt/dist/.env")
	if err != nil {
		envPath := path.Join(cwd, "../..", ".env") 
		err = godotenv.Load(envPath)
		if err != nil {
			log.Fatal("Error loading .env file", err)
			return nil,err
		}
	}

	// Connect to the database
	mg, err := Connect()
	if err != nil {
		log.Fatal(err)
		return nil,err
	}
	tokenHandler := token.NewTokenProviderService(mg.Token, os.Getenv("TOKEN_PATH"))
	api_key := os.Getenv("API_KEY")
	worker := work.NewWorker(mg.Db, api_key, tokenHandler)
	return &WorkSetup {
		Worker: worker,
		Mg:	mg,
	}, err
}


type MongoInstance struct {
	Client *mongo.Client
	Db     *mongo.Database
	Stocks *mongo.Collection
	Token *mongo.Collection
}

func Connect() (*MongoInstance,error) {
	mongoURI := os.Getenv("MONGO_URI")
	client, err := mongo.NewClient(options.Client().ApplyURI(mongoURI))
	if err != nil {
		return nil,err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = client.Connect(ctx)
	db := client.Database(os.Getenv("DB_NAME"))

	if err != nil {
		return nil,err
	}

	stocks := db.Collection("stocks")
	token := db.Collection("toke")

	if err != nil {
		return nil,err
	}

	return &MongoInstance{
		Client: client,
		Db:     db,
		Stocks: stocks,
		Token: token,
	}, nil
}
