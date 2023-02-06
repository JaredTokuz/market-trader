package tests

import (
	"context"
	"log"
	"os"
	"path"
	"testing"
	"time"

	"github.com/jaredtokuz/market-trader/etl"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestMain(m *testing.M) {
	setup()
	log.Println("setup complete")
	code := m.Run()
	shutdown()
	os.Exit(code)
}

func setup() error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	envPath := path.Join(cwd, "../.env")
	err = godotenv.Load(envPath)
	if err != nil {
		log.Fatal("Error loading .env file ", err)
		return err
	}

	return nil
}

func setDatabase() *mongo.Database {
	client, err := mongo.NewClient(options.Client().ApplyURI(os.Getenv("MONGO_URI")))
	if err != nil {
		log.Fatal("Database init client ", err)
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = client.Connect(ctx)
	if err != nil {
		log.Fatal("Database failed to connect ", err)
		return nil
	}

	db := client.Database(os.Getenv("DB_NAME"))

	return db
}

func shutdown() {
	mc := setDatabase()
	mc.Collection(etl.Macros).DeleteMany(context.TODO(), bson.M{})
	mc.Collection(etl.Medium).DeleteMany(context.TODO(), bson.M{})
	mc.Collection(etl.Short).DeleteMany(context.TODO(), bson.M{})
	mc.Collection(etl.Signals).DeleteMany(context.TODO(), bson.M{})
	mc.Collection(etl.ApiQueue).DeleteMany(context.TODO(), bson.M{})
	mc.Collection(etl.APICalls).DeleteMany(context.TODO(), bson.M{})
	mc.Collection(etl.Logs).DeleteMany(context.TODO(), bson.M{})
}
