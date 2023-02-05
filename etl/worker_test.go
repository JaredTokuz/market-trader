package etl

import (
	"context"
	"log"
	"os"
	"path"
	"testing"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
)

func TestMain(m *testing.M) {
	setup()
	log.Println("setup complete")
	code := m.Run()
	shutdown()
	os.Exit(code)
}

// TODO run a test that as a prerequisite
// fills the api queue
func setup() error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	envPath := path.Join(cwd, "../.env")
	err = godotenv.Load(envPath)
	if err != nil {
		log.Fatal("Error loading .env file", err)
		return err
	}

	mongo, err := Connect(os.Getenv("MONGO_URI"))
	if err != nil {
		log.Fatal("Database connection failed")
	}

	cursor, err := mongo.Macros.Find(context.TODO(), bson.M{"signal": true})
	if err != nil {
		log.Fatal("Issue in check daily avg volume", err)
	}
	// http response task
	err = mongo.ApiQueue.Queue(cursor, Signals)
	if err != nil {
		log.Fatal("Work Queue up failed.")
	}

	return nil
}

func shutdown() {}

func TestWorkerGeneral(t *testing.T) {
	InitWorker()
}
