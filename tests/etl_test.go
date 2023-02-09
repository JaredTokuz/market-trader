package tests

import (
	"context"
	"log"
	"os"
	"path"
	"testing"
	"time"

	"github.com/jaredtokuz/market-trader/etl"
	"github.com/jaredtokuz/market-trader/token"
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

func setController() *etl.MongoController {
	m, err := etl.NewMongoController(os.Getenv("MONGO_URI"))
	if err != nil {
		log.Fatal("Failed to connect to mongo controller ", err)

	}
	return m
}

func TestMongoController(t *testing.T) {
	_, err := etl.NewMongoController(os.Getenv("MONGO_URI"))
	if err != nil {
		t.Error("Failed to connect to Mongo Controller", err)
	}
}

func queueMacros(job etl.EtlJob) {
	mc := setController()
	cursor, err := mc.Macros.Find(context.TODO(), bson.M{})
	if err != nil {
		log.Fatal("Issue in check daily avg volume", err)
	}

	err = mc.ApiQueue.Queue(cursor, job)
	if err != nil {
		log.Fatal("Work Queue up failed.")
	}
}

func initializeQueueData(data []interface{}, job etl.EtlJob) {
	mc := setController()

	_, err := mc.Macros.InsertMany(context.TODO(), data)
	if err != nil {
		log.Fatal("insert step failed")
	}

	queueMacros(job)
}

func TestApiQueue(t *testing.T) {
	mc := setController()
	data := []interface{}{
		etl.SymbolDoc{
			Symbol: "TSLA",
		},
		etl.SymbolDoc{
			Symbol: "MSFT",
		},
	}
	initializeQueueData(data, etl.Macros)

	var found *etl.EtlConfig
	found = mc.ApiQueue.Get()
	if found == nil {
		t.Error("Docs not added to queue")
	}
	for _, s := range data {
		mc.ApiQueue.Remove(s.(etl.EtlConfig))
	}
	found = mc.ApiQueue.Get()
	if found != nil {
		t.Error("Docs not removed from queue")
	}
}

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

func TestTransformLoad(t *testing.T) {
	td := setTDApiService()
	mc := setController()

	for _, j := range getEtlJobs() {
		c := etl.EtlConfig{
			Symbol: "TSLA",
			Work:   j,
		}
		success, err := td.Call(c)
		time.Sleep(500)
		if err != nil {
			t.Error("Call failed")
		}
		err = etl.TransformLoad(*mc, *success)
		if err != nil {

		}
	}

}

func TestWorkerGeneral(t *testing.T) {
	data := []interface{}{
		etl.SymbolDoc{
			Symbol: "TSLA",
		},
		etl.SymbolDoc{
			Symbol: "MSFT",
		},
	}
	initializeQueueData(data, etl.Macros)
	queueMacros(etl.Medium)
	queueMacros(etl.Short)
	queueMacros(etl.Signals)

	etl.InitWorker()
}
