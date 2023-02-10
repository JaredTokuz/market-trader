package etl

import (
	"context"
	"log"
	"os"
	"path"
	"testing"
	"time"

	// "github.com/jaredtokuz/market-trader/etl"
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
	envPath = os.Getenv("DOTENV_PATH")
	if envPath != "" {
		err = godotenv.Load(envPath)
	}
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
	err = client.Ping(context.Background(), nil)
	if err != nil {
		log.Fatal("Database failed to ping ", err)
		return nil
	}

	db := client.Database(os.Getenv("DB_NAME"))

	return db
}

func shutdown() {
	mc := setDatabase()
	mc.Collection(Macros).DeleteMany(context.TODO(), bson.M{})
	mc.Collection(Medium).DeleteMany(context.TODO(), bson.M{})
	mc.Collection(Short).DeleteMany(context.TODO(), bson.M{})
	mc.Collection(Signals).DeleteMany(context.TODO(), bson.M{})
	mc.Collection(ApiQueue).DeleteMany(context.TODO(), bson.M{})
	mc.Collection(APICalls).DeleteMany(context.TODO(), bson.M{})
	mc.Collection(Logs).DeleteMany(context.TODO(), bson.M{})
	mc.Client().Disconnect(context.Background())
}

func setController() *MongoController {
	m, err := NewMongoController(os.Getenv("MONGO_URI"), os.Getenv("DB_NAME"))
	if err != nil {
		log.Fatal("Failed to connect to mongo controller ", err)

	}
	return m
}

func TestMongoController(t *testing.T) {
	_, err := NewMongoController(os.Getenv("MONGO_URI"), os.Getenv("DB_NAME"))
	if err != nil {
		t.Error("Failed to connect to Mongo Controller", err)
	}
}

func queueMacros(job EtlJob) {
	mc := setController()
	cursor, err := mc.Macros.Find(context.TODO(), bson.M{})
	if err != nil {
		log.Fatal("Issue in Database Find", err)
	}

	err = mc.ApiQueue.Queue(cursor, job)
	if err != nil {
		log.Fatal("Work Queue up failed.")
	}
}

func initializeQueueData(docs []SymbolDoc, job EtlJob) {
	mc := setController()

	log.Println("Inserting data ", docs)
	data := make([]interface{}, len(docs))
	for i := range docs {
		log.Println("Inserting data ", docs[i], docs[i].ForInsert())
		data[i] = docs[i]
	}

	_, err := mc.Macros.InsertMany(context.TODO(), data, options.InsertMany().SetOrdered(false))
	if err != nil {
		log.Fatal("insert step failed. ", err)
	}

	queueMacros(job)
}

func TestApiQueue(t *testing.T) {
	mc := setController()
	data := []SymbolDoc{
		{
			Symbol: "TSLA",
		},
		{
			Symbol: "MSFT",
		},
	}
	initializeQueueData(data, Macros)

	var found *EtlConfig
	found = mc.ApiQueue.Get()
	if found == nil {
		t.Error("Docs not added to queue")
	}
	for _, s := range data {
		err := mc.ApiQueue.Remove(EtlConfig{Symbol: s.Symbol, Work: Macros})
		if err != nil {
			t.Error("Failed to remove doc from queue")
		}
	}
	found = mc.ApiQueue.Get()
	log.Println("Found ", found)
	if found != nil {
		t.Error("Docs not removed from queue")
	}
}

func setTDApiService() (TDApiService, error) {
	mc := setController()

	config := &Config{ApiKey: os.Getenv("API_KEY"), TokenPath: os.Getenv("TOKEN_PATH")}
	err := config.Validate()
	if err != nil {
		return nil, err
	}

	td := NewTDApiService(mc, config.ApiKey, token.NewAccessTokenService(config.TokenPath))
	return td, nil
}

func getEtlJobs() []EtlJob {
	return []EtlJob{
		Macros,
		Medium,
		Short,
		Signals,
	}
}

func TestCall(t *testing.T) {

	td, err := setTDApiService()
	db := setDatabase()
	if err != nil {
		t.Error("Failed to connect to TDApiService", err)
	}

	for _, j := range getEtlJobs() {
		c := EtlConfig{
			Symbol: "TSLA",
			Work:   j,
		}
		_, err := td.Call(c)
		if err != nil {
			t.Error("Call failed")
		}
		time.Sleep(500)
		// check if logged
		found := db.Collection(APICalls).FindOne(context.TODO(), bson.M{})
		if found != nil {
			t.Error("Not cached in logs")
		}
		var doc *HttpResponsesDocument
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
	td, err := setTDApiService()
	mc := setController()
	if err != nil {
		t.Error("Failed to connect to TDApiService", err)
	}

	for _, j := range getEtlJobs() {
		c := EtlConfig{
			Symbol: "TSLA",
			Work:   j,
		}
		success, err := td.Call(c)
		time.Sleep(500)
		if err != nil {
			t.Error("Call failed")
		}
		err = TransformLoad(*mc, *success)
		if err != nil {

		}
	}

}

func TestWorkerGeneral(t *testing.T) {
	data := []SymbolDoc{
		{
			Symbol: "TSLA",
		},
		{
			Symbol: "MSFT",
		},
	}
	initializeQueueData(data, Macros)
	queueMacros(Medium)
	queueMacros(Short)
	queueMacros(Signals)

	InitWorker()
}
