package etl

import (
	"context"
	"log"
	"os"
	"path"
	"testing"

	"github.com/jaredtokuz/market-trader/token"
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

func setup() error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	envPath := path.Join(cwd, "../..", ".test.env")
	log.Println(envPath)

	err = godotenv.Load(envPath)
	if err != nil {
		log.Fatal("Error loading .env file", err)
		return err
	}

	// Connect to the database
	mg, err := Connect(os.Getenv("MONGO_URI"))
	if err != nil {
		log.Fatal(err)
		return err
	}
	tokenHandler := token.NewAccessTokenService(os.Getenv("TOKEN_PATH"))
	api_key := os.Getenv("API_KEY")
	_ = NewWorker(mg.Db, api_key, tokenHandler)
	return nil
}

func shutdown() {}

func TestAppend(t *testing.T) {
	cursor, err := mg.Stocks.Find(context.TODO(), bson.D{})
	if err != nil {
		t.Error("Issue in check daily avg volume", err)
	}
	workName := "YearDaily"
	err = Append(workName, cursor, mg.Db)
	if err != nil {
		t.Error("append work failed", err)
	}
}

func TestAppendWorkYearDaily(t *testing.T) {
	cursor, err := mg.Stocks.Find(context.TODO(), bson.D{})
	if err != nil {
		t.Error("Stocks Find operation", err)
	}
	workName := "YearDaily"
	err = Append(workName, cursor, mg.Db)
	if err != nil {
		t.Error("append work failed", err)
	}
	err = testWorker.InitWork()
	if err != nil {
		t.Error("init work failed", err)
	}
}

func TestAppendWorkDay15Minute30(t *testing.T) {
	cursor, err := mg.Stocks.Find(context.TODO(), bson.M{"fundamental.vol10DayAvg": bson.M{"$gt": 500000}})
	if err != nil {
		t.Error("Issue in check daily avg volume", err)
	}
	workName := "Day15Minute30"
	err = Append(workName, cursor, mg.Db)
	if err != nil {
		t.Error("append work failed", err)
	}
	err = testWorker.InitWork()
	if err != nil {
		t.Error("init work failed", err)
	}
}

// Since it is 2 days it will not work on sundays
func TestAppendWorkDay2Minute15(t *testing.T) {
	cursor, err := mg.Stocks.Find(context.TODO(), bson.M{"fundamental.vol10DayAvg": bson.M{"$gt": 500000}})
	if err != nil {
		t.Error("Issue in check daily avg volume", err)
	}
	workName := "Day2Minute15"
	err = Append(workName, cursor, mg.Db)
	if err != nil {
		t.Error("append work failed", err)
	}
	err = testWorker.InitWork()
	if err != nil {
		t.Error("init work failed", err)
	}
}

func TestAppendWorkMinute15Signals(t *testing.T) {
	cursor, err := mg.Stocks.Find(context.TODO(), bson.M{"signal": true})
	if err != nil {
		t.Error("Issue in check daily avg volume", err)
	}
	workName := "Minute15Signals"
	err = Append(workName, cursor, mg.Db)
	if err != nil {
		t.Error("append work failed", err)
	}
	err = testWorker.InitWork()
	if err != nil {
		t.Error("init work failed", err)
	}
}

// var testWorker Worker

// type MongoTestInstance struct {
// 	Client *mongo.Client
// 	Db     *mongo.Database
// 	Stocks *mongo.Collection
// 	Token  *mongo.Collection
// }

// var mg MongoTestInstance

// func Connect() error {
// 	mongoURI := os.Getenv("MONGO_URI")
// 	client, err := mongo.NewClient(options.Client().ApplyURI(mongoURI))
// 	if err != nil {
// 		return err
// 	}
// 	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 	defer cancel()

// 	err = client.Connect(ctx)
// 	db := client.Database(os.Getenv("DB_NAME"))

// 	if err != nil {
// 		return err
// 	}

// 	stocks := db.Collection("stocks")
// 	token := client.Database(os.Getenv("PROD_DB_NAME")).Collection("toke")

// 	if err != nil {
// 		return err
// 	}

// 	mg = MongoTestInstance{
// 		Client: client,
// 		Db:     db,
// 		Stocks: stocks,
// 		Token:  token,
// 	}

// 	return nil
// }
