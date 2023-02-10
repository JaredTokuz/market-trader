package etl

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoController struct {
	database *mongo.Database
	Macros   *mongo.Collection /* high level metrics data */
	Medium   *mongo.Collection /* 15 days 30 minutes longer trends */
	Short    *mongo.Collection /* 2 days 15 minutes 56 bars algo... analysis backtesting */
	Signals  *mongo.Collection /* realtime dataset for a trader minimal, calc trade conditions, based on medium and short research */
	ApiQueue ApiQueueService   /* Entry for the database queue for background */
	ApiCalls ApiCallService    /* Logs of TD Ameritrade Responses */
	Logs     *mongo.Collection /* Generic logs */
}

func NewMongoController(mongoURI string, database_name string) (*MongoController, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(mongoURI))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	log.Println("Database connecting to ", database_name)
	db := client.Database(database_name)
	err = db.Client().Ping(ctx, nil)
	if err != nil {
		log.Fatal("Database failed to ping ", err)
		return nil, err
	}
	log.Println("MongoController ready")
	return &MongoController{
		database: db,
		Macros:   db.Collection(Macros),
		Medium:   db.Collection(Medium),
		Short:    db.Collection(Short),
		Signals:  db.Collection(Signals),
		ApiQueue: NewApiQueue(db),
		ApiCalls: NewApiCallService(db),
		Logs:     db.Collection(Logs),
	}, nil
}
