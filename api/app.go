// 🚀 Fiber is an Express inspired web framework written in Go with 💖
// 📌 API Documentation: https://docs.gofiber.io
// 📝 Github Repository: https://github.com/gofiber/fiber

package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/jaredtokuz/market-trader/api/routes"
	"github.com/jaredtokuz/market-trader/stocks"
)

// MongoInstance contains the Mongo client and database objects
type MongoInstance struct {
	Client *mongo.Client
	Db     *mongo.Database
	Stocks *mongo.Collection
}

var mg MongoInstance

func Connect() error {
	mongoURI := os.Getenv("MONGO_URI")
	client, err := mongo.NewClient(options.Client().ApplyURI(mongoURI))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = client.Connect(ctx)
	db := client.Database(os.Getenv("DB_NAME"))

	if err != nil {
		return err
	}

	stocks := client.Database(os.Getenv("DB_NAME")).Collection("stocks")

	if err != nil {
		return err
	}

	mg = MongoInstance{
		Client: client,
		Db:     db,
		Stocks: stocks,
	}

	return nil
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Println("Error loading .env file")
	}

	// Connect to the database
	if err := Connect(); err != nil {
		log.Fatal(err)
	}

	stocksRepo := stocks.NewRepo(mg.Db)
	stocksService := stocks.NewService(stocksRepo)

	// Create new Fiber instance
	app := fiber.New()

	app.Use(cors.New(cors.Config{
		AllowOrigins: "http://localhost:4200",
		AllowMethods: "GET,POST,HEAD,PUT,DELETE,PATCH",
		AllowHeaders: "",
	}))

	api := app.Group("/api")
	routes.StockRouter(api, stocksService)

	// serve Single Page application on "/web"
	// assume static file at dist folder
	app.Static("/", "angular-trader")

	app.Get("/", func(ctx *fiber.Ctx) error {
		return ctx.SendFile("./angular-trader/index.html")
	})

	// Start server on http://localhost:3000
	log.Fatal(app.Listen(":3000"))
}
