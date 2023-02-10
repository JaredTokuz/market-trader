package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/jaredtokuz/market-trader/etl"
)

var columns = []string{"symbol"}

type record struct {
	ID        primitive.ObjectID `bson:"_id,omitempty"`
	Symbol    string             `bson:"symbol"`
	Timestamp int64              `bson:"timestamp"`
}

func main() {
	// Connect to MongoDB
	fmt.Println(os.Getenv("MONGO_URI"))
	mongoController, err := etl.NewMongoController(os.Getenv("MONGO_URI"), os.Getenv("DB_NAME"))
	if err != nil {
		fmt.Println("Mongo Controller failed to create", err)
		os.Exit(1)
	}
	fmt.Print("Mongo Controller created \n")

	// Get a handle to the collection
	collection := mongoController.Macros

	// Open the CSV file
	file, err := os.Open("./data/NASDAQ_20230208.csv")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer file.Close()

	// Create a new CSV reader
	reader := csv.NewReader(file)

	// Read the header row
	header, err := reader.Read()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Map the header to column indices
	indices := make(map[string]int)
	for i, column := range header {
		indices[strings.ToLower(column)] = i
	}

	fmt.Print("Reading rows in... \n")

	// Read the data rows
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Extract the specified columns from the row
		data := make(map[string]interface{})
		for _, column := range columns {
			index, ok := indices[column]
			if !ok {
				fmt.Println("column not found:", column)
				continue
			}

			value := row[index]
			switch column {
			case "column_2":
				v, err := strconv.Atoi(value)
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
				data[column] = v
			default:
				data[column] = value
			}
		}

		// Insert the data into MongoDB
		// Update the record with upsert set to true
		update := bson.D{
			{Key: "$set", Value: data},
			{Key: "$setOnInsert", Value: bson.D{
				{Key: "onInsertDate", Value: time.Now()},
			}},
		}
		updateOptions := options.Update().SetUpsert(true)

		_, err = collection.UpdateOne(context.Background(), bson.D{{Key: "symbol", Value: data["symbol"]}}, update, updateOptions)
		if err != nil {
			fmt.Println("Upsert failed", err)
			os.Exit(1)
		}
	}
	fmt.Print("Upload Completed \n")
}
