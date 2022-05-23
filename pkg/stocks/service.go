package stocks

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"strconv"
	"strings"
	"time"

	"github.com/jaredtokuz/market-trader/pkg/helpers"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//Service is an interface from which our api module can access our repository of all our models
type Service interface {
	FindOneSymbol(collection string, symbol string) (*interface{}, error)
	UploadStocks(filecontent multipart.File) (error)
}

type service struct {
	repository Repository
}

//NewService is used to create a single instance of the service
func NewService(r Repository) Service {
	return &service{
		repository: r,
	}
}

func (s *service) FindOneSymbol(collection string, symbol string) (*interface{}, error) {
	opts := options.FindOneOptions{}
	return s.repository.FindOne(collection, bson.M{"symbol": symbol}, &opts)
}


/** functionality for the csv upload related to www.eoddata.com/download.aspx csv files: nasdaq & nyse */
func (s *service) UploadStocks(fileContent multipart.File) error {
	reader := csv.NewReader(fileContent)
	columns, err := reader.Read()
	if err != nil {
		return err
	}

	var operations []mongo.WriteModel
	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(false)
	for {
		line, err := reader.Read()
		// handle EOF
		if err == io.EOF {
			if len(operations) > 0 {
				err := s.repository.BulkWrite("stocks", operations, &bulkOption)
				if err != nil {
					return err
				}
				operations = nil
			}
			fmt.Println("Done")
			break
		} else if err != nil {
			return err
		}
		// bulk write every 100
		if len(operations) == 100 {
			err := s.repository.BulkWrite("stocks", operations, &bulkOption)
			if err != nil {
				return err
			}
			operations = nil
			fmt.Println("Batch processed")
		}

		
		update := bson.M{}
		symbol := ""
		for i, field := range line {
			if columns[i] != "Symbol" {
				continue // skip this column
			}
			symbol = field
			break
		}
		
		if strings.Contains(symbol, ".") == true {
			continue
		}
		if strings.Contains(symbol, "-") == true {
			continue
		}
		fmt.Println(line)
		update["symbol"] = symbol
		
		filter := bson.M{}
		if update["symbol"] == nil {
			return errors.New("Symbol is missing from the csv file there is something wrong here")
		}
		filter["symbol"] = update["symbol"]
		operations = helpers.AppendUpsertOne(operations, filter, update)
	}
	err = s.repository.UpdateOne("info", bson.M{}, bson.M{"$set": bson.M{"upload_stock_dt": time.Now()} }, &options.UpdateOptions{})
	if err != nil {
		return err
	}
	return nil
}

func million(i int) int {
	return i * 1000 * 1000
}


/** old functionality for the csv upload related to nasdaq.com csv files: nasdaq & nyse */
func (s *service) UploadStocksOld(fileContent multipart.File) error {
	reader := csv.NewReader(fileContent)
	columns, err := reader.Read()
	if err != nil {
		return err
	}

	var operations []mongo.WriteModel
	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(false)
	for {
		line, err := reader.Read()
		// handle EOF
		if err == io.EOF {
			if len(operations) > 0 {
				err := s.repository.BulkWrite("stocks", operations, &bulkOption)
				if err != nil {
					return err
				}
				operations = nil
			}
			break
		} else if err != nil {
			return err
		}
		// bulk write every 100
		if len(operations) == 100 {
			err := s.repository.BulkWrite("stocks", operations, &bulkOption)
			if err != nil {
				return err
			}
			operations = nil
		}

		fmt.Println(line)
		
		update := bson.M{}
		marketCapFailed := false
		for i, field := range line {
			fmt.Printf("%d: %s\n", i, field)
			if helpers.StringInSlice(columns[i], []string{"Symbol","Name","Market Cap","Country","IPO Year","Sector","Industry"}) == false {
				continue // skip this column
			}
			/* filter out Market Cap */
			if columns[i] == "Market Cap" {
				marketCap, err := strconv.Atoi(field)
				if err != nil {
					return err
				}
				if marketCap > million(500) {
					fmt.Print(marketCap, " higher than 500 mil")
				} else {
					marketCapFailed = true
					break // skip this record
				}
			}
			update[columns[i]] = field
		}
		if marketCapFailed == true {
			continue
		}
		
		filter := bson.M{}
		if update["Symbol"] == nil {
			return errors.New("Symbol is missing from the csv file there is something wrong here")
		}
		filter["Symbol"] = update["Symbol"]
		operations = helpers.AppendUpsertOne(operations, filter, update)
		fmt.Println("The len of operations: ", len(operations))
	}
	return nil
}
