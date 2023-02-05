package etl

import (
	"context"
	"errors"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/jaredtokuz/market-trader/helpers"
	"github.com/jaredtokuz/market-trader/token"
	"github.com/montanaflynn/stats"
)

type Worker interface {
	InitWork() error
}
type worker struct {
	Database     *mongo.Database
	APIKey       string
	TokenHandler token.AccessTokenService
}

type WorkStatus struct {
	isWorking bool
}

var ErrAlreadyWorking = errors.New("already working")

// NewWorker is the single instance worker that is being created.
func NewWorker(database *mongo.Database, api_key string, tokenHandler token.AccessTokenService) Worker {
	return &worker{
		Database:     database,
		APIKey:       api_key,
		TokenHandler: tokenHandler,
	}
}

func (w *worker) InitWork() error {
	var workStatus WorkStatus
	res := w.Database.Collection("work-status").FindOne(context.TODO(), bson.M{})
	res.Decode(&workStatus)
	if workStatus.isWorking {
		return ErrAlreadyWorking
	}
	w.Database.Collection("work-status").UpdateOne(context.TODO(), bson.M{}, bson.M{"isWorking": true})
	var workDoc SymbolWorkConfig
	for {
		result := w.Database.Collection("work").FindOne(context.TODO(), bson.M{})
		if result.Err() == mongo.ErrNoDocuments {
			// finished work
			break
		} else if result.Err() != nil {
			return result.Err()
		}
		result.Decode(&workDoc)
		token, err := w.TokenHandler.Fetch()
		if err != nil {
			return err
		}

		// work_config := WorkConfig{database: w.Database, apikey: w.APIKey, token: token, workdoc: workDoc}

		switch workDoc.Work {
		case "YearDaily":
			// err = yearDailyWork(w.Database, w.APIKey, token, workDoc)
		case "Day15Minute30":
			// err = day15Minute30(w.Database, w.APIKey, token, workDoc)
		case "Day2Minute15":
			err = day2Minute15(w.Database, w.APIKey, token, workDoc)
		case "Minute15Signals":
			err = minute15Signals(w.Database, w.APIKey, token, workDoc)
		}
		if err != nil {
			w.Database.Collection("error-logs").InsertOne(context.TODO(), bson.M{"msg": err.Error()})
		}
		// Delete the record after finishing
		_, err = w.Database.Collection("WORK").DeleteOne(context.TODO(), bson.M{"_id": workDoc.ID})
		if err != nil {
			return err
		}
	}
	w.Database.Collection("work-status").UpdateOne(context.TODO(), bson.M{}, bson.M{"isWorking": false})
	return nil
}

func day2Minute15(db *mongo.Database, api_key string, token string, workDoc SymbolWorkConfig) error {
	endDate := helpers.NextDay(helpers.Bod(time.Now()))
	startDate := endDate.AddDate(0, 0, -2)
	var phinp = PriceHistoryInput{
		apikey:                api_key,
		periodType:            "day",
		frequencyType:         "minute",
		frequency:             "15",
		startDate:             strconv.FormatInt(startDate.Unix()*1000, 10),
		endDate:               strconv.FormatInt(endDate.Unix()*1000, 10),
		needExtendedHoursData: "true",
		token:                 token,
		symbol:                workDoc.Symbol,
	}
	ph, err := getPriceHistory(phinp)
	if err != nil {
		return err
	}

	adjph, err := createAdjPriceHistory(ph)
	if err != nil {
		return err
	}

	// TODO some logic that adds a signal/prereq signal

	_, err = db.Collection("day2min15").UpdateOne(context.TODO(),
		bson.M{"symbol": adjph.Symbol},
		bson.M{"$set": adjph},
		options.Update().SetUpsert(true))
	if err != nil {
		return err
	}

	return nil
}

// 56 bars...
func minute15Signals(db *mongo.Database, api_key string, token string, workDoc SymbolWorkConfig) error {
	endDate := helpers.NextDay(helpers.Bod(time.Now()))
	startDate := endDate.Add(time.Hour * -14)
	var phinp = PriceHistoryInput{
		apikey:                api_key,
		periodType:            "day",
		frequencyType:         "minute",
		frequency:             "15",
		startDate:             strconv.FormatInt(startDate.Unix()*1000, 10),
		endDate:               strconv.FormatInt(endDate.Unix()*1000, 10),
		needExtendedHoursData: "true",
		token:                 token,
		symbol:                workDoc.Symbol,
	}
	ph, err := getPriceHistory(phinp)
	if err != nil {
		return err
	}

	adjph, err := createAdjPriceHistory(ph)
	if err != nil {
		return err
	}

	_, err = db.Collection("min15signals").UpdateOne(context.TODO(),
		bson.M{"symbol": adjph.Symbol},
		bson.M{"$set": adjph},
		options.Update().SetUpsert(true))
	if err != nil {
		return err
	}

	return nil
}

func calculatePriceHistory(ph PriceHistory) (*PriceHistory, error) {
	var volumeList []int
	for _, candle := range ph.Candles {
		volumeList = append(volumeList, candle.Volume)
	}
	v := stats.LoadRawData(volumeList)
	meanVol, err := stats.Mean(v)
	if err != nil {
		return nil, err
	}
	stdVol, err := stats.StandardDeviation(v)
	if err != nil {
		return nil, err
	}

	// add new fields to create new struct
	var adjCandles []Candle
	var zscore float64
	for _, candle := range ph.Candles {
		zscore = Round1((float64(candle.Volume) - meanVol) / stdVol)
		adjCandles = append(adjCandles, Candle{
			Volume:    candle.Volume,
			High:      candle.High,
			Low:       candle.Low,
			Open:      candle.Open,
			Close:     candle.Close,
			Datetime:  candle.Datetime,
			VolZScore: zscore,
		})
	}

	adjph := PriceHistory{
		Symbol:     ph.Symbol,
		Candles:    adjCandles,
		MeanVolume: int(meanVol),
		StdVolume:  int(stdVol),
	}

	return &adjph, nil
}

func Round1(number float64) float64 {
	n, _ := stats.Round(number, 1)
	return n
}

func Round(number float64) float64 {
	n, _ := stats.Round(number, 2)
	return n
}

type Candle struct {
	Datetime  uint64  `json:"datetime" bson:"datetime"`
	Close     float32 `json:"close" bson:"close"`
	High      float32 `json:"high" bson:"high"`
	Low       float32 `json:"low" bson:"low"`
	Open      float32 `json:"open" bson:"open"`
	Volume    int     `json:"volume" bson:"volume"`
	VolZScore float64 `json:"volzscore" bson:"volzscore"`
}

type PriceHistory struct {
	Candles    []Candle `json:"candles" bson:"candles"`
	Symbol     string   `json:"symbol" bson:"symbol"`
	MeanVolume int      `json:"meanVolume" bson:"meanVolume"`
	StdVolume  int      `json:"stdVolume" bson:"stdVolume"`
}
