package etl

import (
	"context"
	"strconv"
	"time"

	"github.com/jaredtokuz/market-trader/helpers"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type signals struct {
	ProcessBuilder
}

func SignalsETL(config *ProcessConfig) ProcessETL {
	processBuilder := NewProcessBuilder(config)
	return &medTermQuery{processBuilder}
}

func (i *signals) CallApi() (*ApiCallSuccess, error) {
	endDate := helpers.NextDay(helpers.Bod(time.Now()))
	startDate := endDate.Add(time.Hour * -14)
	var query = PriceHistoryQuery{
		periodType:            "day",
		frequencyType:         "minute",
		frequency:             "15",
		startDate:             strconv.FormatInt(startDate.Unix()*1000, 10),
		endDate:               strconv.FormatInt(endDate.Unix()*1000, 10),
		needExtendedHoursData: "true",
	}
	return i.FetchPriceHistory(query)
}

func (i *signals) Transform(apiCall *ApiCallSuccess) error {
	ph := apiCall.Body.(PriceHistory)

	prices, err := calculatePriceHistory(ph)
	if err != nil {
		return err
	}

	err = i.update(prices)
	if err != nil {
		return err
	}

	return nil
}

func (i *signals) update(ph *PriceHistory) error {
	_, err := i.Mongo().Signals.UpdateOne(context.TODO(),
		bson.M{"symbol": ph.Symbol},
		bson.M{"$set": ph},
		options.Update().SetUpsert(true))
	if err != nil {
		return err
	}

	return nil
}
