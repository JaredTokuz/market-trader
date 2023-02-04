package etl

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/mitchellh/mapstructure"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type financialIntruments struct {
	processConfig
}

type FinancialInstruments interface {
	ProcessConfig
}

func (config *financialIntruments) getHttpResponse() error {
	client := http.Client{}
	req, err := http.NewRequest("GET", instruments_url, nil)
	req.Header.Add("Authorization", "Bearer "+config.token)
	query := req.URL.Query()
	query.Add("apikey", config.apikey)
	query.Add("projection", "fundamental")
	query.Add("symbol", config.workdoc.Symbol)
	req.URL.RawQuery = query.Encode()
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	var body interface{}
	err = json.NewDecoder(resp.Body).Decode(&body)

	_, err = config.database.Collection("HttpResponses").UpdateOne(context.TODO(),
		bson.M{"symbol": config.workdoc.Symbol},
		bson.M{"$set": bson.M{"resp": body, "status": resp.StatusCode, "work_doc": config.workdoc}},
		options.Update().SetUpsert(true))

	return nil
}

func (config *financialIntruments) transform(id primitive.ObjectID) error {
	var data map[string]interface{}

	err := config.database.Collection("HttpResponse").FindOneAndUpdate(context.TODO(),
		bson.M{"_id": id, "lock": bson.M{"$eq": false}},
		bson.M{"lock": true}).Decode(&data)
	if err != nil {
		return err
	}
	// the payload is nested inside the symbol
	payload := data[config.workdoc.Symbol]

	instrument := Instrument{}
	err = mapstructure.Decode(payload, &instrument)
	if err != nil {
		return errors.New("Type assertion instrument not ok!")
	}
	// instrument, ok := payload.(Instrument)

	instrument.Fundamental.MarketCap = round(instrument.Fundamental.MarketCap)

	// we exit earlier and save a smaller payload if marketcap is less than 500 million
	if instrument.Fundamental.MarketCap < 500 {
		_, err = config.database.Collection("macros").UpdateOne(context.TODO(),
			bson.M{"symbol": config.workdoc.Symbol},
			bson.M{"$set": bson.M{"marketCap": instrument.Fundamental.MarketCap}},
			options.Update().SetUpsert(true))
		return nil
	}

	instrument.Fundamental.High52 = round(instrument.Fundamental.High52)
	instrument.Fundamental.Low52 = round(instrument.Fundamental.Low52)
	instrument.Fundamental.DividendAmount = round(instrument.Fundamental.DividendAmount)
	instrument.Fundamental.DividendYield = round(instrument.Fundamental.DividendYield)
	instrument.Fundamental.PeRatio = round(instrument.Fundamental.PeRatio)
	instrument.Fundamental.PegRatio = round(instrument.Fundamental.PegRatio)
	instrument.Fundamental.PbRatio = round(instrument.Fundamental.PbRatio)
	instrument.Fundamental.PrRatio = round(instrument.Fundamental.PrRatio)
	instrument.Fundamental.PcfRatio = round(instrument.Fundamental.PcfRatio)
	instrument.Fundamental.GrossMarginTTM = round(instrument.Fundamental.GrossMarginTTM)
	instrument.Fundamental.GrossMarginMRQ = round(instrument.Fundamental.GrossMarginMRQ)
	instrument.Fundamental.NetProfitMarginTTM = round(instrument.Fundamental.NetProfitMarginTTM)
	instrument.Fundamental.NetProfitMarginMRQ = round(instrument.Fundamental.NetProfitMarginMRQ)
	instrument.Fundamental.OperatingMarginTTM = round(instrument.Fundamental.OperatingMarginTTM)
	instrument.Fundamental.OperatingMarginMRQ = round(instrument.Fundamental.OperatingMarginMRQ)
	instrument.Fundamental.ReturnOnEquity = round(instrument.Fundamental.ReturnOnEquity)
	instrument.Fundamental.ReturnOnAssets = round(instrument.Fundamental.ReturnOnAssets)
	instrument.Fundamental.ReturnOnInvestment = round(instrument.Fundamental.ReturnOnInvestment)
	instrument.Fundamental.QuickRatio = round(instrument.Fundamental.QuickRatio)
	instrument.Fundamental.CurrentRatio = round(instrument.Fundamental.CurrentRatio)
	instrument.Fundamental.InterestCoverage = round(instrument.Fundamental.InterestCoverage)
	instrument.Fundamental.TotalDebtToCapital = round(instrument.Fundamental.TotalDebtToCapital)
	instrument.Fundamental.LtDebtToEquity = round(instrument.Fundamental.LtDebtToEquity)
	instrument.Fundamental.TotalDebtToEquity = round(instrument.Fundamental.TotalDebtToEquity)
	instrument.Fundamental.EpsTTM = round(instrument.Fundamental.EpsTTM)
	instrument.Fundamental.EpsChangePercentTTM = round(instrument.Fundamental.EpsChangePercentTTM)
	instrument.Fundamental.EpsChangeYear = round(instrument.Fundamental.EpsChangeYear)
	instrument.Fundamental.RevChangeTTM = round(instrument.Fundamental.RevChangeTTM)
	instrument.Fundamental.MarketCapFloat = round(instrument.Fundamental.MarketCapFloat)
	instrument.Fundamental.BookValuePerShare = round(instrument.Fundamental.BookValuePerShare)
	instrument.Fundamental.DividendPayAmount = round(instrument.Fundamental.DividendPayAmount)
	instrument.Fundamental.Beta = round(instrument.Fundamental.Beta)

	config.update(instrument)

	_, err = config.database.Collection("HttpResponse").DeleteOne(
		context.TODO(),
		bson.M{"_id": id})
	if err != nil {
		return err
	}

	return nil
}

func (config *financialIntruments) update(intruments Instrument) error {
	_, err := config.database.Collection("macros").UpdateOne(context.TODO(),
		bson.M{"symbol": config.workdoc.Symbol},
		bson.M{"$set": intruments},
		options.Update().SetUpsert(true))
	if err != nil {
		return err
	}
	return nil
}
