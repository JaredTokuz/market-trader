package work

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/jaredtokuz/market-trader/pkg/helpers"
	"github.com/jaredtokuz/market-trader/pkg/token"
	"github.com/montanaflynn/stats"

	"github.com/mitchellh/mapstructure"
)

type Worker interface {
	InitWork() error
}
type worker struct {
	Database *mongo.Database
	APIKey		string
	TokenHandler token.AccessTokenService
}

type WorkStatus struct {
	isWorking bool
}

var ErrAlreadyWorking = errors.New("already working")

//NewWorker is the single instance worker that is being created.
func NewWorker(database *mongo.Database, api_key string, tokenHandler token.AccessTokenService) Worker {
	return &worker{
		Database: database,
		APIKey: api_key,
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
	var workDoc WorkDoc
	for {
		result := w.Database.Collection(WORK).FindOne(context.TODO(), bson.M{})
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

		switch workDoc.Work {
			case "YearDaily":
				err = yearDailyWork(w.Database, w.APIKey, token, workDoc)
			case "Day15Minute30":
				err = day15Minute30(w.Database, w.APIKey, token, workDoc)
			case "Day2Minute15":
				err = day2Minute15(w.Database, w.APIKey, token, workDoc)
			case "Minute15Signals":
				err = minute15Signals(w.Database, w.APIKey, token, workDoc)
		}
		if err != nil {
			w.Database.Collection("error-logs").InsertOne(context.TODO(), bson.M{"msg": err.Error() })
		}
		// Delete the record after finishing
		_, err = w.Database.Collection(WORK).DeleteOne(context.TODO(), bson.M{"_id": workDoc.ID})
		if err != nil {
			return err
		}
	}
	w.Database.Collection("work-status").UpdateOne(context.TODO(), bson.M{}, bson.M{"isWorking": false})
	return nil
}

func yearDailyWork(db *mongo.Database, api_key string, token string, workDoc WorkDoc) error {
	client := http.Client{}
	url := "https://api.tdameritrade.com/v1/instruments"
	req, err := http.NewRequest("GET", url, nil)
	req.Header.Add("Authorization", "Bearer " + token)
	query := req.URL.Query()
	query.Add("apikey", api_key)
	query.Add("projection", "fundamental")
	query.Add("symbol", workDoc.Symbol)
	req.URL.RawQuery = query.Encode()
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	time.Sleep(1300 * time.Millisecond) // adjust for 120 per minute throttle
	defer resp.Body.Close()
	opts := options.UpdateOptions{}
	opts.SetUpsert(true)

	if resp.StatusCode >= 400 {
		var body interface{}
		err = json.NewDecoder(resp.Body).Decode(&body)
		if err != nil {
				return err
		}
		_, err = db.Collection("stocks").UpdateOne(context.TODO(),
			bson.M{"symbol": workDoc.Symbol},
			bson.M{"$set": bson.M{"error": body } },
			&opts)
		errmsg := fmt.Sprintf("%s - %s", workDoc.Symbol, workDoc.Work)
		return errors.New(errmsg)
	}

	var data map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
			return err
	}
	// the payload is nested inside the symbol
	payload := data[workDoc.Symbol]

	instrument := Instrument{}
	err = mapstructure.Decode(payload, &instrument)
	if err != nil {
		return errors.New("Type assertion instrument not ok!")
	}
	// instrument, ok := payload.(Instrument)

	instrument.Fundamental.MarketCap = round(instrument.Fundamental.MarketCap)

	if instrument.Fundamental.MarketCap < 500 {
		_, err = db.Collection("stocks").UpdateOne(context.TODO(),
			bson.M{"symbol": workDoc.Symbol},
			bson.M{"$set": bson.M{"marketCap": instrument.Fundamental.MarketCap }, "$unset": bson.M{ "error": "" } },
			&opts)
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
	
	_, err = db.Collection("stocks").UpdateOne(context.TODO(),
		bson.M{"symbol": workDoc.Symbol},
		bson.M{"$set": instrument, "$unset": bson.M{ "error": "" } },
		&opts)
	if err != nil {
		return err
	}
	
	return nil
}

func day15Minute30(db *mongo.Database, api_key string, token string, workDoc WorkDoc) error {
	endDate := helpers.NextDay(helpers.Bod(time.Now()))
	startDate := endDate.AddDate(0,0,-15)
	var phinp = PriceHistoryInput {
		apikey: api_key, 
		periodType: "day", 
		frequencyType: "minute", 
		frequency: "30",
		startDate: strconv.FormatInt(startDate.Unix()*1000, 10),
		endDate: strconv.FormatInt(endDate.Unix()*1000, 10),
		needExtendedHoursData: "true",
		token: token,
		symbol: workDoc.Symbol,
	}
	ph, err := getPriceHistory(phinp)
	if err != nil {
		return err
	}

	// TODO some logic that adds a signal/prereq signal
	adjph, err := createAdjPriceHistory(ph)
	if err != nil {
		return err
	}

	opts := options.UpdateOptions{}
	opts.SetUpsert(true)
	_, err = db.Collection("day15min30").UpdateOne(context.TODO(),
		bson.M{"symbol": ph.Symbol},
		bson.M{"$set": adjph},
		&opts)
	if err != nil {
		return err
	}

	return nil
}

func day2Minute15(db *mongo.Database, api_key string, token string, workDoc WorkDoc) error {
	endDate := helpers.NextDay(helpers.Bod(time.Now()))
	startDate := endDate.AddDate(0,0,-2)
	var phinp = PriceHistoryInput {
		apikey: api_key, 
		periodType: "day", 
		frequencyType: "minute", 
		frequency: "15",
		startDate: strconv.FormatInt(startDate.Unix()*1000, 10),
		endDate: strconv.FormatInt(endDate.Unix()*1000, 10),
		needExtendedHoursData: "true",
		token: token,
		symbol: workDoc.Symbol,
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

	opts := options.UpdateOptions{}
	opts.SetUpsert(true)
	_, err = db.Collection("day2min15").UpdateOne(context.TODO(),
		bson.M{"symbol": adjph.Symbol},
		bson.M{"$set": adjph},
		&opts)
	if err != nil {
		return err
	}
	
	return nil
}
// 56 bars...
func minute15Signals(db *mongo.Database, api_key string, token string, workDoc WorkDoc) error {
	endDate := helpers.NextDay(helpers.Bod(time.Now()))
	startDate := endDate.Add(time.Hour * -14)
	var phinp = PriceHistoryInput {
		apikey: api_key, 
		periodType: "day", 
		frequencyType: "minute", 
		frequency: "15",
		startDate: strconv.FormatInt(startDate.Unix()*1000, 10),
		endDate: strconv.FormatInt(endDate.Unix()*1000, 10),
		needExtendedHoursData: "true",
		token: token,
		symbol: workDoc.Symbol,
	}
	ph, err := getPriceHistory(phinp)
	if err != nil {
		return err
	}
	
	adjph, err := createAdjPriceHistory(ph)
	if err != nil {
		return err
	}

	opts := options.UpdateOptions{}
	opts.SetUpsert(true)
	_, err = db.Collection("min15signals").UpdateOne(context.TODO(),
		bson.M{"symbol": adjph.Symbol},
		bson.M{"$set": adjph},
		&opts)
	if err != nil {
		return err
	}
	
	return nil
}

func createAdjPriceHistory(ph *PriceHistory) (*AdjPriceHistory, error) {
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
	var adjCandles []AdjCandle
	var zscore float64
	for _, candle := range ph.Candles {
		zscore = round1((float64(candle.Volume) - meanVol) / stdVol)
		adjCandles = append(adjCandles, AdjCandle{
			Volume: candle.Volume,
			High: candle.High,
			Low: candle.Low,
			Open: candle.Open,
			Close: candle.Close,
			Datetime: candle.Datetime,
			VolZScore: zscore,
		})
	}

	adjph := AdjPriceHistory{
		Symbol: ph.Symbol,
		Candles: adjCandles,
		MeanVolume: int(meanVol),
		StdVolume: int(stdVol),
	}

	return &adjph, nil
}

func round1(number float64) float64 {
	n, _ :=stats.Round(number, 1)
	return n
}

func round(number float64) float64 {
	n, _ :=stats.Round(number, 2)
	return n
}

type PriceHistoryInput struct {
	token string
	symbol string
	apikey string
	periodType string // default day
	frequencyType string // ex minute, daily
	frequency string // int ex 5
	startDate string // unix mseconds int
	endDate string // unix mseconds int
	needExtendedHoursData string // bool
}

func getPriceHistory(params PriceHistoryInput) (*PriceHistory, error) {
	// TODO add the retryiable http from hashicorp
	client := http.Client{}
	url := fmt.Sprintf("https://api.tdameritrade.com/v1/marketdata/%v/pricehistory", params.symbol)
	req, err := http.NewRequest("GET", url, nil)
	req.Header.Add("Authorization", "Bearer " + params.token)
	query := req.URL.Query()
	query.Add("apikey", params.apikey)
	query.Add("periodType", params.periodType)
	query.Add("frequencyType", params.frequencyType)
	query.Add("frequency", params.frequency)
	query.Add("endDate", params.endDate)
	query.Add("startDate", params.startDate)
	query.Add("needExtendedHoursData", params.needExtendedHoursData)
	req.URL.RawQuery = query.Encode()
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		b, err := io.ReadAll(resp.Body)
		if err != nil {
				return nil, err
		}
		errmsg := fmt.Sprintf("%s - %s", params.symbol, string(b))
		return nil, errors.New("Price History Response: " + errmsg)
	}

	var priceHistoryResp PriceHistory
	err = json.NewDecoder(resp.Body).Decode(&priceHistoryResp)
	if err != nil {
			return nil, err
	}

	return &priceHistoryResp, nil
}

type Candle struct {
	Datetime	uint64 `json:"datetime" bson:"datetime"`
	Close			float32	`json:"close" bson:"close"`
	High			float32 `json:"high" bson:"high"`
	Low				float32 `json:"low" bson:"low"`
	Open			float32 `json:"open" bson:"open"`
	Volume		int `json:"volume" bson:"volume"`
}

type PriceHistory struct {
	Candles		[]Candle					 `json:"candles" bson:"candles"`
	Symbol		string						 `json:"symbol" bson:"symbol"`
}

type AdjCandle struct {
	Datetime		uint64 `json:"datetime" bson:"datetime"`
	Close			float32	`json:"close" bson:"close"`
	High			float32 `json:"high" bson:"high"`
	Low				float32 `json:"low" bson:"low"`
	Open			float32 `json:"open" bson:"open"`
	Volume			int `json:"volume" bson:"volume"`
	VolZScore		float64 `json:"volzscore" bson:"volzscore"`
}

type AdjPriceHistory struct {
	Candles		[]AdjCandle					 `json:"candles" bson:"candles"`
	Symbol		string						 `json:"symbol" bson:"symbol"`
	MeanVolume int								`json:"meanVolume" bson:"meanVolume"`
	StdVolume int								`json:"stdVolume" bson:"stdVolume"`
}

type StockDoc struct {
	ID        primitive.ObjectID `json:"id,omitempty"  bson:"_id,omitempty"`
	Symbol 		string						 `json:"symbol,omitempty"  bson:"symbol,omitempty"`
	// Name 		string						 `json:"name,omitempty"  bson:"name,omitempty"`
	// MarketCap 		string						 `json:"marketcap,omitempty"  bson:"marketcap,omitempty"`
	// Country 		string						 `json:"country,omitempty"  bson:"country,omitempty"`
	// IPO 		string						 `json:"ipo,omitempty"  bson:"ipo,omitempty"`
	// Sector 		string						 `json:"sector,omitempty"  bson:"sector,omitempty"`
	// Industry 		string						 `json:"industry,omitempty"  bson:"industry,omitempty"`
	// AvgVolume 		int						 `json:"avgVolume,omitempty"  bson:"avgVolume,omitempty"`
}

type Instrument struct {
	Fundamental Fundamental  `json:"fundamental" bson:"fundamental"`
	Cusip string `json:"cusip" bson:"cusip"`
	Symbol string `json:"symbol" bson:"symbol"`
	Description string `json:"description" bson:"description"`
	Exchange string `json:"exchange" bson:"exchange"`
	// assetType string `json:"assetType,omitempty" bson:"assetType,omitempty"`
}

type Fundamental struct {
	Symbol string `json:"symbol" bson:"symbol"`
	High52 float64 `json:"high52" bson:"high52"`
	Low52 float64 `json:"low52" bson:"low52"`
	DividendAmount float64 `json:"dividendAmount" bson:"dividendAmount"`
	DividendYield float64 `json:"dividendYield" bson:"dividendYield"`
	DividendDate string `json:"dividendDate" bson:"dividendDate"`
	PeRatio float64 `json:"peRatio" bson:"peRatio"`
	PegRatio float64 `json:"pegRatio" bson:"pegRatio"`
	PbRatio float64 `json:"pbRatio" bson:"pbRatio"`
	PrRatio float64 `json:"prRatio" bson:"prRatio"`
	PcfRatio float64 `json:"pcfRatio" bson:"pcfRatio"`
	GrossMarginTTM float64 `json:"grossMarginTTM" bson:"grossMarginTTM"`
	GrossMarginMRQ float64 `json:"grossMarginMRQ" bson:"grossMarginMRQ"`
	NetProfitMarginTTM float64 `json:"netProfitMarginTTM" bson:"netProfitMarginTTM"`
	NetProfitMarginMRQ float64 `json:"netProfitMarginMRQ" bson:"netProfitMarginMRQ"`
	OperatingMarginTTM float64 `json:"operatingMarginTTM" bson:"operatingMarginTTM"`
	OperatingMarginMRQ float64 `json:"operatingMarginMRQ" bson:"operatingMarginMRQ"`
	ReturnOnEquity float64 `json:"returnOnEquity" bson:"returnOnEquity"`
	ReturnOnAssets float64 `json:"returnOnAssets" bson:"returnOnAssets"`
	ReturnOnInvestment float64 `json:"returnOnInvestment" bson:"returnOnInvestment"`
	QuickRatio float64 `json:"quickRatio" bson:"quickRatio"`
	CurrentRatio float64 `json:"currentRatio" bson:"currentRatio"`
	InterestCoverage float64 `json:"interestCoverage" bson:"interestCoverage"`
	TotalDebtToCapital float64 `json:"totalDebtToCapital" bson:"totalDebtToCapital"`
	LtDebtToEquity float64 `json:"ltDebtToEquity" bson:"ltDebtToEquity"`
	TotalDebtToEquity float64 `json:"totalDebtToEquity" bson:"totalDebtToEquity"`
	EpsTTM float64 `json:"epsTTM" bson:"epsTTM"`
	EpsChangePercentTTM float64 `json:"epsChangePercentTTM" bson:"epsChangePercentTTM"`
	EpsChangeYear float64 `json:"epsChangeYear" bson:"epsChangeYear"`
	EpsChange  int `json:"epsChange" bson:"epsChange"`
	RevChangeYear  int `json:"revChangeYear" bson:"revChangeYear"`
	RevChangeTTM float64 `json:"revChangeTTM" bson:"revChangeTTM"`
	RevChangeIn  int `json:"revChangeIn" bson:"revChangeIn"`
	SharesOutstanding  int `json:"sharesOutstanding" bson:"sharesOutstanding"`
	MarketCapFloat float64 `json:"marketCapFloat" bson:"marketCapFloat"`
	MarketCap  float64 `json:"marketCap" bson:"marketCap"`
	BookValuePerShare float64 `json:"bookValuePerShare" bson:"bookValuePerShare"`
	ShortIntToFloat  int `json:"shortIntToFloat" bson:"shortIntToFloat"`
	ShortIntDayToCover  int `json:"shortIntDayToCover" bson:"shortIntDayToCover"`
	DivGrowthRate3Year  int `json:"divGrowthRate3Year" bson:"divGrowthRate3Year"`
	DividendPayAmount float64 `json:"dividendPayAmount" bson:"dividendPayAmount"`
	DividendPayDate string `json:"dividendPayDate" bson:"dividendPayDate"`
	Beta float64 `json:"beta" bson:"beta"`
	Vol1DayAvg  int `json:"vol1DayAvg" bson:"vol1DayAvg"`
	Vol10DayAvg  int `json:"vol10DayAvg" bson:"vol10DayAvg"`
	Vol3MonthAvg  int `json:"vol3MonthAvg" bson:"vol3MonthAvg"`
}