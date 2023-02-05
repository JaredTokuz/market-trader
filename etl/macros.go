package etl

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type instrumentQuery struct {
	ProcessBuilder
}

type Instrument struct {
	ID          primitive.ObjectID `json:"id,omitempty"  bson:"_id,omitempty"`
	Fundamental Fundamental        `json:"fundamental" bson:"fundamental"`
	Cusip       string             `json:"cusip" bson:"cusip"`
	Symbol      string             `json:"symbol" bson:"symbol"`
	Description string             `json:"description" bson:"description"`
	Exchange    string             `json:"exchange" bson:"exchange"`
	// assetType string `json:"assetType,omitempty" bson:"assetType,omitempty"`
}

type Fundamental struct {
	Symbol              string  `json:"symbol" bson:"symbol"`
	High52              float64 `json:"high52" bson:"high52"`
	Low52               float64 `json:"low52" bson:"low52"`
	DividendAmount      float64 `json:"dividendAmount" bson:"dividendAmount"`
	DividendYield       float64 `json:"dividendYield" bson:"dividendYield"`
	DividendDate        string  `json:"dividendDate" bson:"dividendDate"`
	PeRatio             float64 `json:"peRatio" bson:"peRatio"`
	PegRatio            float64 `json:"pegRatio" bson:"pegRatio"`
	PbRatio             float64 `json:"pbRatio" bson:"pbRatio"`
	PrRatio             float64 `json:"prRatio" bson:"prRatio"`
	PcfRatio            float64 `json:"pcfRatio" bson:"pcfRatio"`
	GrossMarginTTM      float64 `json:"grossMarginTTM" bson:"grossMarginTTM"`
	GrossMarginMRQ      float64 `json:"grossMarginMRQ" bson:"grossMarginMRQ"`
	NetProfitMarginTTM  float64 `json:"netProfitMarginTTM" bson:"netProfitMarginTTM"`
	NetProfitMarginMRQ  float64 `json:"netProfitMarginMRQ" bson:"netProfitMarginMRQ"`
	OperatingMarginTTM  float64 `json:"operatingMarginTTM" bson:"operatingMarginTTM"`
	OperatingMarginMRQ  float64 `json:"operatingMarginMRQ" bson:"operatingMarginMRQ"`
	ReturnOnEquity      float64 `json:"returnOnEquity" bson:"returnOnEquity"`
	ReturnOnAssets      float64 `json:"returnOnAssets" bson:"returnOnAssets"`
	ReturnOnInvestment  float64 `json:"returnOnInvestment" bson:"returnOnInvestment"`
	QuickRatio          float64 `json:"quickRatio" bson:"quickRatio"`
	CurrentRatio        float64 `json:"currentRatio" bson:"currentRatio"`
	InterestCoverage    float64 `json:"interestCoverage" bson:"interestCoverage"`
	TotalDebtToCapital  float64 `json:"totalDebtToCapital" bson:"totalDebtToCapital"`
	LtDebtToEquity      float64 `json:"ltDebtToEquity" bson:"ltDebtToEquity"`
	TotalDebtToEquity   float64 `json:"totalDebtToEquity" bson:"totalDebtToEquity"`
	EpsTTM              float64 `json:"epsTTM" bson:"epsTTM"`
	EpsChangePercentTTM float64 `json:"epsChangePercentTTM" bson:"epsChangePercentTTM"`
	EpsChangeYear       float64 `json:"epsChangeYear" bson:"epsChangeYear"`
	EpsChange           int     `json:"epsChange" bson:"epsChange"`
	RevChangeYear       int     `json:"revChangeYear" bson:"revChangeYear"`
	RevChangeTTM        float64 `json:"revChangeTTM" bson:"revChangeTTM"`
	RevChangeIn         int     `json:"revChangeIn" bson:"revChangeIn"`
	SharesOutstanding   int     `json:"sharesOutstanding" bson:"sharesOutstanding"`
	MarketCapFloat      float64 `json:"marketCapFloat" bson:"marketCapFloat"`
	MarketCap           float64 `json:"marketCap" bson:"marketCap"`
	BookValuePerShare   float64 `json:"bookValuePerShare" bson:"bookValuePerShare"`
	ShortIntToFloat     int     `json:"shortIntToFloat" bson:"shortIntToFloat"`
	ShortIntDayToCover  int     `json:"shortIntDayToCover" bson:"shortIntDayToCover"`
	DivGrowthRate3Year  int     `json:"divGrowthRate3Year" bson:"divGrowthRate3Year"`
	DividendPayAmount   float64 `json:"dividendPayAmount" bson:"dividendPayAmount"`
	DividendPayDate     string  `json:"dividendPayDate" bson:"dividendPayDate"`
	Beta                float64 `json:"beta" bson:"beta"`
	Vol1DayAvg          int     `json:"vol1DayAvg" bson:"vol1DayAvg"`
	Vol10DayAvg         int     `json:"vol10DayAvg" bson:"vol10DayAvg"`
	Vol3MonthAvg        int     `json:"vol3MonthAvg" bson:"vol3MonthAvg"`
}

func MacrosETL(config *ProcessConfig) ProcessETL {
	processBuilder := NewProcessBuilder(config)
	return &instrumentQuery{processBuilder}
}

func (i *instrumentQuery) CallApi() (*ApiCallSuccess, error) {
	client := http.Client{}
	req, err := http.NewRequest("GET", InstrumentsUrl, nil)
	i.AddAuth(req)
	query := req.URL.Query()
	i.AddApiKey(&query)
	query.Add("projection", "fundamental")
	query.Add("symbol", i.WorkConfig().Symbol)
	req.URL.RawQuery = query.Encode()
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	var body interface{}
	err = json.NewDecoder(resp.Body).Decode(&body)

	i.InsertResponse(resp, body)

	if resp.StatusCode >= 400 {
		return nil, errors.New("Api call failed")
	}

	return CreateApiSuccess(body, i.WorkConfig()), nil
}

func (i *instrumentQuery) Transform(apiCall *ApiCallSuccess) error {
	instrument := apiCall.Body.(Instrument)

	instrument.Fundamental.MarketCap = Round(instrument.Fundamental.MarketCap)

	// we exit earlier and save a smaller payload if marketcap is less than 500 million
	if instrument.Fundamental.MarketCap < 500 {
		_, err := i.Mongo().Macros.UpdateOne(context.TODO(),
			bson.M{"symbol": i.WorkConfig().Symbol},
			bson.M{"$set": bson.M{"marketCap": instrument.Fundamental.MarketCap}},
			options.Update().SetUpsert(true))
		if err != nil {
			return err
		}
		return nil
	}

	instrument.Fundamental.High52 = Round(instrument.Fundamental.High52)
	instrument.Fundamental.Low52 = Round(instrument.Fundamental.Low52)
	instrument.Fundamental.DividendAmount = Round(instrument.Fundamental.DividendAmount)
	instrument.Fundamental.DividendYield = Round(instrument.Fundamental.DividendYield)
	instrument.Fundamental.PeRatio = Round(instrument.Fundamental.PeRatio)
	instrument.Fundamental.PegRatio = Round(instrument.Fundamental.PegRatio)
	instrument.Fundamental.PbRatio = Round(instrument.Fundamental.PbRatio)
	instrument.Fundamental.PrRatio = Round(instrument.Fundamental.PrRatio)
	instrument.Fundamental.PcfRatio = Round(instrument.Fundamental.PcfRatio)
	instrument.Fundamental.GrossMarginTTM = Round(instrument.Fundamental.GrossMarginTTM)
	instrument.Fundamental.GrossMarginMRQ = Round(instrument.Fundamental.GrossMarginMRQ)
	instrument.Fundamental.NetProfitMarginTTM = Round(instrument.Fundamental.NetProfitMarginTTM)
	instrument.Fundamental.NetProfitMarginMRQ = Round(instrument.Fundamental.NetProfitMarginMRQ)
	instrument.Fundamental.OperatingMarginTTM = Round(instrument.Fundamental.OperatingMarginTTM)
	instrument.Fundamental.OperatingMarginMRQ = Round(instrument.Fundamental.OperatingMarginMRQ)
	instrument.Fundamental.ReturnOnEquity = Round(instrument.Fundamental.ReturnOnEquity)
	instrument.Fundamental.ReturnOnAssets = Round(instrument.Fundamental.ReturnOnAssets)
	instrument.Fundamental.ReturnOnInvestment = Round(instrument.Fundamental.ReturnOnInvestment)
	instrument.Fundamental.QuickRatio = Round(instrument.Fundamental.QuickRatio)
	instrument.Fundamental.CurrentRatio = Round(instrument.Fundamental.CurrentRatio)
	instrument.Fundamental.InterestCoverage = Round(instrument.Fundamental.InterestCoverage)
	instrument.Fundamental.TotalDebtToCapital = Round(instrument.Fundamental.TotalDebtToCapital)
	instrument.Fundamental.LtDebtToEquity = Round(instrument.Fundamental.LtDebtToEquity)
	instrument.Fundamental.TotalDebtToEquity = Round(instrument.Fundamental.TotalDebtToEquity)
	instrument.Fundamental.EpsTTM = Round(instrument.Fundamental.EpsTTM)
	instrument.Fundamental.EpsChangePercentTTM = Round(instrument.Fundamental.EpsChangePercentTTM)
	instrument.Fundamental.EpsChangeYear = Round(instrument.Fundamental.EpsChangeYear)
	instrument.Fundamental.RevChangeTTM = Round(instrument.Fundamental.RevChangeTTM)
	instrument.Fundamental.MarketCapFloat = Round(instrument.Fundamental.MarketCapFloat)
	instrument.Fundamental.BookValuePerShare = Round(instrument.Fundamental.BookValuePerShare)
	instrument.Fundamental.DividendPayAmount = Round(instrument.Fundamental.DividendPayAmount)
	instrument.Fundamental.Beta = Round(instrument.Fundamental.Beta)

	err := i.update(instrument)
	if err != nil {
		return err
	}

	return nil
}

func (i *instrumentQuery) update(intruments Instrument) error {
	_, err := i.Mongo().Macros.UpdateOne(context.TODO(),
		bson.M{"symbol": i.WorkConfig().Symbol},
		bson.M{"$set": intruments},
		options.Update().SetUpsert(true))
	if err != nil {
		return err
	}

	err = i.Finish()
	if err != nil {
		return err
	}
	return nil
}
