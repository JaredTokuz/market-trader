package etl

import (
	"fmt"

	"github.com/montanaflynn/stats"
)

const TDA_BASE_URL = "https://api.tdameritrade.com/v1"
const InstrumentsUrl = TDA_BASE_URL + "/instruments"

func PriceHistoryUrl(symbol string) string {
	return fmt.Sprintf(TDA_BASE_URL+"/marketdata/%v/pricehistory", symbol)
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
	for _, candle := range ph.Candles {
		adjCandles = append(adjCandles, Candle{
			Volume:   candle.Volume,
			High:     Round(candle.High),
			Low:      Round(candle.Low),
			Open:     Round(candle.Open),
			Close:    Round(candle.Close),
			Datetime: candle.Datetime,
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
	Datetime uint64  `json:"datetime" bson:"datetime"`
	Close    float64 `json:"close" bson:"close"`
	High     float64 `json:"high" bson:"high"`
	Low      float64 `json:"low" bson:"low"`
	Open     float64 `json:"open" bson:"open"`
	Volume   int     `json:"volume" bson:"volume"`
}

type PriceHistory struct {
	Candles    []Candle `json:"candles" bson:"candles"`
	Symbol     string   `json:"symbol" bson:"symbol"`
	MeanVolume int      `json:"meanVolume" bson:"meanVolume"`
	StdVolume  int      `json:"stdVolume" bson:"stdVolume"`
}
