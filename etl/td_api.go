package etl

import (
	"fmt"
)

const TDA_BASE_URL = "https://api.tdameritrade.com/v1"
const InstrumentsUrl = TDA_BASE_URL + "/instruments"

func PriceHistoryUrl(symbol string) string {
	return fmt.Sprintf(TDA_BASE_URL+"/marketdata/%v/pricehistory", symbol)
}
