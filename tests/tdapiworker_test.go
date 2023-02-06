package tests

import (
	"testing"

	"github.com/jaredtokuz/market-trader/etl"
)

func TestWorkerGeneral(t *testing.T) {
	data := []interface{}{
		etl.SymbolDoc{
			Symbol: "TSLA",
		},
		etl.SymbolDoc{
			Symbol: "MSFT",
		},
	}
	initializeQueueData(data, etl.Macros)
	queueMacros(etl.Medium)
	queueMacros(etl.Short)
	queueMacros(etl.Signals)

	etl.InitWorker()
}
