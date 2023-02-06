package tests

import (
	"testing"
	"time"

	"github.com/jaredtokuz/market-trader/etl"
)

func TestTransformLoad(t *testing.T) {
	td := setTDApiService()
	mc := setController()

	for _, j := range getEtlJobs() {
		c := etl.EtlConfig{
			Symbol: "TSLA",
			Work:   j,
		}
		success, err := td.Call(c)
		time.Sleep(500)
		if err != nil {
			t.Error("Call failed")
		}
		err = etl.TransformLoad(*mc, *success)
		if err != nil {

		}
	}

}
