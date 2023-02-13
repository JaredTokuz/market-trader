package etl

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/jaredtokuz/market-trader/shared"
	"github.com/jaredtokuz/market-trader/token"

	retryablehttp "github.com/hashicorp/go-retryablehttp"
)

type tdapiconfig struct {
	mongo  *MongoController
	apikey string
	token  token.AccessTokenService
}

type TDApiService interface {
	Call(etlConfig EtlConfig) (*ApiCallSuccess, error)                                      /* Makes a request */
	AddAuth(req *http.Request)                                                              /* helper */
	AddApiKey(req *url.Values)                                                              /* helper */
	InsertResponse(etlConfig EtlConfig, resp *http.Response, decodedBody interface{}) error /* log api response */
}

func NewTDApiService(
	mongo *MongoController,
	apikey string,
	token token.AccessTokenService,
) TDApiService {
	return &tdapiconfig{mongo: mongo, apikey: apikey, token: token}
}

func (i *tdapiconfig) Call(etlConfig EtlConfig) (*ApiCallSuccess, error) {
	retryClient := retryablehttp.NewClient()

	retryClient.RetryMax = 4
	retryClient.RetryWaitMin = time.Duration(1) * time.Second
	retryClient.RetryWaitMin = time.Duration(3) * time.Second

	client := retryClient.StandardClient() // convert to *http.Client

	var (
		req *http.Request
		err error
	)
	// Dynamically set url/method
	switch etlConfig.Work {
	case Macros:
		req, err = http.NewRequest("GET", InstrumentsUrl, nil)
	case Medium, Short, Signals:
		req, err = http.NewRequest("GET", PriceHistoryUrl(etlConfig.Symbol), nil)
	}
	query := req.URL.Query()
	i.AddAuth(req)
	i.AddApiKey(&query)

	// Dynamically add query params
	switch etlConfig.Work {
	case Macros:
		query.Add("projection", "fundamental")
		query.Add("symbol", etlConfig.Symbol)
	case Medium:
		endDate := shared.NextDay(shared.Bod(time.Now()))
		startDate := endDate.AddDate(0, 0, -15)
		i.AddFetchPriceHistoryQuery(&query, PriceHistoryQuery{
			periodType:            "day",
			frequencyType:         "minute",
			frequency:             "30",
			startDate:             stringFormatDate(startDate),
			endDate:               stringFormatDate(endDate),
			needExtendedHoursData: "true",
		})
	case Short, Signals:
		endDate := shared.NextDay(shared.Bod(time.Now()))
		startDate := endDate.Add(time.Hour * -14)
		i.AddFetchPriceHistoryQuery(&query, PriceHistoryQuery{
			periodType:            "day",
			frequencyType:         "minute",
			frequency:             "15",
			startDate:             stringFormatDate(startDate),
			endDate:               stringFormatDate(endDate),
			needExtendedHoursData: "true",
		})
	}

	req.URL.RawQuery = query.Encode()
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	var body interface{}
	err = json.NewDecoder(resp.Body).Decode(&body)
	i.InsertResponse(etlConfig, resp, body)
	if resp.StatusCode >= 400 {
		return nil, errors.New("Api call failed with status code: " + strconv.Itoa(resp.StatusCode))
	}
	return CreateApiSuccess(body, etlConfig), nil
}

func (i *tdapiconfig) AddAuth(req *http.Request) {
	req.Header.Add("Authorization", "Bearer "+i.token.Fetch())
}

func (i *tdapiconfig) AddApiKey(query *url.Values) {
	query.Add("apikey", i.apikey)
}

type PriceHistoryQuery struct {
	periodType            string // default day
	frequencyType         string // ex minute, daily
	frequency             string // int ex 5
	startDate             string // unix mseconds int
	endDate               string // unix mseconds int
	needExtendedHoursData string // bool
}

func (i *tdapiconfig) AddFetchPriceHistoryQuery(query *url.Values, p PriceHistoryQuery) {
	query.Add("periodType", p.periodType)
	query.Add("frequencyType", p.frequencyType)
	query.Add("frequency", p.frequency)
	query.Add("startDate", p.startDate)
	query.Add("endDate", p.endDate)
	query.Add("needExtendedHoursData", p.needExtendedHoursData)

}

// log the api calls in table for transparency and analysis
func (i *tdapiconfig) InsertResponse(etlConfig EtlConfig, resp *http.Response, decodedBody interface{}) error {
	document := HttpResponsesDocument{
		EtlConfig: etlConfig,
		Response: APIResponse{
			Body:    decodedBody,
			Status:  resp.StatusCode,
			Request: shared.FormatRequest(resp.Request),
		},
	}
	err := i.mongo.ApiCalls.Cache(etlConfig, document)
	if err != nil {
		return err
	}
	return nil
}

type ApiCallSuccess struct {
	Body      interface{}
	etlConfig EtlConfig
}

func CreateApiSuccess(body interface{}, etlConfig EtlConfig) *ApiCallSuccess {
	return &ApiCallSuccess{Body: body, etlConfig: etlConfig}
}

func stringFormatDate(t time.Time) string {
	return strconv.FormatInt(t.Unix()*1000, 10)
}
