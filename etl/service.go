package etl

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"

	"github.com/jaredtokuz/market-trader/helpers"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ProcessConfig struct {
	mongo      MongoController
	apikey     string
	token      string
	workConfig SymbolWorkConfig
}

func NewProcessConfig(
	mongo MongoController,
	apikey string,
	token string,
	workConfig SymbolWorkConfig) *ProcessConfig {
	return &ProcessConfig{mongo: mongo, apikey: apikey, token: token, workConfig: workConfig}
}

type ProcessBuilder interface {
	AddAuth(req *http.Request)                                          /* helper */
	AddApiKey(req *url.Values)                                          /* helper */
	InsertResponse(resp *http.Response, decodedBody interface{}) error  /* log api response */
	Finish() error                                                      /* clean up */
	Mongo() MongoController                                             /* mongo catalog */
	WorkConfig() SymbolWorkConfig                                       /* individual work data */
	SetWorkConfig(workConfig SymbolWorkConfig) SymbolWorkConfig         /* set individual work data */
	FetchPriceHistory(query PriceHistoryQuery) (*ApiCallSuccess, error) /* fetch candles */
}

func NewProcessBuilder(config *ProcessConfig) ProcessBuilder {
	return config
}

type HttpResponsesDocument struct {
	response   APIResponse
	workConfig SymbolWorkConfig
}

type APIResponse struct {
	body    interface{}
	status  int
	request string
}

// log the api calls in table for transparency and analysis
func (p *ProcessConfig) InsertResponse(resp *http.Response, decodedBody interface{}) error {
	document := HttpResponsesDocument{
		workConfig: p.workConfig,
		response: APIResponse{
			body:    decodedBody,
			status:  resp.StatusCode,
			request: helpers.FormatRequest(resp.Request),
		},
	}
	_, err := p.mongo.ApiCalls().UpdateOne(context.TODO(),
		bson.M{"symbol": p.workConfig.Symbol, "work": p.workConfig.Work},
		bson.M{"$set": document},
		options.Update().SetUpsert(true))
	if err != nil {
		return err
	}
	return nil
}

// deletes the doc from the queue since its fully done
func (p *ProcessConfig) Finish() error {
	err := p.mongo.ApiQueue().Remove(p.workConfig)
	if err != nil {
		return err
	}
	return nil
}

func (p *ProcessConfig) AddAuth(req *http.Request) {
	req.Header.Add("Authorization", "Bearer "+p.token)
}

func (p *ProcessConfig) AddApiKey(query *url.Values) {
	query.Add("apikey", p.apikey)
}

func (p *ProcessConfig) Mongo() MongoController {
	return p.mongo
}

func (p *ProcessConfig) WorkConfig() SymbolWorkConfig {
	return p.workConfig
}

func (p *ProcessConfig) SetWorkConfig(workConfig SymbolWorkConfig) SymbolWorkConfig {
	p.workConfig = workConfig
	return p.workConfig
}

type PriceHistoryQuery struct {
	periodType            string // default day
	frequencyType         string // ex minute, daily
	frequency             string // int ex 5
	startDate             string // unix mseconds int
	endDate               string // unix mseconds int
	needExtendedHoursData string // bool
}

func (p *ProcessConfig) FetchPriceHistory(query PriceHistoryQuery) (*ApiCallSuccess, error) {
	client := http.Client{}
	req, err := http.NewRequest("GET", PriceHistoryUrl(p.WorkConfig().Symbol), nil)
	p.AddAuth(req)
	q := req.URL.Query()
	p.AddApiKey(&q)
	q.Add("periodType", query.periodType)
	q.Add("frequencyType", query.frequencyType)
	q.Add("frequency", query.frequency)
	q.Add("startDate", query.startDate)
	q.Add("endDate", query.endDate)
	q.Add("needExtendedHoursData", query.needExtendedHoursData)

	req.URL.RawQuery = q.Encode()
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	var body interface{}
	err = json.NewDecoder(resp.Body).Decode(&body)

	p.InsertResponse(resp, body)

	if resp.StatusCode >= 400 {
		return nil, errors.New("Api call failed")
	}

	return CreateApiSuccess(body, p.WorkConfig()), nil
}

/*
|
|
|
|
|
|
|
|
SERVICE
|
|
*/
type ApiCallSuccess struct {
	Body       interface{}
	WorkConfig SymbolWorkConfig
}

func CreateApiSuccess(body interface{}, workConfig SymbolWorkConfig) *ApiCallSuccess {
	return &ApiCallSuccess{Body: body, WorkConfig: workConfig}
}

type ProcessETL[T any] interface {
	// First step return the bottle necked api call retricting to a single consumer
	// Reads from the queue which is a prerequisite
	// Logs response
	CallApi() (*ApiCallSuccess, error)
	// Transforms and updates
	Transform(apiCall *ApiCallSuccess) (*T, error)
}
