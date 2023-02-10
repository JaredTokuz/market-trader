package etl

import (
	"github.com/go-playground/validator"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type SymbolDoc struct {
	ID     *primitive.ObjectID `json:"_id,omitempty"  bson:"_id,omitempty"`
	Symbol string              `json:"symbol"  bson:"symbol"`
}

func (s SymbolDoc) ForInsert() bson.M {
	return bson.M{"symbol": s.Symbol}
}

type EtlConfig struct {
	ID     *primitive.ObjectID `bson:"_id,omitempty"`
	Symbol string              `bson:"symbol"`
	Work   EtlJob              `bson:"work"`
}

type EtlJob string

// Mongo Collection names OR Task Names
const (
	Undefined EtlJob = "unknown"
	Macros           = "Macros"
	Medium           = "Medium"
	Short            = "Short"
	Signals          = "Signals"
)

// Other Mongo Collections
const (
	ApiQueue = "ApiQueue"
	APICalls = "APICalls"
	Logs     = "Logs"
)

type Config struct {
	// Key is the API Key
	ApiKey string `json:"key" validate:"required"`

	// TokenPath is the path to the token file
	TokenPath string `json:"path" validate:"required"`
}

func (a Config) Validate() error {
	validate := validator.New()
	return validate.Struct(a)
}
