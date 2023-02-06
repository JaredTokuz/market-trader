package etl

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type SymbolDoc struct {
	ID     primitive.ObjectID `json:"_id,omitempty"  bson:"_id,omitempty"`
	Symbol string             `json:"symbol"  bson:"symbol"`
}

type EtlConfig struct {
	ID     primitive.ObjectID `bson:"_id,omitempty"`
	Symbol string             `bson:"symbol"`
	Work   EtlJob             `bson:"work"`
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
