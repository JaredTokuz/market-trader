package token

import (
	"context"
	"io/ioutil"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type TokenProviderService interface {
	Fetch() (string, error)
}

type tokenHandler struct {
	Path		string
	Value		string
	Collection *mongo.Collection
	Expiration	time.Time
}

func NewTokenProviderService(collection *mongo.Collection, tokenPath string) TokenProviderService {
	var tokeDoc PartialTokeDoc
	result := collection.FindOne(context.TODO(), bson.M{})
	result.Decode(&tokeDoc)
	b, err:= fileToString(tokenPath)
	if err != nil {
		log.Fatal("failure loading token file ",err)
	}
	return &tokenHandler{
		Collection: collection,
		Expiration: tokeDoc.Until,
		Path: tokenPath,
		Value: b,
	}
}

type PartialTokeDoc struct {
	ID			primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	Until		time.Time					 `json:"until" bson:"until"`
}

func (a *tokenHandler) refreshToken() error {
	var tokeDoc PartialTokeDoc
	result := a.Collection.FindOne(context.TODO(), bson.M{})
	err := result.Decode(&tokeDoc)
	if err != nil {
		return err
	}
	a.Expiration = tokeDoc.Until
	b, err:= fileToString(a.Path)
	if err != nil {
		return err
	}
	a.Value = b
	return nil
}

func (a *tokenHandler) isTokenExpired() bool {
	return time.Now().After(a.Expiration)
}

func (a *tokenHandler) Fetch() (string, error) {
	if a.isTokenExpired() == true {
		err := a.refreshToken()
		if err != nil {
			return "", err
		}
	}
	return a.Value, nil
}


func fileToString(filePath string) (string, error) {
	b, err:= ioutil.ReadFile(filePath)
	if err != nil {
		return "",err
	}
	return string(b), nil
}