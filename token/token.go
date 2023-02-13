package token

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"time"
)

type AccessTokenService interface {
	Fetch() string
}

type tokenHandler struct {
	Path       string
	Token      string
	Expiration time.Time
}

func NewAccessTokenService(file_path string) AccessTokenService {
	accessTokenPayload := getAccessToken(file_path)
	access_response_date, err := time.Parse(time.RFC1123, accessTokenPayload.Headers.Date)
	if err != nil {
		log.Fatal("Failure to parse access token header date", err.Error())
	}
	return &tokenHandler{
		Path:       file_path,
		Token:      accessTokenPayload.Data.AccessToken,
		Expiration: access_response_date.Add(time.Second * time.Duration(accessTokenPayload.Data.ExpiresIn)),
	}
}

func (a *tokenHandler) Fetch() string {
	if a.isTokenExpired() == true {
		accessTokenPayload := getAccessToken(a.Path)
		a.Token = accessTokenPayload.Data.AccessToken
	}
	return a.Token
}

func (a *tokenHandler) isTokenExpired() bool {
	return time.Now().After(a.Expiration)
}

type accessTokenPayload struct {
	Headers accessTokenHeader
	Data    accessTokenData
}

type accessTokenHeader struct {
	Date string `json:"Date"`
}

type accessTokenData struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
}

func getAccessToken(file_path string) accessTokenPayload {
	tokenFile, err := ioutil.ReadFile(file_path)
	if err != nil {
		log.Fatal("opening config file", err.Error())
	}
	accessTokenPayload := accessTokenPayload{}
	if err = json.Unmarshal(tokenFile, &accessTokenPayload); err != nil {
		log.Fatal("parsing config file", err.Error())
	}
	return accessTokenPayload
}
