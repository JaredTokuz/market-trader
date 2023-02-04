package token

import (
	"encoding/json"
	"log"
	"os"
	"time"
)

type AccessTokenService interface {
	Fetch() (string, error)
}

type tokenHandler struct {
	Path		string
	Token		string
	Expiration	time.Time
}

type accessTokenPayload struct {
	headers		accessTokenHeader
	data		accessTokenData
}

type accessTokenHeader struct {
	Date	string	`json:"Date"`
}

type accessTokenData struct {
	RefreshToken	string	`json:"refresh_token"`
	ExpiresIn		int	`json:"expires_in"`
}

func NewAccessTokenService(file_path string) AccessTokenService {
	accessTokenPayload := getAccessToken(file_path)
	access_response_date, err := time.Parse(time.RFC1123, accessTokenPayload.headers.Date)
	if err != nil {
        log.Fatal("parsing access token header date", err.Error())
    }
	return &tokenHandler{
		Path: file_path,
		Token: accessTokenPayload.data.RefreshToken,
		Expiration: access_response_date.Add(time.Second * time.Duration(accessTokenPayload.data.ExpiresIn)),
	}
}

func getAccessToken(file_path string) accessTokenPayload {
	tokenFile, err := os.Open(file_path)
    if err != nil {
        log.Fatal("opening config file", err.Error())
    }
    jsonParser := json.NewDecoder(tokenFile)
	var accessTokenPayload = accessTokenPayload{}
    if err = jsonParser.Decode(&accessTokenPayload); err != nil {
        log.Fatal("parsing config file", err.Error())
    }
	return accessTokenPayload
}

func (a *tokenHandler) isTokenExpired() bool {
	return time.Now().After(a.Expiration)
}

func (a *tokenHandler) Fetch() (string, error) {
	if a.isTokenExpired() == true {
		accessTokenPayload := getAccessToken(a.Path)
		a.Token = accessTokenPayload.data.RefreshToken
	}
	return a.Token, nil
}
