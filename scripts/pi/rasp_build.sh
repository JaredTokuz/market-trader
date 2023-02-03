#!/usr/bin/env bash

env GOOS=linux GOARCH=arm GOARM=7 go build -o ./dist/yearDaily ./cmd/yearDaily

env GOOS=linux GOARCH=arm GOARM=7 go build -o ./dist/day2Minute15 ./cmd/day2Minute15

env GOOS=linux GOARCH=arm GOARM=7 go build -o ./dist/day15Minute30 ./cmd/day15Minute30

env GOOS=linux GOARCH=arm GOARM=7 go build -o ./dist/minute15Signal ./cmd/minute15Signal