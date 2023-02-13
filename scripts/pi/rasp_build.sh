#!/usr/bin/env bash

env GOOS=linux GOARCH=arm GOARM=7 go build -o ./dist/macros ./cmd/assign/macros

env GOOS=linux GOARCH=arm GOARM=7 go build -o ./dist/medium ./cmd/assign/medium

env GOOS=linux GOARCH=arm GOARM=7 go build -o ./dist/short ./cmd/assign/short

env GOOS=linux GOARCH=arm GOARM=7 go build -o ./dist/signals ./cmd/assign/signals

env GOOS=linux GOARCH=arm GOARM=7 go build -o ./dist/worker ./cmd/worker