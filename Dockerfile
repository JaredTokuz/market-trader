# syntax=docker/dockerfile:1

FROM golang:1.18-alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

ADD angular-trader /app/angular-trader
ADD api /app/api
ADD pkg /app/pkg

RUN go build -o trader ./api

EXPOSE 3000

CMD [ "./trader" ]