FROM golang:1.22.3-bookworm

WORKDIR /app/
COPY ./go.mod /app/go.mod
COPY ./go.sum /app/go.sum
COPY ./main.go /app/main.go

RUN go build

CMD ./es-geoip