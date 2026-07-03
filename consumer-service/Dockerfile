FROM golang:1.26 AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY consumer-service ./consumer-service

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/consumer ./consumer-service

FROM gcr.io/distroless/static-debian12

WORKDIR /

COPY --from=builder /out/consumer /consumer

ENTRYPOINT ["/consumer"]