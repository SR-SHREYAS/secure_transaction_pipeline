FROM golang:1.26 AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY producer-service ./producer-service

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/producer ./producer-service

FROM gcr.io/distroless/static-debian12

WORKDIR /

COPY --from=builder /out/producer /producer

EXPOSE 8080

ENTRYPOINT ["/producer"]