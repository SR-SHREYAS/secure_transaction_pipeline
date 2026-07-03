## secure_transaction_pipeline 

Every time you tap your card or hit "pay" online, a chain of backend systems evaluates that transaction in milliseconds. These systems communicate by sending events to each other, scoring each payment for fraud before it clears.

In this project, we will build a secure transaction pipeline with Go and Apache Kafka. Two microservices communicate through events to ingest, score, and store payment transactions with fraud detection.

## Environment setup

Use a single root `.env` file in the repository root. Do not create separate `.env` files inside `producer-service` or `consumer-service`.

For local Docker Compose runs, the root `.env` should contain:

```env
POSTGRES_PASSWORD=postgres
```

Docker Compose injects this value into the Postgres, Producer, and Consumer containers. The Go services read configuration only through `os.Getenv(...)`.

### Local development

If you run the services directly with `go run`, export the required variables in your shell first. The application does not load `.env` files itself.

Producer:

```bash
export KAFKA_BROKERS=localhost:9092
cd producer-service
go run main.go
```

Consumer:

```bash
export KAFKA_BROKERS=localhost:9092
export KAFKA_GROUP_ID=order-processors
export KAFKA_TOPIC=orders
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_USER=orders
export POSTGRES_PASSWORD=postgres
export POSTGRES_DB=orders
export REDIS_ADDR=localhost:6379
export REDIS_DB=0
cd consumer-service
go run main.go
```

### Docker Compose

Run the full stack with:

```bash
docker compose up --build
```

