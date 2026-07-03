## consumer service 

the producer service creates order and publish it to kafka 
the consumer service would read these services from kafka and perform a fraud detection 

>> go build ./consumer
// to build a universal binary for the program

client.PollFetches(ctx) blocks until Kafka delivers one or more records. If the context is cancelled (from Ctrl+C), it returns immediately so you can exit the loop.

fetches.Errors() checks for any transport-level problems (network issues, broker failures). The loop logs them and moves on rather than crashing.

fetches.RecordIter() gives you an iterator over every record in the fetch batch. Each call to iter.Next() returns the next kgo.Record.

processRecord unmarshals the JSON value back into an Order struct and logs it. For now it only logs. In the next step, you will add database persistence and idempotency.

>> cd producer-service/go run main.go
after running producer server
>> cd consumer-service/go run main.go

push order in producer service with curl command , or use post man api documentation 

loggs of new incomming order would show up in consumer-service/go run terminal
logs for only newer messages/orders 
why ?
When your consumer processes a record, franz-go automatically commits the offset back to Kafka. On restart, Kafka tells the consumer to start reading from the next uncommitted offset. This is the core value of consumer groups: crash recovery without reprocessing.


  // postgres.go 
sql.Open("postgres", ...) creates a connection pool to PostgreSQL using the credentials from your Docker Compose stack. The connection string is built from environment variables such as POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, and POSTGRES_DB.

createTable() ensures the orders table exists before any records are processed.

// redis.go
redis.NewClient(&redis.Options{
    Addr:     os.Getenv("REDIS_ADDR"),
    Password: os.Getenv("REDIS_PASSWORD"),
    DB:       dbFromEnvOrDefault(),
}) connects to the Redis instance for fast key lookups, using environment variables like REDIS_ADDR and REDIS_DB instead of hardcoded localhost values.

// The `CREATE TABLE IF NOT EXISTS` statement on startup means the consumer is self-contained. You do not need a separate migration step or manual SQL. The id column is the primary key, which also serves as a safety net for duplicates at the database level.

// Redis gives you a fast in-memory lookup (microseconds) to catch duplicates before hitting the database. The ON CONFLICT DO NOTHING clause on the PostgreSQL INSERT acts as a safety net for edge cases where the Redis key expired (after 24 hours) but the order already exists in the database.

This two-layer approach is the standard production pattern: a fast cache for the hot path and a durable store as the final authority.