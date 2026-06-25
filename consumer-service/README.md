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
