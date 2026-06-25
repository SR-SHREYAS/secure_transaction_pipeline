package api

import (
	"context"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"

	app "secure_transaction_pipeline/consumer-service/app"
)

// Consumer polls Kafka, groups messages into typed batches, and forwards them to the app layer.
type Consumer struct {
	client *kgo.Client
	app    app.App
}

// NewConsumer wires the Kafka client and app layer together.
func NewConsumer(client *kgo.Client, app app.App) *Consumer {
	return &Consumer{client: client, app: app}
}

// ConsumeOrders continuously fetches Kafka records and forwards raw batches to the app layer.
func (c *Consumer) ProcessOrders(ctx context.Context) {
	// consume loop: poll for new messages, decode them, and forward to the app layer
	for {
		fetches := c.client.PollFetches(ctx) // poll for new messages from Kafka , deliver records in batches
		if ctx.Err() != nil {
			return
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				log.Printf("fetch error: topic=%s partition=%d err=%v", e.Topic, e.Partition, e.Err)
			}
			continue
		}

		messages := make([][]byte, 0) // empty slice to hold the raw message payloads
		iter := fetches.RecordIter()  // iterate over all records in the fetches
		for !iter.Done() {
			record := iter.Next()
			messages = append(messages, record.Value)
		}

		if len(messages) == 0 {
			continue
		}

		if err := c.app.ProcessOrders(ctx, messages); err != nil {
			log.Printf("failed to forward orders to app layer: %v", err)
		}
	}
}
