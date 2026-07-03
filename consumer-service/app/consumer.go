package app

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	models "secure_transaction_pipeline/consumer-service/models"
	"secure_transaction_pipeline/consumer-service/storage/postgres"
	"secure_transaction_pipeline/consumer-service/storage/redis"
)

// App defines the consumer-side application contract.
type App interface {
	ProcessOrders(ctx context.Context, messages [][]byte) error
}

// AppImpl is the default application layer implementation.
type AppImpl struct {
	client        *kgo.Client
	postgresStore *postgres.PostgresStorage
	redisStore    *redis.RedisStorage
}

// NewApp builds the application layer around the shared Kafka client.
func NewApp(client *kgo.Client, postgres *postgres.PostgresStorage, redis *redis.RedisStorage) *AppImpl {
	return &AppImpl{client: client, postgresStore: postgres, redisStore: redis}
}

// ProcessOrders receives raw Kafka payloads from the API layer, decodes them, and handles them.
func (a *AppImpl) ProcessOrders(ctx context.Context, messages [][]byte) error {
	for _, message := range messages {
		var order models.Order
		if err := json.Unmarshal(message, &order); err != nil {
			log.Printf("failed to decode order: %v", err)
			continue
		}

		// check redis: has the order been processed before? if yes, skip it
		processed, found, err := a.redisStore.Get(ctx, "processed_order:"+order.ID)
		if err != nil {
			return fmt.Errorf("failed to check redis for processed order: %w", err)
		}
		// only skip if the key exists and is explicitly marked as processed
		if found && processed == "true" {
			log.Printf("order %s already processed, skipping", order.ID)
			continue
		}

		//mark order as confirmed in redis before persisting to postgres
		order.Status = "confirmed"

		//insert into postgres with ON CONFLICT as a safety net to avoid duplicates
		_, err = a.postgresStore.ExecContext(ctx, `
			INSERT INTO orders (id, customer, product, quantity, price, status, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (id) DO NOTHING`,
			order.ID,
			order.Customer,
			order.Product,
			order.Quantity,
			order.Price,
			order.Status,
			order.CreatedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to insert order into postgres: %w", err)
		}

		// mark this order ID as processed in redis with 24 hr TTL
		err = a.redisStore.Set(ctx, "processed_order:"+order.ID, "true", 24*time.Hour)
		if err != nil {
			return fmt.Errorf("failed to mark order as processed in redis: %w", err)
		}

		log.Printf("processed order: ID=%s, customer=%s, product=%s, quantity=%d, price=%.2f, status=%s, created_at=%s",
			order.ID,
			order.Customer,
			order.Product,
			order.Quantity,
			order.Price,
			order.Status,
			order.CreatedAt,
		)
	}

	return nil
}

// // getOrders receives decoded orders from the API layer and handles them.
// func (a *AppImpl) getOrders(ctx context.Context, orders []models.Order) error {
// 	for _, order := range orders {
// 		log.Printf("consumed order: ID=%s, customer=%s, product=%s, quantity=%d, price=%.2f, status=%s, created_at=%d",
// 			order.ID,
// 			order.Customer,
// 			order.Product,
// 			order.Quantity,
// 			order.Price,
// 			order.Status,
// 			order.CreatedAt,
// 		)
// 	}

// 	return nil
// }
