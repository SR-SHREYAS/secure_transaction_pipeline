package app

import (
	"context"
	"encoding/json"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"

	models "secure_transaction_pipeline/consumer-service/models"
)

// App defines the consumer-side application contract.
type App interface {
	ProcessOrders(ctx context.Context, messages [][]byte) error
}

// AppImpl is the default application layer implementation.
type AppImpl struct {
	client *kgo.Client
}

// NewApp builds the application layer around the shared Kafka client.
func NewApp(client *kgo.Client) *AppImpl {
	return &AppImpl{client: client}
}

// ProcessOrders receives raw Kafka payloads from the API layer, decodes them, and handles them.
func (a *AppImpl) ProcessOrders(ctx context.Context, messages [][]byte) error {
	for _, message := range messages {
		var order models.Order
		if err := json.Unmarshal(message, &order); err != nil {
			log.Printf("failed to decode order: %v", err)
			continue
		}

		log.Printf("consumed order: ID=%s, customer=%s, product=%s, quantity=%d, price=%.2f, status=%s, created_at=%s",
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
