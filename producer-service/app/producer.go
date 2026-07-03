package app

import (
	"context"
	"encoding/json"
	"fmt"
	"secure_transaction_pipeline/producer-service/models"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

// App interface defines the application layer contract
type App interface {
	CreateOrder(ctx context.Context, req models.OrderRequest) (*models.Order, error)
}

// App owns application-level dependencies and business operations.
type AppImpl struct {
	client *kgo.Client
}

// NewApp builds an App around the shared Kafka client.
func NewApp(client *kgo.Client) *AppImpl {
	return &AppImpl{client: client}
}

// CreateOrder sends a record to Kafka and returns any broker/client error.
func (a *AppImpl) CreateOrder(ctx context.Context, OrderReq models.OrderRequest) (*models.Order, error) {
	response_order := models.Order{
		ID:        uuid.New().String(),
		Customer:  OrderReq.Customer,
		Product:   OrderReq.Product,
		Quantity:  OrderReq.Quantity,
		Price:     OrderReq.Price,
		Status:    "Placed",
		CreatedAt: time.Now(),
	}

	// serialize the order to JSON for sending to Kafka
	orderBytes, err := json.Marshal(response_order)
	if err != nil {
		return &models.Order{}, err // return an empty order and the error
	}

	// kafka record with order id as key
	// using the order ID as the key ensures that all events for one order land on the same partition, preserving order for that order's events
	record := &kgo.Record{
		Topic: "orders",
		Key:   []byte(response_order.ID),
		Value: orderBytes,
	}
	publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := a.client.ProduceSync(publishCtx, record).FirstErr(); err != nil {
		return &models.Order{}, fmt.Errorf("failed to produce order to kafka: %w", err)
	}
	return &response_order, nil
}
