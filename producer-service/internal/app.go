package internal

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

// App owns application-level dependencies and business operations.
type App struct {
	client *kgo.Client
}

// NewApp builds an App around the shared Kafka client.
func NewApp(client *kgo.Client) *App {
	return &App{client: client}
}

// CreateOrder sends a record to Kafka and returns any broker/client error.
func (a *App) CreateOrder(ctx context.Context, OrderReq OrderRequest) (Order, error) {
	response_order := Order{
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
		return Order{}, err // return an empty order and the error
	}

	// kafka record with order id as key
	// using the order ID as the key ensures that all events for one order land on the same partition, preserving order for that order's events
	record := &kgo.Record{
		Topic: "orders",
		Key:   []byte(response_order.ID),
		Value: orderBytes,
	}
	if err := a.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return Order{}, err // return an empty order and the error
	}
	return response_order, nil
}
