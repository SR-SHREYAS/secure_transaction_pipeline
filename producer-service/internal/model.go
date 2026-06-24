package internal

import "time"

// Order represents a confirmed order event flowing through Kafka
type Order struct {
	ID        string    `json:"id"`
	Customer  string    `json:"customer"`
	Product   string    `json:"product"`
	Quantity  int       `json:"quantity"`
	Price     float64   `json:"price"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// OrderRequest is the incoming payload from the HTTP client
type OrderRequest struct {
	Customer string  `json:"customer"`
	Product  string  `json:"product"`
	Quantity int     `json:"quantity"`
	Price    float64 `json:"price"`
}
