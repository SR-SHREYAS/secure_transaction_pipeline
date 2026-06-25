package api

import (
	"encoding/json"
	"net/http"
	"secure_transaction_pipeline/producer-service/app"
	"secure_transaction_pipeline/producer-service/models"
)

// Producer contains HTTP handlers and their dependencies.
type Producer struct {
	app app.App
}

// NewProducer wires the HTTP layer to the handler layer.
func NewProducer(app app.App) *Producer {
	return &Producer{app: app}
}

// healthCheck responds with a simple OK for liveness probes
func (h *Producer) healthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

// create order handles incoming order requests, validates them, and produces them to Kafka
func (h *Producer) createOrder(w http.ResponseWriter, r *http.Request) {
	//POST request only
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse the incoming JSON payload into an OrderRequest struct
	var orderReq models.OrderRequest
	if err := json.NewDecoder(r.Body).Decode(&orderReq); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	// Validate the order request
	if orderReq.Customer == "" || orderReq.Product == "" || orderReq.Quantity <= 0 || orderReq.Price <= 0 {
		http.Error(w, "Invalid order data", http.StatusBadRequest)
		return
	}

	// produce through the app layer so the handler only sees a publish error
	response_order, err := h.app.CreateOrder(r.Context(), orderReq)
	if err != nil {
		http.Error(w, "Failed to produce order to Kafka", http.StatusInternalServerError)
		return
	}

	// return the created order to client
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response_order)
}
