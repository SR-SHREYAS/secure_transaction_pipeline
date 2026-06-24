package internal

import "net/http"

func RegisterRoutes(handler *Handler) {
	http.HandleFunc("/orders", handler.createOrder)
	http.HandleFunc("/health", handler.healthCheck)
}
