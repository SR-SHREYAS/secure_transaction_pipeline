package api

import "net/http"

func RegisterRoutes(producer *Producer) {
	http.HandleFunc("/orders", producer.createOrder)
	http.HandleFunc("/health", producer.healthCheck)
}
