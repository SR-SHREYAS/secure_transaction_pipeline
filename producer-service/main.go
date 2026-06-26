package main

import (
	"fmt"
	"log"
	"net/http"

	"secure_transaction_pipeline/producer-service/api"
	producerapp "secure_transaction_pipeline/producer-service/app"
	"secure_transaction_pipeline/producer-service/messages"

	"github.com/joho/godotenv"
)

func main() {
	// Load environment variables from .env file
	if err := godotenv.Load("../.env"); err != nil {
		log.Fatalf("failed to load .env: %v", err)
	}

	kafkaClient, err := messages.NewKafkaClient()
	if err != nil {
		log.Fatalf("failed to create kafka client: %v", err)
	}
	defer kafkaClient.Close()

	producerApp := producerapp.NewApp(kafkaClient)
	producer := api.NewProducer(producerApp)
	api.RegisterRoutes(producer)

	fmt.Println("Producer running on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil)) // client-facing HTTP server
}
