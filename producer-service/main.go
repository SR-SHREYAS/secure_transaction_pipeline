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
	// Optionally load environment variables from a .env file in local/dev.
	// In containerized or production environments, it's fine if this file is absent.
	if err := godotenv.Load("../.env"); err != nil {
		log.Printf("no .env file loaded: %v (continuing with existing environment)", err)
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
