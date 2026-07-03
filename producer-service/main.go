package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"

	"secure_transaction_pipeline/producer-service/api"
	producerapp "secure_transaction_pipeline/producer-service/app"
	"secure_transaction_pipeline/producer-service/messages"

	"github.com/joho/godotenv"
)

func loadEnv() {
	paths := []string{".env", "../.env"}

	for _, path := range paths {
		if err := godotenv.Load(path); err == nil {
			log.Printf("loaded environment from %s", path)
			return
		} else if errors.Is(err, os.ErrNotExist) {
			continue
		}
	}

	log.Printf("no .env file loaded from %v (continuing with existing environment)", paths)
}

func main() {
	// In local/dev, load environment from service or project root.
	// In containerized/production environments, it's fine if no file is present.
	loadEnv()

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
