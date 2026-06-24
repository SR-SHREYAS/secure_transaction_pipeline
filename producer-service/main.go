package main

import (
	"fmt"
	"log"
	"net/http"

	"secure_transaction_pipeline/producer-service/internal"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	var err error
	// Connect to Kafka using the franz-go client
	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"), // seed broker is used to discover the cluster
	)
	if err != nil {
		log.Fatalf("failed to create kafka client: %v", err)
	}
	defer client.Close()

	app := internal.NewApp(client)
	handler := internal.NewHandler(app)
	internal.RegisterRoutes(handler)

	fmt.Println("Producer running on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
