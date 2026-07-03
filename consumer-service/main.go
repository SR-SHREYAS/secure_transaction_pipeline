package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"secure_transaction_pipeline/consumer-service/api"
	app "secure_transaction_pipeline/consumer-service/app"
	"secure_transaction_pipeline/consumer-service/messages"
	postgresstorage "secure_transaction_pipeline/consumer-service/storage/postgres"
	redisstorage "secure_transaction_pipeline/consumer-service/storage/redis"

	"github.com/joho/godotenv"
)

func main() {
	// Optionally load environment variables from a .env file in local/dev.
	// In containerized or production environments, it's fine if this file is absent.
	if err := godotenv.Load("../.env"); err != nil {
		log.Printf("no .env file loaded: %v (continuing with existing environment)", err)
	}

	postgresStorage, err := postgresstorage.NewStorage()
	if err != nil {
		log.Fatalf("failed to create postgres storage: %v", err)
	}
	defer postgresStorage.Close()
	redisStorage, err := redisstorage.NewRedisStorage()
	if err != nil {
		log.Fatalf("failed to create redis storage: %v", err)
	}
	defer redisStorage.Close()

	kafkaClient, err := messages.NewKafkaClient()
	if err != nil {
		log.Fatalf("failed to create kafka client: %v", err)
	}
	defer kafkaClient.Close()

	ctx, cancel := context.WithCancel(context.Background()) // Create a context that can be canceled to handle graceful shutdown
	defer cancel()

	sigCh := make(chan os.Signal, 1) // Create a channel to listen for OS signals (like SIGINT and SIGTERM) for graceful shutdown
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() { // Start a goroutine to listen for OS signals and cancel the context when a signal is received
		<-sigCh
		fmt.Println("Shutting down gracefully...")
		cancel()
	}()

	fmt.Println("Starting consumer, waiting for orders...")

	consumerApp := app.NewApp(kafkaClient, postgresStorage, redisStorage)
	consumer := api.NewConsumer(kafkaClient, consumerApp)
	go consumer.ProcessOrders(ctx) // Start the consumer in a separate goroutine

	<-ctx.Done()

}
