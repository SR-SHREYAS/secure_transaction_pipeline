package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"secure_transaction_pipeline/consumer-service/api"
	app "secure_transaction_pipeline/consumer-service/app"
	"secure_transaction_pipeline/consumer-service/messages"
	postgresstorage "secure_transaction_pipeline/consumer-service/storage/postgres"
	redisstorage "secure_transaction_pipeline/consumer-service/storage/redis"

	"github.com/joho/godotenv"
	"github.com/twmb/franz-go/pkg/kgo"
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

func loadBrokers() []string {
	raw := os.Getenv("KAFKA_BROKERS")
	if raw == "" {
		log.Fatalf("KAFKA_BROKERS is not set")
	}

	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		broker := strings.TrimSpace(part)
		if broker != "" {
			brokers = append(brokers, broker)
		}
	}

	if len(brokers) == 0 {
		log.Fatalf("KAFKA_BROKERS is empty")
	}

	return brokers
}

func main() {
	// In local/dev, load environment from service or project root.
	// In containerized/production environments, it's fine if no file is present.
	loadEnv()

	postgresStorage, err := postgresstorage.NewStorage()
	if err != nil {
		log.Fatalf("failed to create postgres storage: %v", err)
	}
	defer postgresStorage.Close()

	ctx, cancel := context.WithCancel(context.Background()) // context to handle graceful shutdown
	defer cancel()

	redisStorage, err := redisstorage.NewRedisStorage(ctx)
	if err != nil {
		log.Fatalf("failed to create redis storage: %v", err)
	}
	defer redisStorage.Close()

	dlqClient, err := kgo.NewClient(
		kgo.SeedBrokers(loadBrokers()...),
	)
	if err != nil {
		log.Fatalf("failed to create dlq kafka client: %v", err)
	}
	defer dlqClient.Close()

	kafkaClient, err := messages.NewKafkaClient()
	if err != nil {
		log.Fatalf("failed to create kafka client: %v", err)
	}
	defer kafkaClient.Close()

	sigCh := make(chan os.Signal, 1) // Create a channel to listen for OS signals (like SIGINT and SIGTERM) for graceful shutdown
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() { // Start a goroutine to listen for OS signals and cancel the context when a signal is received
		<-sigCh
		fmt.Println("Shutting down gracefully...")
		cancel()
	}()

	fmt.Println("Starting consumer, waiting for orders...")

	consumerApp := app.NewApp(kafkaClient, dlqClient, postgresStorage, redisStorage)
	consumer := api.NewConsumer(kafkaClient, consumerApp)
	go consumer.ProcessOrders(ctx) // Start the consumer in a separate goroutine

	<-ctx.Done()

}
