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

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	var err error
	// Connect to Kafka using the franz-go client
	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"), // seed broker is used to discover the cluster
		kgo.ConsumerGroup("order-processors"),
		kgo.ConsumeTopics("orders"),
	)
	if err != nil {
		log.Fatalf("failed to create kafka client: %v", err)
	}
	defer client.Close()

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

	consumerApp := app.NewApp(client)
	consumer := api.NewConsumer(client, consumerApp)
	go consumer.ProcessOrders(ctx) // Start the consumer in a separate goroutine

	<-ctx.Done()

}
