package messages

import (
	"fmt"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

func NewKafkaClient() (*kgo.Client, error) {
	brokers, err := getBrokers()
	if err != nil {
		return nil, err
	}

	groupID := os.Getenv("KAFKA_GROUP_ID")
	if groupID == "" {
		return nil, fmt.Errorf("KAFKA_GROUP_ID is not set")
	}

	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		return nil, fmt.Errorf("KAFKA_TOPIC is not set")
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	return client, nil
}

func getBrokers() ([]string, error) {
	raw := os.Getenv("KAFKA_BROKERS")
	if raw == "" {
		return nil, fmt.Errorf("KAFKA_BROKERS is not set")
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
		return nil, fmt.Errorf("KAFKA_BROKERS is empty")
	}

	return brokers, nil
}
