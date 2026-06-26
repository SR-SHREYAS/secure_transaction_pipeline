package redis

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisStorage struct {
	redis_client *redis.Client
}

func NewRedisStorage() (*RedisStorage, error) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		return nil, fmt.Errorf("REDIS_ADDR is not set")
	}

	dbValue := os.Getenv("REDIS_DB")
	if dbValue == "" {
		return nil, fmt.Errorf("REDIS_DB is not set")
	}

	db, err := strconv.Atoi(dbValue)
	if err != nil {
		return nil, fmt.Errorf("invalid REDIS_DB: %w", err)
	}

	password := os.Getenv("REDIS_PASSWORD")

	redis_client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	if err := redis_client.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	return &RedisStorage{redis_client: redis_client}, nil
}

// Get returns a Redis value for the provided key.
func (r *RedisStorage) Get(ctx context.Context, key string) (string, error) {
	return r.redis_client.Get(ctx, key).Result()
}

// Set stores a Redis value with a TTL.
func (r *RedisStorage) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	return r.redis_client.Set(ctx, key, value, ttl).Err()
}

// Close releases the underlying redis client resources.
func (r *RedisStorage) Close() error {
	return r.redis_client.Close()
}
