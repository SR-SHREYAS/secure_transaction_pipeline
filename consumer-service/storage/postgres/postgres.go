package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	_ "github.com/lib/pq"
)

type PostgresStorage struct {
	db *sql.DB
}

func NewStorage() (*PostgresStorage, error) {
	host := os.Getenv("POSTGRES_HOST")
	if host == "" {
		return nil, fmt.Errorf("POSTGRES_HOST is not set")
	}

	port := os.Getenv("POSTGRES_PORT")
	if port == "" {
		return nil, fmt.Errorf("POSTGRES_PORT is not set")
	}

	user := os.Getenv("POSTGRES_USER")
	if user == "" {
		return nil, fmt.Errorf("POSTGRES_USER is not set")
	}

	password := os.Getenv("POSTGRES_PASSWORD")
	if password == "" {
		return nil, fmt.Errorf("POSTGRES_PASSWORD is not set")
	}

	dbName := os.Getenv("POSTGRES_DB")
	if dbName == "" {
		return nil, fmt.Errorf("POSTGRES_DB is not set")
	}

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbName)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres database: %w", err)
	}

	if err := db.PingContext(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to connect to postgres database: %w", err)
	}

	storage := &PostgresStorage{db: db}

	// create a table for orders if it doesn't exist
	if err := storage.createTable(); err != nil {
		return nil, fmt.Errorf("failed to create orders table: %w", err)
	}
	return storage, nil
}

func (p *PostgresStorage) createTable() error {
	_, err := p.db.Exec(`
		CREATE TABLE IF NOT EXISTS orders (
			id VARCHAR(36) PRIMARY KEY,
			customer VARCHAR(255) NOT NULL,
			product VARCHAR(255) NOT NULL,
			quantity INTEGER NOT NULL,
			price DECIMAL(10,2) NOT NULL,
			status VARCHAR(50) NOT NULL,
			created_at TIMESTAMP NOT NULL
		)
	`)
	return err
}

// ExecContext runs a SQL statement against the storage database.
func (p *PostgresStorage) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return p.db.ExecContext(ctx, query, args...)
}

// Close releases the underlying database resources.
func (p *PostgresStorage) Close() error {
	return p.db.Close()
}
