package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
)

func newDBConnection(ctx context.Context, cfg *config.PostgresConfig) (*sql.DB, error) {
	// Build PostgreSQL connection string with timeout
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.SSLMode)

	// Add connect_timeout if specified
	if cfg.ConnectTimeout > 0 {
		dsn += fmt.Sprintf(" connect_timeout=%d", int(cfg.ConnectTimeout.Seconds()))
	}

	// Debug output for CI troubleshooting
	log.Printf("DEBUG: Connecting to PostgreSQL with DSN: host=%s port=%d dbname=%s sslmode=%s", cfg.Host, cfg.Port, cfg.Database, cfg.SSLMode)

	// Open database connection
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Printf("DEBUG: Failed to open connection: %v", err)
		return nil, err
	}

	if pingErr := db.PingContext(ctx); pingErr != nil {
		log.Printf("DEBUG: Failed to ping database: %v", pingErr)
		return nil, pingErr
	}

	log.Printf("DEBUG: Successfully connected to PostgreSQL")

	// Configure connection pool and timeouts
	db.SetMaxOpenConns(cfg.MaxConnections)
	db.SetMaxIdleConns(cfg.MinConnections)
	db.SetConnMaxLifetime(cfg.MaxLifetime)
	db.SetConnMaxIdleTime(cfg.MaxIdleTime)

	// Set statement timeout if specified
	if cfg.StatementTimeout > 0 {
		_, err := db.ExecContext(ctx, fmt.Sprintf("SET statement_timeout = '%dms'", cfg.StatementTimeout.Milliseconds()))
		if err != nil {
			return nil, xerrors.Errorf("failed to set statement timeout: %w", err)
		}
	}

	// Run database migrations
	if err := runMigrations(ctx, db); err != nil {
		return nil, xerrors.Errorf("failed to run migrations: %w", err)
	}
	return db, nil
}
