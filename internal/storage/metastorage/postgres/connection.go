package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"golang.org/x/xerrors"
)

func createDatabase(ctx context.Context, cfg *config.PostgresConfig) error {
	adminDSN := fmt.Sprintf(
		"host=%s port=%d dbname=postgres user=%s password=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.SSLMode,
	)
	admin, err := sql.Open("postgres", adminDSN)
	if err != nil {
		return err
	}
	defer admin.Close()

	_, err = admin.ExecContext(ctx, fmt.Sprintf(
		`CREATE DATABASE "%s" OWNER "%s"`, cfg.Database, cfg.User))
	// ignore “already exists” race
	if pgErr, ok := err.(*pq.Error); ok && pgErr.Code == "42P04" {
		return nil
	}
	return err
}

func newDBConnection(ctx context.Context, cfg *config.PostgresConfig) (*sql.DB, error) {
	// Build PostgreSQL connection string
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.SSLMode)

	// Open database connection
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	if pingErr := db.PingContext(ctx); pingErr != nil {
		// Detect “database does not exist”
		if pgErr, ok := pingErr.(*pq.Error); ok && pgErr.Code == "3D000" {
			if err := createDatabase(ctx, cfg); err != nil {
				return nil, xerrors.Errorf("creating database: %w", err)
			}
			// retry
			if pingErr = db.PingContext(ctx); pingErr == nil {
				goto MIGRATE
			}
		}
		return nil, pingErr
	}

	// Configure connection pool
	db.SetMaxOpenConns(cfg.MaxConnections)
	db.SetMaxIdleConns(cfg.MinConnections)
	db.SetConnMaxLifetime(cfg.MaxLifetime)
	db.SetConnMaxIdleTime(cfg.MaxIdleTime)

	// Run database migrations
MIGRATE:
	if err := runMigrations(ctx, db); err != nil {
		return nil, xerrors.Errorf("failed to run migrations: %w", err)
	}
	return db, nil
}
