package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
)

func createDatabase(ctx context.Context, cfg *config.PostgresConfig) error {
	// Build connection string with timeout
	dsn := fmt.Sprintf(
		"host=%s port=%d dbname=postgres user=%s password=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.SSLMode,
	)

	// Add connect_timeout if specified
	if cfg.ConnectTimeout > 0 {
		dsn += fmt.Sprintf(" connect_timeout=%d", int(cfg.ConnectTimeout.Seconds()))
	}

	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := admin.Close(); closeErr != nil {
			// Log the close error but don't override the original error
			_ = closeErr
		}
	}()

	_, err = admin.ExecContext(ctx, fmt.Sprintf(
		`CREATE DATABASE "%s" OWNER "%s"`, cfg.Database, cfg.User))
	// ignore "already exists" race
	if pgErr, ok := err.(*pq.Error); ok && pgErr.Code == "42P04" {
		return nil
	}
	return err
}

func newDBConnection(ctx context.Context, cfg *config.PostgresConfig) (*sql.DB, error) {
	// Build PostgreSQL connection string with timeout
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.SSLMode)

	// Add connect_timeout if specified
	if cfg.ConnectTimeout > 0 {
		dsn += fmt.Sprintf(" connect_timeout=%d", int(cfg.ConnectTimeout.Seconds()))
	}

	// Open database connection
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	if pingErr := db.PingContext(ctx); pingErr != nil {
		// Detect "database does not exist"
		if pgErr, ok := pingErr.(*pq.Error); ok && pgErr.Code == "3D000" {
			if err := createDatabase(ctx, cfg); err != nil {
				return nil, xerrors.Errorf("creating database: %w", err)
			}
			// retry
			if pingErr = db.PingContext(ctx); pingErr == nil {
				goto CONFIGURE
			}
		}
		return nil, pingErr
	}

	// Configure connection pool and timeouts
CONFIGURE:
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
