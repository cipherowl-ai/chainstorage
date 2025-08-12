package postgres

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/log"
)

func newDBConnection(ctx context.Context, cfg *config.PostgresConfig) (*sql.DB, error) {
	logger := log.WithPackage(log.NewDevelopment())

	// Build PostgreSQL connection string with timeout
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.SSLMode)

	// Add connect_timeout if specified
	if cfg.ConnectTimeout > 0 {
		dsn += fmt.Sprintf(" connect_timeout=%d", int(cfg.ConnectTimeout.Seconds()))
	}

	// Debug output for CI troubleshooting
	logger.Debug("Connecting to PostgreSQL",
		zap.String("host", cfg.Host),
		zap.Int("port", cfg.Port),
		zap.String("database", cfg.Database),
		zap.String("ssl_mode", cfg.SSLMode))

	// Open database connection
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		logger.Error("Failed to open connection", zap.Error(err))
		return nil, err
	}

	if pingErr := db.PingContext(ctx); pingErr != nil {
		logger.Error("Failed to ping database", zap.Error(pingErr))
		return nil, pingErr
	}

	logger.Debug("Successfully connected to PostgreSQL")

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

	// Check if tables exist, if not run migrations
	tablesExist, err := checkTablesExist(ctx, db)
	if err != nil {
		return nil, xerrors.Errorf("failed to check if tables exist: %w", err)
	}

	if !tablesExist {
		logger.Debug("Tables don't exist, running migrations")
		if err := runMigrations(ctx, db); err != nil {
			return nil, xerrors.Errorf("failed to run migrations: %w", err)
		}
		logger.Debug("Migrations completed successfully")
	} else {
		logger.Debug("Tables already exist, skipping migrations")
	}

	return db, nil
}

// checkTablesExist checks if the core PostgreSQL tables exist
// Returns true if the main tables exist, false otherwise
func checkTablesExist(ctx context.Context, db *sql.DB) (bool, error) {
	// Check for the existence of block_metadata table as it's the primary table
	query := `
		SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_schema = 'public' 
			AND table_name = 'block_metadata'
		)`

	var exists bool
	err := db.QueryRowContext(ctx, query).Scan(&exists)
	if err != nil {
		return false, xerrors.Errorf("failed to check table existence: %w", err)
	}

	return exists, nil
}
