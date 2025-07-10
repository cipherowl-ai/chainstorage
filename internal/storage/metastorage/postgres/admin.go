package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/lib/pq"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
)

// SetupDatabase creates a database and roles for a new network in PostgreSQL
// This function is intended to be called by administrators during setup
func SetupDatabase(ctx context.Context, masterCfg *config.PostgresConfig, workerUser string, serverUser string, dbName string) error {
	log.Printf("Setting up PostgreSQL database: %s with roles: %s (worker), %s (server)", dbName, workerUser, serverUser)

	// Connect to the default 'postgres' database with master credentials
	dsn := fmt.Sprintf(
		"host=%s port=%d dbname=postgres user=%s password=%s sslmode=%s",
		masterCfg.Host, masterCfg.Port, masterCfg.User, masterCfg.Password, masterCfg.SSLMode,
	)
	if masterCfg.ConnectTimeout > 0 {
		dsn += fmt.Sprintf(" connect_timeout=%d", int(masterCfg.ConnectTimeout.Seconds()))
	}

	adminDB, err := sql.Open("postgres", dsn)
	if err != nil {
		return xerrors.Errorf("failed to connect to postgres db with master user: %w", err)
	}
	defer func() {
		if closeErr := adminDB.Close(); closeErr != nil {
			log.Printf("Warning: failed to close admin database connection: %v", closeErr)
		}
	}()

	if err := adminDB.PingContext(ctx); err != nil {
		return xerrors.Errorf("failed to ping postgres db with master user: %w", err)
	}

	log.Printf("Successfully connected to PostgreSQL as master user")

	// Create worker role with LOGIN capability
	log.Printf("Creating worker role: %s", workerUser)
	if _, err := adminDB.ExecContext(ctx, fmt.Sprintf("CREATE ROLE %s WITH LOGIN", pq.QuoteIdentifier(workerUser))); err != nil {
		if pgErr, ok := err.(*pq.Error); ok && pgErr.Code.Name() == "duplicate_object" {
			log.Printf("Worker role %s already exists, skipping creation", workerUser)
		} else {
			return xerrors.Errorf("failed to create worker role %s: %w", workerUser, err)
		}
	} else {
		log.Printf("Successfully created worker role: %s", workerUser)
	}

	// Create server role with LOGIN capability
	log.Printf("Creating server role: %s", serverUser)
	if _, err := adminDB.ExecContext(ctx, fmt.Sprintf("CREATE ROLE %s WITH LOGIN", pq.QuoteIdentifier(serverUser))); err != nil {
		if pgErr, ok := err.(*pq.Error); ok && pgErr.Code.Name() == "duplicate_object" {
			log.Printf("Server role %s already exists, skipping creation", serverUser)
		} else {
			return xerrors.Errorf("failed to create server role %s: %w", serverUser, err)
		}
	} else {
		log.Printf("Successfully created server role: %s", serverUser)
	}

	// Create application database owned by the worker role
	log.Printf("Creating database: %s", dbName)
	ownerOpt := fmt.Sprintf("OWNER = %s", pq.QuoteIdentifier(workerUser))
	if _, err := adminDB.ExecContext(ctx, fmt.Sprintf(`CREATE DATABASE %s WITH %s`, pq.QuoteIdentifier(dbName), ownerOpt)); err != nil {
		if pgErr, ok := err.(*pq.Error); ok && pgErr.Code.Name() == "duplicate_database" {
			log.Printf("Database %s already exists, skipping creation", dbName)
		} else {
			return xerrors.Errorf("failed to create database %s: %w", dbName, err)
		}
	} else {
		log.Printf("Successfully created database: %s", dbName)
	}

	// Connect to the application database to set up permissions
	log.Printf("Setting up permissions for database: %s", dbName)
	appDsn := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		masterCfg.Host, masterCfg.Port, dbName, masterCfg.User, masterCfg.Password, masterCfg.SSLMode,
	)
	if masterCfg.ConnectTimeout > 0 {
		appDsn += fmt.Sprintf(" connect_timeout=%d", int(masterCfg.ConnectTimeout.Seconds()))
	}

	appDB, err := sql.Open("postgres", appDsn)
	if err != nil {
		return xerrors.Errorf("failed to connect to app database %s: %w", dbName, err)
	}
	defer func() {
		if closeErr := appDB.Close(); closeErr != nil {
			log.Printf("Warning: failed to close app database connection: %v", closeErr)
		}
	}()

	if err := appDB.PingContext(ctx); err != nil {
		return xerrors.Errorf("failed to ping app database %s: %w", dbName, err)
	}

	// Grant connect permissions to server role
	log.Printf("Granting CONNECT permission on database %s to %s", dbName, serverUser)
	if _, err := appDB.ExecContext(ctx, fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s", pq.QuoteIdentifier(dbName), pq.QuoteIdentifier(serverUser))); err != nil {
		return xerrors.Errorf("failed to grant connect on db %s to role %s: %w", dbName, serverUser, err)
	}

	// Grant usage on public schema
	log.Printf("Granting USAGE on schema public to %s", serverUser)
	if _, err := appDB.ExecContext(ctx, fmt.Sprintf("GRANT USAGE ON SCHEMA public TO %s", pq.QuoteIdentifier(serverUser))); err != nil {
		return xerrors.Errorf("failed to grant usage on schema public to role %s: %w", serverUser, err)
	}

	// Grant SELECT on all current tables to server role
	log.Printf("Granting SELECT on all existing tables to %s", serverUser)
	if _, err := appDB.ExecContext(ctx, fmt.Sprintf("GRANT SELECT ON ALL TABLES IN SCHEMA public TO %s", pq.QuoteIdentifier(serverUser))); err != nil {
		// This might fail if no tables exist yet, which is fine
		log.Printf("Warning: failed to grant select on existing tables (this is normal if no tables exist yet): %v", err)
	}

	// Grant SELECT on all future tables to server role
	log.Printf("Setting up default privileges for future tables for %s", serverUser)
	if _, err := appDB.ExecContext(ctx, fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO %s", pq.QuoteIdentifier(serverUser))); err != nil {
		return xerrors.Errorf("failed to alter default privileges for role %s: %w", serverUser, err)
	}

	// Also need to ensure the worker role has the necessary permissions on the database
	log.Printf("Ensuring worker role %s has full access to database %s", workerUser, dbName)
	if _, err := appDB.ExecContext(ctx, fmt.Sprintf("GRANT ALL PRIVILEGES ON DATABASE %s TO %s", pq.QuoteIdentifier(dbName), pq.QuoteIdentifier(workerUser))); err != nil {
		log.Printf("Warning: failed to grant all privileges on database to worker role: %v", err)
	}

	if _, err := appDB.ExecContext(ctx, fmt.Sprintf("GRANT ALL PRIVILEGES ON SCHEMA public TO %s", pq.QuoteIdentifier(workerUser))); err != nil {
		log.Printf("Warning: failed to grant all privileges on schema to worker role: %v", err)
	}

	log.Printf("✅ Successfully set up database %s with roles %s (worker) and %s (server)", dbName, workerUser, serverUser)
	log.Printf("📋 Next steps:")
	log.Printf("   1. Set passwords for the roles: ALTER ROLE %s PASSWORD 'your_password';", workerUser)
	log.Printf("   2. Set passwords for the roles: ALTER ROLE %s PASSWORD 'your_password';", serverUser)
	log.Printf("   3. Update your application configuration to use these credentials")

	return nil
}
