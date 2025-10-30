package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/lib/pq"
	"github.com/pressly/goose/v3"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
	"github.com/coinbase/chainstorage/internal/utils/log"
)

func newDBMigrateCommand() *cobra.Command {
	var (
		masterUser     string
		masterPassword string
		host           string
		port           int
		sslMode        string
		connectTimeout time.Duration
		dryRun         bool
		dbName         string
	)

	cmd := &cobra.Command{
		Use:   "db-migrate",
		Short: "Run PostgreSQL schema migrations",
		Long: `Run PostgreSQL schema migrations using admin/master credentials.

This command MUST be run from the ChainStorage admin pod or with master credentials.
It applies any pending schema migrations to the database.

The migrations are tracked via the goose_db_version table, ensuring idempotency.
Running this command multiple times is safe - it will only apply new migrations.

Required flags:
- --master-user: PostgreSQL master/admin username
- --master-password: PostgreSQL master/admin password

Optional flags:
- --host: PostgreSQL host (default: from environment)
- --port: PostgreSQL port (default: 5432)
- --db-name: Database name (default: chainstorage_{blockchain}_{network})
- --ssl-mode: SSL mode (default: require)
- --dry-run: Show pending migrations without applying them

Example usage:
  # Run migrations for ethereum-mainnet (from admin pod)
  ./admin db-migrate --blockchain ethereum --network mainnet --env dev --master-user postgres --master-password <password>

  # Dry run to see pending migrations
  ./admin db-migrate --blockchain ethereum --network mainnet --env dev --master-user postgres --master-password <password> --dry-run

  # Use custom database name
  ./admin db-migrate --blockchain ethereum --network mainnet --env dev --master-user postgres --master-password <password> --db-name my_custom_db`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDBMigrate(masterUser, masterPassword, host, port, dbName, sslMode, connectTimeout, dryRun)
		},
	}

	cmd.Flags().StringVar(&masterUser, "master-user", "", "PostgreSQL master/admin username (required)")
	cmd.Flags().StringVar(&masterPassword, "master-password", "", "PostgreSQL master/admin password (required)")
	cmd.Flags().StringVar(&host, "host", "", "PostgreSQL host (uses environment if not specified)")
	cmd.Flags().IntVar(&port, "port", 5432, "PostgreSQL port")
	cmd.Flags().StringVar(&dbName, "db-name", "", "Database name (default: chainstorage_{blockchain}_{network})")
	cmd.Flags().StringVar(&sslMode, "ssl-mode", "require", "SSL mode (disable, require, verify-ca, verify-full)")
	cmd.Flags().DurationVar(&connectTimeout, "connect-timeout", 30*time.Second, "Connection timeout")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Show pending migrations without applying them")

	cmd.MarkFlagRequired("master-user")
	cmd.MarkFlagRequired("master-password")

	return cmd
}

func runDBMigrate(masterUser, masterPassword, host string, port int, dbName, sslMode string, connectTimeout time.Duration, dryRun bool) error {
	ctx := context.Background()
	logger := log.WithPackage(logger)

	// Determine host from environment if not provided
	if host == "" {
		host = getEnvOrDefault("CHAINSTORAGE_AWS_POSTGRES_HOST", "localhost")
	}

	// Determine database name if not provided
	if dbName == "" {
		dbName = fmt.Sprintf("chainstorage_%s_%s", commonFlags.blockchain, commonFlags.network)
		dbName = replaceHyphensWithUnderscores(dbName)
	}

	logger.Info("Starting database migration",
		zap.String("blockchain", commonFlags.blockchain),
		zap.String("network", commonFlags.network),
		zap.String("env", commonFlags.env),
		zap.String("host", host),
		zap.Int("port", port),
		zap.String("database", dbName),
		zap.Bool("dry_run", dryRun),
	)

	// Build connection config
	cfg := &config.PostgresConfig{
		Host:           host,
		Port:           port,
		Database:       dbName,
		User:           masterUser,
		Password:       masterPassword,
		SSLMode:        sslMode,
		ConnectTimeout: connectTimeout,
	}

	// Connect to database
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s connect_timeout=%d",
		cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.SSLMode, int(cfg.ConnectTimeout.Seconds()))

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return xerrors.Errorf("failed to open database connection: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return xerrors.Errorf("failed to ping database: %w", err)
	}

	logger.Info("Successfully connected to database")

	// Set up goose
	goose.SetBaseFS(postgres.GetEmbeddedMigrations())
	if err := goose.SetDialect("postgres"); err != nil {
		return xerrors.Errorf("failed to set goose dialect: %w", err)
	}

	if dryRun {
		// Show pending migrations
		logger.Info("Checking for pending migrations (dry run)")

		currentVersion, err := goose.GetDBVersion(db)
		if err != nil {
			return xerrors.Errorf("failed to get current database version: %w", err)
		}

		logger.Info("Current database version", zap.Int64("version", currentVersion))

		migrations, err := goose.CollectMigrations("db/migrations", 0, 9999999)
		if err != nil {
			return xerrors.Errorf("failed to collect migrations: %w", err)
		}

		fmt.Printf("\n📋 Migration Status:\n")
		fmt.Printf("Current version: %d\n\n", currentVersion)

		hasPending := false
		for _, migration := range migrations {
			if migration.Version > currentVersion {
				fmt.Printf("  [PENDING] %d: %s\n", migration.Version, migration.Source)
				hasPending = true
			} else {
				fmt.Printf("  [APPLIED] %d: %s\n", migration.Version, migration.Source)
			}
		}

		if !hasPending {
			fmt.Printf("\n✅ No pending migrations. Database is up to date!\n")
		} else {
			fmt.Printf("\n⚠️  Pending migrations found. Run without --dry-run to apply them.\n")
		}

		return nil
	}

	// Apply migrations
	logger.Info("Applying database migrations")
	if err := goose.UpContext(ctx, db, "db/migrations"); err != nil {
		return xerrors.Errorf("failed to run migrations: %w", err)
	}

	currentVersion, err := goose.GetDBVersion(db)
	if err != nil {
		return xerrors.Errorf("failed to get current database version: %w", err)
	}

	logger.Info("Migrations completed successfully", zap.Int64("current_version", currentVersion))
	fmt.Printf("\n✅ Database migrations completed successfully!\n")
	fmt.Printf("Current version: %d\n", currentVersion)

	return nil
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func init() {
	rootCmd.AddCommand(newDBMigrateCommand())
}
