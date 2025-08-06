package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/pressly/goose/v3"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/utils/log"
)

// DBInitSecret is no longer needed - we parse flat JSON directly

func newDBInitCommand() *cobra.Command {
	var (
		awsRegion string
		dryRun    bool
	)

	cmd := &cobra.Command{
		Use:   "db-init",
		Short: "Initialize database and users for a specific network from AWS Secrets Manager",
		Long: `Initialize PostgreSQL database and users for a specific blockchain network based on configuration stored in AWS Secrets Manager.

This command:
1. Uses master credentials from environment variables (injected by Kubernetes)
2. Fetches network-specific credentials from AWS Secrets Manager
3. Creates a database for the specified network
4. Creates network-specific server (read-only) and worker (read-write) users
5. Sets up proper permissions for each role
6. Is idempotent - can be run multiple times safely

The command must be run from within the Kubernetes cluster (e.g., admin pod) 
with proper IAM role attached to access the secret.

Example usage:
  # Initialize database for ethereum-mainnet
  chainstorage admin db-init --blockchain ethereum --network mainnet --env dev

  # Dry run to preview changes
  chainstorage admin db-init --blockchain ethereum --network mainnet --env dev --dry-run

  # Use specific AWS region
  chainstorage admin db-init --blockchain ethereum --network mainnet --env prod --aws-region us-west-2`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Use commonFlags from common.go for blockchain, network, and env
			return runDBInit(commonFlags.blockchain, commonFlags.network, commonFlags.env, awsRegion, dryRun)
		},
	}

	cmd.Flags().StringVar(&awsRegion, "aws-region", "us-east-1", "AWS region")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Preview changes without applying them")

	return cmd
}

func runDBInit(blockchain, network, env, awsRegion string, dryRun bool) error {
	ctx := context.Background()
	logger := log.WithPackage(logger)

	logger.Info("Starting database initialization",
		zap.String("blockchain", blockchain),
		zap.String("network", network),
		zap.String("environment", env),
		zap.String("region", awsRegion),
		zap.Bool("dry_run", dryRun))

	// Get master credentials from environment variables
	masterHost := os.Getenv("CHAINSTORAGE_CLUSTER_ENDPOINT")
	masterPortStr := os.Getenv("CHAINSTORAGE_CLUSTER_PORT")
	masterUser := os.Getenv("CHAINSTORAGE_MASTER_USERNAME")
	masterPassword := os.Getenv("CHAINSTORAGE_MASTER_PASSWORD")

	if masterHost == "" || masterPortStr == "" || masterUser == "" || masterPassword == "" {
		return xerrors.New("missing required environment variables: CHAINSTORAGE_CLUSTER_ENDPOINT, CHAINSTORAGE_CLUSTER_PORT, CHAINSTORAGE_MASTER_USERNAME, CHAINSTORAGE_MASTER_PASSWORD")
	}

	var masterPort int
	if _, err := fmt.Sscanf(masterPortStr, "%d", &masterPort); err != nil {
		return xerrors.Errorf("invalid port number: %s", masterPortStr)
	}

	// Construct secret name
	secretName := fmt.Sprintf("chainstorage/db-creds/%s", env)

	// Fetch secret from AWS Secrets Manager
	secretData, err := fetchSecret(ctx, secretName, awsRegion)
	if err != nil {
		return xerrors.Errorf("failed to fetch secret: %w", err)
	}

	// Construct lookup keys for this network
	// Replace hyphens with underscores for the key lookup
	networkKey := strings.ReplaceAll(fmt.Sprintf("%s_%s", blockchain, network), "-", "_")

	// Extract network-specific values from flat secret
	dbName, err := getStringFromSecret(secretData, fmt.Sprintf("%s_database_name", networkKey))
	if err != nil {
		return xerrors.Errorf("failed to get database name: %w", err)
	}

	serverUsername, err := getStringFromSecret(secretData, fmt.Sprintf("%s_server_username", networkKey))
	if err != nil {
		return xerrors.Errorf("failed to get server username: %w", err)
	}

	serverPassword, err := getStringFromSecret(secretData, fmt.Sprintf("%s_server_password", networkKey))
	if err != nil {
		return xerrors.Errorf("failed to get server password: %w", err)
	}

	workerUsername, err := getStringFromSecret(secretData, fmt.Sprintf("%s_worker_username", networkKey))
	if err != nil {
		return xerrors.Errorf("failed to get worker username: %w", err)
	}

	workerPassword, err := getStringFromSecret(secretData, fmt.Sprintf("%s_worker_password", networkKey))
	if err != nil {
		return xerrors.Errorf("failed to get worker password: %w", err)
	}

	logger.Info("Successfully fetched credentials from AWS Secrets Manager",
		zap.String("database", dbName),
		zap.String("server_user", serverUsername),
		zap.String("worker_user", workerUsername))

	if dryRun {
		logger.Info("DRY RUN MODE - No changes will be made",
			zap.String("database", dbName),
			zap.String("server_user", serverUsername),
			zap.String("worker_user", workerUsername))
		return nil
	}

	// Connect to PostgreSQL as master user
	masterDSN := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=require",
		masterHost, masterPort, masterUser, masterPassword)

	db, err := sql.Open("postgres", masterDSN)
	if err != nil {
		return xerrors.Errorf("failed to connect to database: %w", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			logger.Warn("Failed to close database connection", zap.Error(closeErr))
		}
	}()

	// Set connection pool settings
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(30 * time.Second)

	// Test connection
	if err := db.PingContext(ctx); err != nil {
		return xerrors.Errorf("failed to ping database: %w", err)
	}

	logger.Info("Successfully connected to PostgreSQL cluster")

	// Create users
	logger.Info("Creating users")

	if err := createUser(db, workerUsername, workerPassword, true, logger); err != nil {
		return xerrors.Errorf("failed to create worker user %s: %w", workerUsername, err)
	}
	logger.Info("Created/verified worker user", zap.String("username", workerUsername))

	if err := createUser(db, serverUsername, serverPassword, false, logger); err != nil {
		return xerrors.Errorf("failed to create server user %s: %w", serverUsername, err)
	}
	logger.Info("Created/verified server user", zap.String("username", serverUsername))

	// Create database
	logger.Info("Creating database")
	if err := createDatabase(db, dbName, workerUsername, logger); err != nil {
		return xerrors.Errorf("failed to create database %s: %w", dbName, err)
	}
	logger.Info("Created/verified database", zap.String("database", dbName))

	// Run migrations on the network database
	logger.Info("Running migrations")
	if err := runMigrations(ctx, masterHost, masterPort, masterUser, masterPassword, dbName, logger); err != nil {
		return xerrors.Errorf("failed to run migrations: %w", err)
	}
	logger.Info("Migrations completed successfully")

	// Grant permissions
	logger.Info("Setting up permissions")

	// Grant CONNECT permission to server user
	if err := grantConnectPermission(db, dbName, serverUsername, logger); err != nil {
		return xerrors.Errorf("failed to grant CONNECT permission on %s to %s: %w",
			dbName, serverUsername, err)
	}

	// Connect to the network database to grant permissions
	netDSN := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=require",
		masterHost, masterPort, masterUser, masterPassword, dbName)

	netDB, err := sql.Open("postgres", netDSN)
	if err != nil {
		return xerrors.Errorf("failed to connect to database %s: %w", dbName, err)
	}
	defer func() {
		if closeErr := netDB.Close(); closeErr != nil {
			logger.Warn("Failed to close network database connection", zap.Error(closeErr))
		}
	}()

	// Grant full access to worker user (owner)
	if err := grantFullAccess(netDB, workerUsername, logger); err != nil {
		return xerrors.Errorf("failed to grant full access on %s to %s: %w", dbName, workerUsername, err)
	}
	logger.Info("Granted full access to worker user", zap.String("username", workerUsername))

	// Grant permissions to server user
	if err := grantReadOnlyAccess(netDB, serverUsername, workerUsername, logger); err != nil {
		return xerrors.Errorf("failed to grant permissions on %s: %w", dbName, err)
	}
	logger.Info("Granted read-only access to server user", zap.String("username", serverUsername))

	logger.Info("Database initialization completed successfully",
		zap.String("network", fmt.Sprintf("%s-%s", blockchain, network)),
		zap.String("database", dbName),
		zap.String("worker_user", workerUsername),
		zap.String("server_user", serverUsername))

	logger.Info("Next steps",
		zap.String("step1", "Deploy chainstorage worker pods to start data ingestion"),
		zap.String("step2", "Deploy chainstorage server pods for API access"),
		zap.String("step3", "Monitor logs for successful connections"))

	return nil
}

func fetchSecret(ctx context.Context, secretName, region string) (map[string]interface{}, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, xerrors.Errorf("failed to load AWS config: %w", err)
	}

	client := secretsmanager.NewFromConfig(cfg)

	input := &secretsmanager.GetSecretValueInput{
		SecretId: &secretName,
	}

	result, err := client.GetSecretValue(ctx, input)
	if err != nil {
		return nil, xerrors.Errorf("failed to get secret value: %w", err)
	}

	// Parse the flat JSON secret
	var secretData map[string]interface{}
	if err := json.Unmarshal([]byte(*result.SecretString), &secretData); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal secret: %w", err)
	}

	return secretData, nil
}

func getStringFromSecret(secret map[string]interface{}, key string) (string, error) {
	value, ok := secret[key]
	if !ok {
		return "", xerrors.Errorf("key %s not found in secret", key)
	}

	strValue, ok := value.(string)
	if !ok {
		return "", xerrors.Errorf("value for key %s is not a string", key)
	}

	return strValue, nil
}

func createUser(db *sql.DB, username, password string, canCreateDB bool, logger *zap.Logger) error {
	// Check if user exists
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM pg_user WHERE usename = $1)`
	if err := db.QueryRow(query, username).Scan(&exists); err != nil {
		return xerrors.Errorf("failed to check if user exists: %w", err)
	}

	if exists {
		logger.Info("User already exists, updating password", zap.String("username", username))
		// Update password for existing user
		alterQuery := fmt.Sprintf("ALTER USER %s WITH PASSWORD %s",
			pq.QuoteIdentifier(username), pq.QuoteLiteral(password))
		if _, err := db.Exec(alterQuery); err != nil {
			return xerrors.Errorf("failed to update user password: %w", err)
		}
		return nil
	}

	// Create user with proper quoting
	createQuery := fmt.Sprintf("CREATE USER %s WITH LOGIN PASSWORD %s",
		pq.QuoteIdentifier(username), pq.QuoteLiteral(password))

	if canCreateDB {
		createQuery += " CREATEDB"
	}

	if _, err := db.Exec(createQuery); err != nil {
		return xerrors.Errorf("failed to create user: %w", err)
	}

	return nil
}

func createDatabase(db *sql.DB, dbName, owner string, logger *zap.Logger) error {
	// Check if database exists
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)`
	if err := db.QueryRow(query, dbName).Scan(&exists); err != nil {
		return xerrors.Errorf("failed to check if database exists: %w", err)
	}

	if exists {
		logger.Info("Database already exists, checking ownership", zap.String("database", dbName))
		// Check current owner
		var currentOwner string
		ownerQuery := `SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = $1`
		if err := db.QueryRow(ownerQuery, dbName).Scan(&currentOwner); err != nil {
			return xerrors.Errorf("failed to get database owner: %w", err)
		}

		if currentOwner == owner {
			logger.Info("Database already owned by expected owner", zap.String("database", dbName), zap.String("owner", owner))
			return nil
		} else {
			logger.Info("Transferring database ownership", zap.String("database", dbName), zap.String("current_owner", currentOwner), zap.String("new_owner", owner))
			// Transfer ownership
			alterQuery := fmt.Sprintf("ALTER DATABASE %s OWNER TO %s",
				pq.QuoteIdentifier(dbName), pq.QuoteIdentifier(owner))
			if _, err := db.Exec(alterQuery); err != nil {
				return xerrors.Errorf("failed to transfer database ownership: %w", err)
			}
			return nil
		}
	}

	// Create database with master user as owner first, then transfer ownership
	createQuery := fmt.Sprintf("CREATE DATABASE %s ENCODING 'UTF8'",
		pq.QuoteIdentifier(dbName))

	if _, err := db.Exec(createQuery); err != nil {
		return xerrors.Errorf("failed to create database: %w", err)
	}

	// Transfer ownership to the specified owner
	alterQuery := fmt.Sprintf("ALTER DATABASE %s OWNER TO %s",
		pq.QuoteIdentifier(dbName), pq.QuoteIdentifier(owner))

	if _, err := db.Exec(alterQuery); err != nil {
		return xerrors.Errorf("failed to transfer database ownership: %w", err)
	}

	return nil
}

func grantConnectPermission(db *sql.DB, dbName, username string, logger *zap.Logger) error {
	// Grant CONNECT permission on database
	grantQuery := fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s",
		pq.QuoteIdentifier(dbName), pq.QuoteIdentifier(username))

	if _, err := db.Exec(grantQuery); err != nil {
		// This might fail if permission already exists, which is fine
		return xerrors.Errorf("failed to grant connect permission: %w", err)
	}

	return nil
}

func grantReadOnlyAccess(db *sql.DB, readUser, ownerUser string, logger *zap.Logger) error {
	queries := []string{
		fmt.Sprintf("GRANT USAGE ON SCHEMA public TO %s", pq.QuoteIdentifier(readUser)),
		fmt.Sprintf("GRANT SELECT ON ALL TABLES IN SCHEMA public TO %s", pq.QuoteIdentifier(readUser)),
		fmt.Sprintf("GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO %s", pq.QuoteIdentifier(readUser)),
		// Set default privileges for future objects created by the owner
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR USER %s IN SCHEMA public GRANT SELECT ON TABLES TO %s",
			pq.QuoteIdentifier(ownerUser), pq.QuoteIdentifier(readUser)),
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR USER %s IN SCHEMA public GRANT SELECT ON SEQUENCES TO %s",
			pq.QuoteIdentifier(ownerUser), pq.QuoteIdentifier(readUser)),
	}

	for _, q := range queries {
		if _, err := db.Exec(q); err != nil {
			// Some permissions might already exist, log but don't fail
			logger.Warn("Failed to grant read-only permission (continuing)", zap.Error(err))
		}
	}

	return nil
}

func grantFullAccess(db *sql.DB, username string, logger *zap.Logger) error {
	queries := []string{
		fmt.Sprintf("GRANT ALL PRIVILEGES ON SCHEMA public TO %s", pq.QuoteIdentifier(username)),
		fmt.Sprintf("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO %s", pq.QuoteIdentifier(username)),
		fmt.Sprintf("GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO %s", pq.QuoteIdentifier(username)),
		fmt.Sprintf("GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO %s", pq.QuoteIdentifier(username)),
		// Set default privileges for future objects
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO %s", pq.QuoteIdentifier(username)),
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO %s", pq.QuoteIdentifier(username)),
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON FUNCTIONS TO %s", pq.QuoteIdentifier(username)),
	}

	for _, q := range queries {
		if _, err := db.Exec(q); err != nil {
			// Some permissions might already exist, log but don't fail
			logger.Warn("Failed to grant full access permission (continuing)", zap.Error(err))
		}
	}

	return nil
}

func runMigrations(ctx context.Context, host string, port int, user, password, dbName string, logger *zap.Logger) error {
	// Connect to the network database to run migrations
	migrationDSN := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=require",
		host, port, dbName, user, password)

	migrationDB, err := sql.Open("postgres", migrationDSN)
	if err != nil {
		return xerrors.Errorf("failed to connect to database for migrations: %w", err)
	}
	defer func() {
		if closeErr := migrationDB.Close(); closeErr != nil {
			logger.Warn("Failed to close migration database connection", zap.Error(closeErr))
		}
	}()

	// Test connection
	if err := migrationDB.PingContext(ctx); err != nil {
		return xerrors.Errorf("failed to ping migration database: %w", err)
	}

	// Set dialect for goose
	if err := goose.SetDialect("postgres"); err != nil {
		return xerrors.Errorf("failed to set goose dialect: %w", err)
	}

	// Run migrations using the file system path
	migrationsDir := "/app/migrations"
	if err := goose.UpContext(ctx, migrationDB, migrationsDir); err != nil {
		return xerrors.Errorf("failed to run migrations: %w", err)
	}

	return nil
}

func init() {
	rootCmd.AddCommand(newDBInitCommand())
}
