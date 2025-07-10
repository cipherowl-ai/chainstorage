package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/lib/pq"
	"github.com/spf13/cobra"
	"golang.org/x/xerrors"

	_ "github.com/lib/pq"
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

	log.Printf("🚀 Starting database initialization for %s-%s...", blockchain, network)
	log.Printf("   Environment: %s", env)
	log.Printf("   Region: %s", awsRegion)
	log.Printf("   Dry-run: %v", dryRun)
	log.Printf("")

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

	log.Printf("✅ Successfully fetched credentials from AWS Secrets Manager")
	log.Printf("   Database: %s", dbName)
	log.Printf("   Server user: %s", serverUsername)
	log.Printf("   Worker user: %s", workerUsername)
	log.Printf("")

	if dryRun {
		log.Printf("🔍 DRY RUN MODE - No changes will be made")
		log.Printf("")
		log.Printf("Would create:")
		log.Printf("   - Database: %s", dbName)
		log.Printf("   - User: %s (read-only access)", serverUsername)
		log.Printf("   - User: %s (read-write access)", workerUsername)
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
			log.Printf("Warning: failed to close database connection: %v", closeErr)
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

	log.Printf("✅ Successfully connected to PostgreSQL cluster")
	log.Printf("")

	// Create users
	log.Printf("Creating users...")

	if err := createUser(db, workerUsername, workerPassword, true); err != nil {
		return xerrors.Errorf("failed to create worker user %s: %w", workerUsername, err)
	}
	log.Printf("   ✓ Created/verified worker user: %s", workerUsername)

	if err := createUser(db, serverUsername, serverPassword, false); err != nil {
		return xerrors.Errorf("failed to create server user %s: %w", serverUsername, err)
	}
	log.Printf("   ✓ Created/verified server user: %s", serverUsername)
	log.Printf("")

	// Create database
	log.Printf("Creating database...")
	if err := createDatabase(db, dbName, workerUsername); err != nil {
		return xerrors.Errorf("failed to create database %s: %w", dbName, err)
	}
	log.Printf("   ✓ Created/verified database: %s", dbName)

	// Grant permissions
	log.Printf("Setting up permissions...")

	// Grant CONNECT permission to server user
	if err := grantConnectPermission(db, dbName, serverUsername); err != nil {
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
			log.Printf("Warning: failed to close network database connection: %v", closeErr)
		}
	}()

	// Grant permissions to server user
	if err := grantReadOnlyAccess(netDB, serverUsername, workerUsername); err != nil {
		return xerrors.Errorf("failed to grant permissions on %s: %w", dbName, err)
	}
	log.Printf("   ✓ Granted read-only access to: %s", serverUsername)

	log.Printf("")
	log.Printf("✅ Database initialization completed successfully!")
	log.Printf("   Network: %s-%s", blockchain, network)
	log.Printf("   Database: %s", dbName)
	log.Printf("   Users created: %s (worker), %s (server)", workerUsername, serverUsername)
	log.Printf("")
	log.Printf("Next steps:")
	log.Printf("   1. Deploy chainstorage worker pods to start data ingestion")
	log.Printf("   2. Deploy chainstorage server pods for API access")
	log.Printf("   3. Monitor logs for successful connections")

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

func createUser(db *sql.DB, username, password string, canCreateDB bool) error {
	// Check if user exists
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM pg_user WHERE usename = $1)`
	if err := db.QueryRow(query, username).Scan(&exists); err != nil {
		return xerrors.Errorf("failed to check if user exists: %w", err)
	}

	if exists {
		log.Printf("     User %s already exists, updating password", username)
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

func createDatabase(db *sql.DB, dbName, owner string) error {
	// Check if database exists
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)`
	if err := db.QueryRow(query, dbName).Scan(&exists); err != nil {
		return xerrors.Errorf("failed to check if database exists: %w", err)
	}

	if exists {
		log.Printf("     Database %s already exists, skipping creation", dbName)
		return nil
	}

	// Create database with proper quoting
	createQuery := fmt.Sprintf("CREATE DATABASE %s OWNER %s ENCODING 'UTF8'",
		pq.QuoteIdentifier(dbName), pq.QuoteIdentifier(owner))

	if _, err := db.Exec(createQuery); err != nil {
		return xerrors.Errorf("failed to create database: %w", err)
	}

	return nil
}

func grantConnectPermission(db *sql.DB, dbName, username string) error {
	// Grant CONNECT permission on database
	grantQuery := fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s",
		pq.QuoteIdentifier(dbName), pq.QuoteIdentifier(username))

	if _, err := db.Exec(grantQuery); err != nil {
		// This might fail if permission already exists, which is fine
		log.Printf("     Warning: %v (continuing...)", err)
	}

	return nil
}

func grantReadOnlyAccess(db *sql.DB, readUser, ownerUser string) error {
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
			log.Printf("     Warning: %v (continuing...)", err)
		}
	}

	return nil
}

func grantFullAccess(db *sql.DB, username string) error {
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
			log.Printf("     Warning: %v (continuing...)", err)
		}
	}

	return nil
}

func init() {
	rootCmd.AddCommand(newDBInitCommand())
}
