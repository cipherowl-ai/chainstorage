package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/lib/pq"
	"github.com/spf13/cobra"
	"golang.org/x/xerrors"

	_ "github.com/lib/pq"
)

type DBInitSecret struct {
	ClusterEndpoint string `json:"cluster_endpoint"`
	ClusterPort     int    `json:"cluster_port"`
	MasterUsername  string `json:"master_username"`
	MasterPassword  string `json:"master_password"`
	WorkerUsername  string `json:"worker_username"`
	WorkerPassword  string `json:"worker_password"`
	ServerUsername  string `json:"server_username"`
	ServerPassword  string `json:"server_password"`
	Networks        map[string]struct {
		DatabaseName string `json:"database_name"`
		Username     string `json:"username"`
		Password     string `json:"password"`
	} `json:"networks"`
}

func newDBInitCommand() *cobra.Command {
	var (
		secretName string
		awsRegion  string
		dryRun     bool
	)

	cmd := &cobra.Command{
		Use:   "db-init",
		Short: "Initialize databases and users from AWS Secrets Manager",
		Long: `Initialize PostgreSQL databases and users based on configuration stored in AWS Secrets Manager.

This command:
1. Fetches database initialization configuration from AWS Secrets Manager
2. Creates role-based users (worker with read/write, server with read-only)
3. Creates a database for each configured network
4. Sets up proper permissions for each role
5. Is idempotent - can be run multiple times safely

The command must be run from within the Kubernetes cluster (e.g., admin pod) 
with proper IAM role attached to access the secret.

Example usage:
  # Initialize databases for dev environment
  chainstorage admin db-init --secret-name=chainstorage/db-init/dev

  # Dry run to preview changes
  chainstorage admin db-init --secret-name=chainstorage/db-init/dev --dry-run

  # Use specific AWS region
  chainstorage admin db-init --secret-name=chainstorage/db-init/prod --aws-region=us-west-2`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDBInit(secretName, awsRegion, dryRun)
		},
	}

	cmd.Flags().StringVar(&secretName, "secret-name", "", "AWS Secrets Manager secret name")
	cmd.Flags().StringVar(&awsRegion, "aws-region", "us-east-1", "AWS region")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Preview changes without applying them")

	if err := cmd.MarkFlagRequired("secret-name"); err != nil {
		return nil
	}

	return cmd
}

func runDBInit(secretName, awsRegion string, dryRun bool) error {
	ctx := context.Background()

	log.Printf("🚀 Starting database initialization...")
	log.Printf("   Secret: %s", secretName)
	log.Printf("   Region: %s", awsRegion)
	log.Printf("   Dry-run: %v", dryRun)
	log.Printf("")

	// Fetch secret from AWS Secrets Manager
	secret, err := fetchSecret(ctx, secretName, awsRegion)
	if err != nil {
		return xerrors.Errorf("failed to fetch secret: %w", err)
	}

	log.Printf("✅ Successfully fetched secret from AWS Secrets Manager")
	log.Printf("   Cluster: %s:%d", secret.ClusterEndpoint, secret.ClusterPort)
	log.Printf("   Networks to configure: %d", len(secret.Networks))
	log.Printf("")

	if dryRun {
		log.Printf("🔍 DRY RUN MODE - No changes will be made")
		log.Printf("")
		log.Printf("Would create users:")
		log.Printf("   - %s (worker with CREATEDB privilege)", secret.WorkerUsername)
		log.Printf("   - %s (server with read-only access)", secret.ServerUsername)
		log.Printf("")
		log.Printf("Would create databases:")

		// Sort networks for consistent output
		var networkNames []string
		for network := range secret.Networks {
			networkNames = append(networkNames, network)
		}
		sort.Strings(networkNames)

		for _, network := range networkNames {
			cfg := secret.Networks[network]
			log.Printf("   - %s for network %s", cfg.DatabaseName, network)
			if cfg.Username != "" {
				log.Printf("     with user: %s", cfg.Username)
			}
		}
		return nil
	}

	// Connect to PostgreSQL as master user
	masterDSN := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=require",
		secret.ClusterEndpoint, secret.ClusterPort, secret.MasterUsername, secret.MasterPassword)

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

	// Create role-based users
	log.Printf("Creating role-based users...")

	if err := createUser(db, secret.WorkerUsername, secret.WorkerPassword, true); err != nil {
		return xerrors.Errorf("failed to create worker user %s: %w", secret.WorkerUsername, err)
	}
	log.Printf("   ✓ Created/verified worker user: %s", secret.WorkerUsername)

	if err := createUser(db, secret.ServerUsername, secret.ServerPassword, false); err != nil {
		return xerrors.Errorf("failed to create server user %s: %w", secret.ServerUsername, err)
	}
	log.Printf("   ✓ Created/verified server user: %s", secret.ServerUsername)
	log.Printf("")

	// Process each network
	log.Printf("Setting up network databases...")
	successCount := 0

	// Sort networks for consistent processing order
	var networkNames []string
	for network := range secret.Networks {
		networkNames = append(networkNames, network)
	}
	sort.Strings(networkNames)

	for _, network := range networkNames {
		netConfig := secret.Networks[network]
		log.Printf("")
		log.Printf("Processing network: %s", network)

		// Create database
		if err := createDatabase(db, netConfig.DatabaseName, secret.WorkerUsername); err != nil {
			return xerrors.Errorf("failed to create database %s: %w", netConfig.DatabaseName, err)
		}
		log.Printf("   ✓ Created/verified database: %s", netConfig.DatabaseName)

		// Grant CONNECT permission to server user
		if err := grantConnectPermission(db, netConfig.DatabaseName, secret.ServerUsername); err != nil {
			return xerrors.Errorf("failed to grant CONNECT permission on %s to %s: %w",
				netConfig.DatabaseName, secret.ServerUsername, err)
		}

		// Connect to the network database to grant permissions
		netDSN := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=require",
			secret.ClusterEndpoint, secret.ClusterPort, secret.MasterUsername,
			secret.MasterPassword, netConfig.DatabaseName)

		netDB, err := sql.Open("postgres", netDSN)
		if err != nil {
			return xerrors.Errorf("failed to connect to database %s: %w", netConfig.DatabaseName, err)
		}
		defer func() {
			if closeErr := netDB.Close(); closeErr != nil {
				log.Printf("Warning: failed to close network database connection: %v", closeErr)
			}
		}()

		// Grant permissions to server user
		if err := grantReadOnlyAccess(netDB, secret.ServerUsername, secret.WorkerUsername); err != nil {
			return xerrors.Errorf("failed to grant permissions on %s: %w", netConfig.DatabaseName, err)
		}
		log.Printf("   ✓ Granted read-only access to: %s", secret.ServerUsername)

		// Create network-specific user if specified
		if netConfig.Username != "" && netConfig.Username != secret.WorkerUsername && netConfig.Username != secret.ServerUsername {
			if err := createUser(db, netConfig.Username, netConfig.Password, false); err != nil {
				return xerrors.Errorf("failed to create user %s: %w", netConfig.Username, err)
			}
			log.Printf("   ✓ Created/verified network user: %s", netConfig.Username)

			// Grant permissions to network-specific user
			if err := grantConnectPermission(db, netConfig.DatabaseName, netConfig.Username); err != nil {
				return xerrors.Errorf("failed to grant CONNECT permission to %s: %w", netConfig.Username, err)
			}

			if err := grantFullAccess(netDB, netConfig.Username); err != nil {
				return xerrors.Errorf("failed to grant full access to %s: %w", netConfig.Username, err)
			}
			log.Printf("   ✓ Granted full access to network user: %s", netConfig.Username)
		}

		successCount++
	}

	log.Printf("")
	log.Printf("✅ Database initialization completed successfully!")
	log.Printf("   Total networks configured: %d", successCount)
	log.Printf("")
	log.Printf("Next steps:")
	log.Printf("   1. Deploy chainstorage worker pods to start data ingestion")
	log.Printf("   2. Deploy chainstorage server pods for API access")
	log.Printf("   3. Monitor logs for successful connections")

	return nil
}

func fetchSecret(ctx context.Context, secretName, region string) (*DBInitSecret, error) {
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

	var secret DBInitSecret
	if err := json.Unmarshal([]byte(*result.SecretString), &secret); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal secret: %w", err)
	}

	return &secret, nil
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
