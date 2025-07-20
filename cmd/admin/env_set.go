package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"golang.org/x/xerrors"
)

func newEnvSetCommand() *cobra.Command {
	var (
		awsRegion string
		export    bool
	)

	cmd := &cobra.Command{
		Use:   "env-set",
		Short: "Set environment variables for PostgreSQL credentials from AWS Secrets Manager",
		Long: `Set environment variables for PostgreSQL credentials from AWS Secrets Manager.

This command fetches database credentials from AWS Secrets Manager and outputs them
as environment variables that can be used in Kubernetes deployments.

Example usage:
  # Output environment variables for ethereum mainnet worker
  chainstorage admin env-set --blockchain ethereum --network mainnet --env dev --role worker

  # Output as shell export format (for sourcing in bash)
  chainstorage admin env-set --blockchain ethereum --network mainnet --env dev --role worker --export

Available roles:
  - worker: Read-write access user
  - server: Read-only access user

The command will output the following environment variables:
  - CHAINSTORAGE_AWS_POSTGRES_USER
  - CHAINSTORAGE_AWS_POSTGRES_PASSWORD
  - CHAINSTORAGE_AWS_POSTGRES_DATABASE
  - CHAINSTORAGE_AWS_POSTGRES_HOST (from master credentials)
  - CHAINSTORAGE_AWS_POSTGRES_PORT (from master credentials)
  - CHAINSTORAGE_AWS_POSTGRES_SSL_MODE (default: require)`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Parse role flag
			role, err := cmd.Flags().GetString("role")
			if err != nil {
				return err
			}

			if role != "worker" && role != "server" {
				return xerrors.New("role must be either 'worker' or 'server'")
			}

			return runEnvSet(commonFlags.blockchain, commonFlags.network, commonFlags.env, awsRegion, role, export)
		},
	}

	cmd.Flags().StringVar(&awsRegion, "aws-region", "us-east-1", "AWS region for Secrets Manager")
	cmd.Flags().String("role", "", "Database role: worker (read-write) or server (read-only)")
	cmd.Flags().BoolVar(&export, "export", false, "Output as shell export format (export VAR=value)")

	// Mark role as required
	if err := cmd.MarkFlagRequired("role"); err != nil {
		panic(err)
	}

	return cmd
}

type EnvVars struct {
	User     string `json:"CHAINSTORAGE_AWS_POSTGRES_USER"`
	Password string `json:"CHAINSTORAGE_AWS_POSTGRES_PASSWORD"`
	Database string `json:"CHAINSTORAGE_AWS_POSTGRES_DATABASE"`
	Host     string `json:"CHAINSTORAGE_AWS_POSTGRES_HOST"`
	Port     string `json:"CHAINSTORAGE_AWS_POSTGRES_PORT"`
	SSLMode  string `json:"CHAINSTORAGE_AWS_POSTGRES_SSL_MODE"`
}

func runEnvSet(blockchain, network, env, awsRegion, role string, export bool) error {
	ctx := context.Background()

	log.Printf("🔧 Setting environment variables for %s-%s %s role...", blockchain, network, role)
	log.Printf("   Environment: %s", env)
	log.Printf("   AWS Region: %s", awsRegion)
	log.Printf("   Role: %s", role)
	log.Printf("")

	// Get master credentials from environment variables
	masterHost := os.Getenv("CHAINSTORAGE_CLUSTER_ENDPOINT")
	masterPortStr := os.Getenv("CHAINSTORAGE_CLUSTER_PORT")

	if masterHost == "" || masterPortStr == "" {
		return xerrors.New("missing required environment variables: CHAINSTORAGE_CLUSTER_ENDPOINT, CHAINSTORAGE_CLUSTER_PORT")
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

	var username, password string
	if role == "worker" {
		username, err = getStringFromSecret(secretData, fmt.Sprintf("%s_worker_username", networkKey))
		if err != nil {
			return xerrors.Errorf("failed to get worker username: %w", err)
		}
		password, err = getStringFromSecret(secretData, fmt.Sprintf("%s_worker_password", networkKey))
		if err != nil {
			return xerrors.Errorf("failed to get worker password: %w", err)
		}
	} else {
		username, err = getStringFromSecret(secretData, fmt.Sprintf("%s_server_username", networkKey))
		if err != nil {
			return xerrors.Errorf("failed to get server username: %w", err)
		}
		password, err = getStringFromSecret(secretData, fmt.Sprintf("%s_server_password", networkKey))
		if err != nil {
			return xerrors.Errorf("failed to get server password: %w", err)
		}
	}

	log.Printf("✅ Successfully fetched credentials from AWS Secrets Manager")
	log.Printf("   Database: %s", dbName)
	log.Printf("   User: %s", username)
	log.Printf("")

	// Create environment variables
	envVars := EnvVars{
		User:     username,
		Password: password,
		Database: dbName,
		Host:     masterHost,
		Port:     masterPortStr,
		SSLMode:  "require", // Default SSL mode
	}

	// Output environment variables
	if export {
		// Shell export format (for sourcing in bash)
		fmt.Printf("export CHAINSTORAGE_AWS_POSTGRES_USER=\"%s\"\n", envVars.User)
		fmt.Printf("export CHAINSTORAGE_AWS_POSTGRES_PASSWORD=\"%s\"\n", envVars.Password)
		fmt.Printf("export CHAINSTORAGE_AWS_POSTGRES_DATABASE=\"%s\"\n", envVars.Database)
		fmt.Printf("export CHAINSTORAGE_AWS_POSTGRES_HOST=\"%s\"\n", envVars.Host)
		fmt.Printf("export CHAINSTORAGE_AWS_POSTGRES_PORT=\"%s\"\n", envVars.Port)
		fmt.Printf("export CHAINSTORAGE_AWS_POSTGRES_SSL_MODE=\"%s\"\n", envVars.SSLMode)
	} else {
		// Standard env format (for .env files or direct use)
		fmt.Printf("CHAINSTORAGE_AWS_POSTGRES_USER=%s\n", envVars.User)
		fmt.Printf("CHAINSTORAGE_AWS_POSTGRES_PASSWORD=%s\n", envVars.Password)
		fmt.Printf("CHAINSTORAGE_AWS_POSTGRES_DATABASE=%s\n", envVars.Database)
		fmt.Printf("CHAINSTORAGE_AWS_POSTGRES_HOST=%s\n", envVars.Host)
		fmt.Printf("CHAINSTORAGE_AWS_POSTGRES_PORT=%s\n", envVars.Port)
		fmt.Printf("CHAINSTORAGE_AWS_POSTGRES_SSL_MODE=%s\n", envVars.SSLMode)
	}

	log.Printf("")
	log.Printf("✅ Environment variables ready for %s-%s %s role", blockchain, network, role)
	log.Printf("   Use these variables in your Kubernetes deployment or shell environment")

	return nil
}

func init() {
	rootCmd.AddCommand(newEnvSetCommand())
}
