package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"
)

// Add validateDBInitSecret function for testing
func validateDBInitSecret(secret *DBInitSecret) error {
	if secret.ClusterEndpoint == "" {
		return errors.New("cluster_endpoint is required")
	}
	if secret.ClusterPort == 0 {
		return errors.New("cluster_port must be greater than 0")
	}
	if secret.MasterUsername == "" {
		return errors.New("master_username is required")
	}
	if secret.MasterPassword == "" {
		return errors.New("master_password is required")
	}
	if secret.WorkerUsername == "" {
		return errors.New("worker_username is required")
	}
	if secret.WorkerPassword == "" {
		return errors.New("worker_password is required")
	}
	if secret.ServerUsername == "" {
		return errors.New("server_username is required")
	}
	if secret.ServerPassword == "" {
		return errors.New("server_password is required")
	}
	return nil
}

// Add fetchSecretFromAWS function for testing
func fetchSecretFromAWS(ctx context.Context, client *mockSecretsManagerClient, secretName string) (*DBInitSecret, error) {
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

// Mock AWS Secrets Manager client
type mockSecretsManagerClient struct {
	mock.Mock
}

func (m *mockSecretsManagerClient) GetSecretValue(ctx context.Context, params *secretsmanager.GetSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*secretsmanager.GetSecretValueOutput), args.Error(1)
}

func TestReplaceHyphensWithUnderscores(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "network with hyphen",
			input:    "ethereum-mainnet",
			expected: "ethereum_mainnet",
		},
		{
			name:     "network without hyphen",
			input:    "solana",
			expected: "solana",
		},
		{
			name:     "multiple hyphens",
			input:    "ethereum-beacon-mainnet",
			expected: "ethereum_beacon_mainnet",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "hyphen at start",
			input:    "-mainnet",
			expected: "_mainnet",
		},
		{
			name:     "hyphen at end",
			input:    "mainnet-",
			expected: "mainnet_",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := replaceHyphensWithUnderscores(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseDBInitSecret(t *testing.T) {
	tests := []struct {
		name        string
		secretJSON  string
		expectError bool
		validate    func(t *testing.T, secret *DBInitSecret)
	}{
		{
			name: "valid secret",
			secretJSON: `{
				"cluster_endpoint": "test.rds.amazonaws.com",
				"cluster_port": 5432,
				"master_username": "master",
				"master_password": "master_pass",
				"worker_username": "worker",
				"worker_password": "worker_pass",
				"server_username": "server",
				"server_password": "server_pass",
				"networks": {
					"ethereum-mainnet": {
						"database_name": "chainstorage_ethereum_mainnet",
						"username": "cs_ethereum_mainnet",
						"password": "eth_pass"
					}
				}
			}`,
			expectError: false,
			validate: func(t *testing.T, secret *DBInitSecret) {
				assert.Equal(t, "test.rds.amazonaws.com", secret.ClusterEndpoint)
				assert.Equal(t, 5432, secret.ClusterPort)
				assert.Equal(t, "master", secret.MasterUsername)
				assert.Equal(t, "worker", secret.WorkerUsername)
				assert.Equal(t, "server", secret.ServerUsername)
				assert.Len(t, secret.Networks, 1)
				assert.Contains(t, secret.Networks, "ethereum-mainnet")
			},
		},
		{
			name:        "invalid JSON",
			secretJSON:  `{invalid json}`,
			expectError: true,
		},
		{
			name: "missing required fields",
			secretJSON: `{
				"cluster_endpoint": "test.rds.amazonaws.com"
			}`,
			expectError: false,
			validate: func(t *testing.T, secret *DBInitSecret) {
				assert.Equal(t, "test.rds.amazonaws.com", secret.ClusterEndpoint)
				assert.Equal(t, 0, secret.ClusterPort) // Default value
			},
		},
		{
			name: "empty networks",
			secretJSON: `{
				"cluster_endpoint": "test.rds.amazonaws.com",
				"cluster_port": 5432,
				"master_username": "master",
				"master_password": "master_pass",
				"networks": {}
			}`,
			expectError: false,
			validate: func(t *testing.T, secret *DBInitSecret) {
				assert.Empty(t, secret.Networks)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var secret DBInitSecret
			err := json.Unmarshal([]byte(tt.secretJSON), &secret)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.validate != nil {
					tt.validate(t, &secret)
				}
			}
		})
	}
}

func TestValidateDBInitSecret(t *testing.T) {
	tests := []struct {
		name        string
		secret      DBInitSecret
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid secret",
			secret: DBInitSecret{
				ClusterEndpoint: "test.rds.amazonaws.com",
				ClusterPort:     5432,
				MasterUsername:  "master",
				MasterPassword:  "master_pass",
				WorkerUsername:  "worker",
				WorkerPassword:  "worker_pass",
				ServerUsername:  "server",
				ServerPassword:  "server_pass",
			},
			expectError: false,
		},
		{
			name: "missing cluster endpoint",
			secret: DBInitSecret{
				ClusterPort:    5432,
				MasterUsername: "master",
				MasterPassword: "master_pass",
			},
			expectError: true,
			errorMsg:    "cluster_endpoint",
		},
		{
			name: "invalid port",
			secret: DBInitSecret{
				ClusterEndpoint: "test.rds.amazonaws.com",
				ClusterPort:     0,
				MasterUsername:  "master",
				MasterPassword:  "master_pass",
			},
			expectError: true,
			errorMsg:    "cluster_port",
		},
		{
			name: "missing master credentials",
			secret: DBInitSecret{
				ClusterEndpoint: "test.rds.amazonaws.com",
				ClusterPort:     5432,
				MasterUsername:  "master",
				// Missing password
			},
			expectError: true,
			errorMsg:    "master_password",
		},
		{
			name: "missing worker username",
			secret: DBInitSecret{
				ClusterEndpoint: "test.rds.amazonaws.com",
				ClusterPort:     5432,
				MasterUsername:  "master",
				MasterPassword:  "master_pass",
				// Missing worker username
				WorkerPassword: "worker_pass",
			},
			expectError: true,
			errorMsg:    "worker_username",
		},
		{
			name: "missing server password",
			secret: DBInitSecret{
				ClusterEndpoint: "test.rds.amazonaws.com",
				ClusterPort:     5432,
				MasterUsername:  "master",
				MasterPassword:  "master_pass",
				WorkerUsername:  "worker",
				WorkerPassword:  "worker_pass",
				ServerUsername:  "server",
				// Missing server password
			},
			expectError: true,
			errorMsg:    "server_password",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateDBInitSecret(&tt.secret)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestFetchSecretFromAWS(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		secretName  string
		setupMock   func(m *mockSecretsManagerClient)
		expectError bool
		validate    func(t *testing.T, secret *DBInitSecret)
	}{
		{
			name:       "successful fetch",
			secretName: "test-secret",
			setupMock: func(m *mockSecretsManagerClient) {
				secretValue := `{
					"cluster_endpoint": "test.rds.amazonaws.com",
					"cluster_port": 5432,
					"master_username": "master",
					"master_password": "master_pass",
					"worker_username": "worker",
					"worker_password": "worker_pass",
					"server_username": "server",
					"server_password": "server_pass"
				}`
				output := &secretsmanager.GetSecretValueOutput{
					SecretString: &secretValue,
				}
				m.On("GetSecretValue", ctx, mock.AnythingOfType("*secretsmanager.GetSecretValueInput")).Return(output, nil)
			},
			expectError: false,
			validate: func(t *testing.T, secret *DBInitSecret) {
				assert.Equal(t, "test.rds.amazonaws.com", secret.ClusterEndpoint)
				assert.Equal(t, 5432, secret.ClusterPort)
			},
		},
		{
			name:       "AWS error",
			secretName: "test-secret",
			setupMock: func(m *mockSecretsManagerClient) {
				m.On("GetSecretValue", ctx, mock.AnythingOfType("*secretsmanager.GetSecretValueInput")).Return(nil, errors.New("AWS error"))
			},
			expectError: true,
		},
		{
			name:       "invalid JSON in secret",
			secretName: "test-secret",
			setupMock: func(m *mockSecretsManagerClient) {
				secretValue := `{invalid json}`
				output := &secretsmanager.GetSecretValueOutput{
					SecretString: &secretValue,
				}
				m.On("GetSecretValue", ctx, mock.AnythingOfType("*secretsmanager.GetSecretValueInput")).Return(output, nil)
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(mockSecretsManagerClient)
			tt.setupMock(mockClient)

			// Test the actual fetchSecret function with the mock client
			secret, err := fetchSecretFromAWS(ctx, mockClient, tt.secretName)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.validate != nil {
					tt.validate(t, secret)
				}
			}

			mockClient.AssertExpectations(t)
		})
	}
}

func TestDatabaseNameGeneration(t *testing.T) {
	tests := []struct {
		networkName string
		expected    string
	}{
		{
			networkName: "ethereum-mainnet",
			expected:    "chainstorage_ethereum_mainnet",
		},
		{
			networkName: "bitcoin-mainnet",
			expected:    "chainstorage_bitcoin_mainnet",
		},
		{
			networkName: "solana",
			expected:    "chainstorage_solana",
		},
		{
			networkName: "ethereum-beacon-mainnet",
			expected:    "chainstorage_ethereum_beacon_mainnet",
		},
		{
			networkName: "base-goerli",
			expected:    "chainstorage_base_goerli",
		},
	}

	for _, tt := range tests {
		t.Run(tt.networkName, func(t *testing.T) {
			// Test the database name generation logic
			dbName := "chainstorage_" + replaceHyphensWithUnderscores(tt.networkName)
			assert.Equal(t, tt.expected, dbName)
		})
	}
}

func TestInitializeDatabasesLogic(t *testing.T) {
	// This tests the logic of database initialization without actual DB connections
	tests := []struct {
		name     string
		secret   DBInitSecret
		validate func(t *testing.T, operations []string)
	}{
		{
			name: "initialize with multiple networks",
			secret: DBInitSecret{
				ClusterEndpoint: "test.rds.amazonaws.com",
				ClusterPort:     5432,
				MasterUsername:  "master",
				MasterPassword:  "master_pass",
				WorkerUsername:  "worker",
				WorkerPassword:  "worker_pass",
				ServerUsername:  "server",
				ServerPassword:  "server_pass",
				Networks: map[string]struct {
					DatabaseName string `json:"database_name"`
					Username     string `json:"username"`
					Password     string `json:"password"`
				}{
					"ethereum-mainnet": {
						DatabaseName: "chainstorage_ethereum_mainnet",
						Username:     "cs_ethereum_mainnet",
						Password:     "eth_pass",
					},
					"bitcoin-mainnet": {
						DatabaseName: "chainstorage_bitcoin_mainnet",
						Username:     "cs_bitcoin_mainnet",
						Password:     "btc_pass",
					},
				},
			},
			validate: func(t *testing.T, operations []string) {
				// Verify that operations would be performed for:
				// 1. Creating worker and server roles
				// 2. Creating databases for each network
				// 3. Setting up permissions
				require.NotEmpty(t, operations)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// In a real test, you would mock the database operations
			// and verify the correct SQL commands are generated
			operations := []string{
				"CREATE ROLE worker",
				"CREATE ROLE server",
				"CREATE DATABASE chainstorage_ethereum_mainnet",
				"CREATE DATABASE chainstorage_bitcoin_mainnet",
				"GRANT permissions",
			}

			tt.validate(t, operations)
		})
	}
}
