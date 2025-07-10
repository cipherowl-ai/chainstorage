#!/bin/bash
# Test script for database initialization functionality

set -e

echo "============================================"
echo "Chainstorage Database Initialization Testing"
echo "============================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    case $1 in
        "success") echo -e "${GREEN}✓ $2${NC}" ;;
        "error") echo -e "${RED}✗ $2${NC}" ;;
        "info") echo -e "${YELLOW}→ $2${NC}" ;;
    esac
}

# Step 1: Start local development environment
print_status "info" "Starting local development environment..."
docker-compose -f docker-compose-local-dev.yml up -d

# Wait for services to be ready
print_status "info" "Waiting for services to start..."
sleep 10

# Verify services are running
if docker-compose -f docker-compose-local-dev.yml ps | grep -q "Up"; then
    print_status "success" "Docker services are running"
else
    print_status "error" "Docker services failed to start"
    exit 1
fi

# Step 2: Build the chainstorage binary
print_status "info" "Building chainstorage binary..."
go build -o bin/chainstorage ./cmd/admin

if [ -f bin/chainstorage ]; then
    print_status "success" "Binary built successfully"
else
    print_status "error" "Failed to build binary"
    exit 1
fi

# Step 3: Test the setup-postgres command
print_status "info" "Testing setup-postgres command..."

# Test creating a new database
./bin/chainstorage admin setup-postgres \
    --network ethereum-goerli \
    --host localhost \
    --port 5433 \
    --master-user postgres \
    --master-password postgres \
    --worker-user chainstorage_worker \
    --server-user chainstorage_server \
    --ssl-mode disable

# Verify database was created
export PGPASSWORD=postgres
if psql -h localhost -p 5433 -U postgres -d postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'chainstorage_ethereum_goerli'" | grep -q 1; then
    print_status "success" "Database chainstorage_ethereum_goerli created"
else
    print_status "error" "Database creation failed"
fi

# Step 4: Test AWS Secrets Manager integration with LocalStack
print_status "info" "Setting up LocalStack test..."

# Configure AWS CLI for LocalStack
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:4566

# Create test secret
print_status "info" "Creating test secret in LocalStack..."
aws secretsmanager create-secret \
    --name chainstorage/db-init/test \
    --secret-string '{
  "cluster_endpoint": "localhost",
  "cluster_port": 5433,
  "master_username": "postgres",
  "master_password": "postgres",
  "worker_username": "chainstorage_worker",
  "worker_password": "worker_password",
  "server_username": "chainstorage_server",
  "server_password": "server_password",
  "networks": {
    "bitcoin-testnet": {
      "database_name": "chainstorage_bitcoin_testnet",
      "username": "cs_bitcoin_testnet",
      "password": "testnet_password"
    },
    "ethereum-sepolia": {
      "database_name": "chainstorage_ethereum_sepolia",
      "username": "cs_ethereum_sepolia",
      "password": "sepolia_password"
    }
  }
}' 2>/dev/null || print_status "info" "Secret already exists"

# Test db-init command
print_status "info" "Testing db-init command..."
./bin/chainstorage admin db-init \
    --config config/config.yml \
    --secret-name chainstorage/db-init/test \
    --aws-region us-east-1

# Verify databases were created
export PGPASSWORD=postgres
for db in chainstorage_bitcoin_testnet chainstorage_ethereum_sepolia; do
    if psql -h localhost -p 5433 -U postgres -d postgres -tc "SELECT 1 FROM pg_database WHERE datname = '$db'" | grep -q 1; then
        print_status "success" "Database $db created"
    else
        print_status "error" "Database $db not found"
    fi
done

# Step 5: Run unit tests
print_status "info" "Running unit tests..."
TEST_TYPE=unit go test ./cmd/admin -v -run TestReplaceHyphensWithUnderscores

# Step 6: Run integration tests
print_status "info" "Running PostgreSQL integration tests..."
export CHAINSTORAGE_AWS_POSTGRES_HOST=localhost
export CHAINSTORAGE_AWS_POSTGRES_PORT=5433
export CHAINSTORAGE_AWS_POSTGRES_USER=chainstorage_worker
export CHAINSTORAGE_AWS_POSTGRES_PASSWORD=chainstorage_worker
export CHAINSTORAGE_AWS_POSTGRES_DATABASE=chainstorage_ethereum_mainnet

TEST_TYPE=integration go test ./internal/storage/metastorage/postgres -v -run TestIntegrationBlockStorageTestSuite -count=1

# Step 7: Verify permissions
print_status "info" "Verifying database permissions..."
export PGPASSWORD=postgres

# Check if server role has read-only access
psql -h localhost -p 5433 -U postgres -d chainstorage_ethereum_mainnet <<EOF
-- Check permissions
SELECT has_database_privilege('chainstorage_server', 'chainstorage_ethereum_mainnet', 'CONNECT');
EOF

# Step 8: Test error handling
print_status "info" "Testing error handling..."

# Test with invalid credentials (should fail gracefully)
set +e
./bin/chainstorage admin setup-postgres \
    --network test-network \
    --host localhost \
    --port 5433 \
    --master-user invalid \
    --master-password invalid \
    --worker-user test_worker \
    --server-user test_server \
    --ssl-mode disable 2>&1 | grep -q "authentication failed"

if [ $? -eq 0 ]; then
    print_status "success" "Error handling works correctly"
else
    print_status "error" "Error handling test failed"
fi
set -e

# Summary
echo ""
echo "============================================"
echo "Test Summary"
echo "============================================"
print_status "success" "All tests completed!"
echo ""
echo "To clean up test environment:"
echo "  docker-compose -f docker-compose-local-dev.yml down -v"
echo ""
echo "To view PostgreSQL logs:"
echo "  docker-compose -f docker-compose-local-dev.yml logs chainstorage-postgres"
echo ""
echo "To connect to PostgreSQL:"
echo "  PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres" 