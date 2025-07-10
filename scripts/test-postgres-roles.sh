#!/bin/bash
set -e

echo "🧪 Testing Chainstorage PostgreSQL Role-Based Setup"
echo "=================================================="

# Configuration
POSTGRES_HOST=${CHAINSTORAGE_AWS_POSTGRES_HOST:-localhost}
POSTGRES_PORT=${CHAINSTORAGE_AWS_POSTGRES_PORT:-5432}
POSTGRES_DB="postgres"

# Users and passwords (from init script)
MASTER_USER="postgres"
MASTER_PASS="postgres"
WORKER_USER="chainstorage_worker"
WORKER_PASS="chainstorage_worker"
SERVER_USER="chainstorage_server"
SERVER_PASS="chainstorage_server"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to run SQL and check result
test_sql() {
    local user=$1
    local pass=$2
    local description=$3
    local sql=$4
    local expect_success=${5:-true}
    
    echo -e "\n${BLUE}Testing: $description${NC}"
    echo "User: $user"
    echo "SQL: $sql"
    
    if PGPASSWORD=$pass psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $user -d $POSTGRES_DB -c "$sql" >/dev/null 2>&1; then
        if [ "$expect_success" = "true" ]; then
            echo -e "${GREEN}✅ PASS${NC}"
        else
            echo -e "${RED}❌ FAIL (Expected failure but succeeded)${NC}"
            return 1
        fi
    else
        if [ "$expect_success" = "false" ]; then
            echo -e "${GREEN}✅ PASS (Expected failure)${NC}"
        else
            echo -e "${RED}❌ FAIL${NC}"
            return 1
        fi
    fi
}

# Function to test database creation
test_db_creation() {
    local user=$1
    local pass=$2
    local can_create=$3
    local test_db="test_db_$(date +%s)"
    
    echo -e "\n${BLUE}Testing database creation for $user${NC}"
    
    if PGPASSWORD=$pass psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $user -d $POSTGRES_DB -c "CREATE DATABASE \"$test_db\";" >/dev/null 2>&1; then
        # Clean up the test database
        PGPASSWORD=$pass psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $user -d $POSTGRES_DB -c "DROP DATABASE \"$test_db\";" >/dev/null 2>&1
        
        if [ "$can_create" = "true" ]; then
            echo -e "${GREEN}✅ PASS - Can create databases${NC}"
        else
            echo -e "${RED}❌ FAIL - Should not be able to create databases${NC}"
            return 1
        fi
    else
        if [ "$can_create" = "false" ]; then
            echo -e "${GREEN}✅ PASS - Cannot create databases (as expected)${NC}"
        else
            echo -e "${RED}❌ FAIL - Should be able to create databases${NC}"
            return 1
        fi
    fi
}

echo -e "\n${YELLOW}🔍 Checking PostgreSQL connection...${NC}"
if ! pg_isready -h $POSTGRES_HOST -p $POSTGRES_PORT >/dev/null 2>&1; then
    echo -e "${RED}❌ PostgreSQL is not running on $POSTGRES_HOST:$POSTGRES_PORT${NC}"
    echo "Start it with: docker-compose -f docker-compose-local-dev.yml up -d chainstorage-postgres"
    exit 1
fi
echo -e "${GREEN}✅ PostgreSQL is running${NC}"

echo -e "\n${YELLOW}🔍 Verifying users exist...${NC}"
for user in $MASTER_USER $WORKER_USER $SERVER_USER; do
    if PGPASSWORD=postgres psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U postgres -d postgres -t -c "SELECT 1 FROM pg_roles WHERE rolname='$user'" | grep -q 1; then
        echo -e "${GREEN}✅ User '$user' exists${NC}"
    else
        echo -e "${RED}❌ User '$user' does not exist${NC}"
        echo "Run: docker-compose -f docker-compose-local-dev.yml down && docker-compose -f docker-compose-local-dev.yml up -d"
        exit 1
    fi
done

echo -e "\n${YELLOW}📋 Testing user permissions...${NC}"

# Test Master User (should be able to do everything)
echo -e "\n${BLUE}=== MASTER USER TESTS ===${NC}"
test_sql $MASTER_USER $MASTER_PASS "Connect to database" "SELECT 1;"
test_sql $MASTER_USER $MASTER_PASS "Create table" "CREATE TABLE IF NOT EXISTS test_master (id INT);"
test_sql $MASTER_USER $MASTER_PASS "Insert data" "INSERT INTO test_master VALUES (1);"
test_sql $MASTER_USER $MASTER_PASS "Read data" "SELECT * FROM test_master;"
test_sql $MASTER_USER $MASTER_PASS "Drop table" "DROP TABLE IF EXISTS test_master;"
test_db_creation $MASTER_USER $MASTER_PASS "true"

# Test Worker User (should be able to read/write and create databases)
echo -e "\n${BLUE}=== WORKER USER TESTS ===${NC}"
test_sql $WORKER_USER $WORKER_PASS "Connect to database" "SELECT 1;"
test_sql $WORKER_USER $WORKER_PASS "Create table" "CREATE TABLE IF NOT EXISTS test_worker (id INT);"
test_sql $WORKER_USER $WORKER_PASS "Insert data" "INSERT INTO test_worker VALUES (1);"
test_sql $WORKER_USER $WORKER_PASS "Read data" "SELECT * FROM test_worker;"
test_sql $WORKER_USER $WORKER_PASS "Drop table" "DROP TABLE IF EXISTS test_worker;"
test_db_creation $WORKER_USER $WORKER_PASS "true"

# Test Server User (should only be able to read)
echo -e "\n${BLUE}=== SERVER USER TESTS ===${NC}"
test_sql $SERVER_USER $SERVER_PASS "Connect to database" "SELECT 1;"

# Create a test table with master user for server to read
PGPASSWORD=$MASTER_PASS psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $MASTER_USER -d $POSTGRES_DB -c "CREATE TABLE IF NOT EXISTS test_server_read (id INT); INSERT INTO test_server_read VALUES (42);" >/dev/null 2>&1

test_sql $SERVER_USER $SERVER_PASS "Read existing data" "SELECT * FROM test_server_read;"
test_sql $SERVER_USER $SERVER_PASS "Try to create table" "CREATE TABLE test_server_fail (id INT);" "false"
test_sql $SERVER_USER $SERVER_PASS "Try to insert data" "INSERT INTO test_server_read VALUES (2);" "false"
test_db_creation $SERVER_USER $SERVER_PASS "false"

# Clean up
PGPASSWORD=$MASTER_PASS psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $MASTER_USER -d $POSTGRES_DB -c "DROP TABLE IF EXISTS test_server_read;" >/dev/null 2>&1

echo -e "\n${GREEN}🎉 All tests completed!${NC}"
echo -e "\n${YELLOW}📊 Summary:${NC}"
echo "✅ Master User: Full admin access (migrations, database creation, all operations)"
echo "✅ Worker User: Read/write access + database creation (data ingestion)"  
echo "✅ Server User: Read-only access (API serving)"
echo ""
echo -e "${BLUE}Ready to test chainstorage with role-based PostgreSQL!${NC}" 