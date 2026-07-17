package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestIntegrationMigrationRoleMembershipAllowsWorkerOwnedDDL(t *testing.T) {
	if os.Getenv("TEST_TYPE") != "integration" {
		t.Skip("integration test")
	}

	host := getEnvOrDefault("CHAINSTORAGE_AWS_POSTGRES_HOST", "localhost")
	if host != "localhost" && host != "127.0.0.1" && host != "::1" && host != "postgres" {
		t.Fatalf("refusing to run migration ownership test against PostgreSQL host %q", host)
	}

	port := 5433
	if value := os.Getenv("CHAINSTORAGE_AWS_POSTGRES_PORT"); value != "" {
		parsed, err := strconv.Atoi(value)
		require.NoError(t, err)
		port = parsed
	}

	masterUser := getEnvOrDefault("CHAINSTORAGE_AWS_POSTGRES_USER", "postgres")
	masterPassword := getEnvOrDefault("CHAINSTORAGE_AWS_POSTGRES_PASSWORD", "postgres")
	workerUser := "cs_solana_mainnet_worker"
	workerPassword := getEnvOrDefault("CHAINSTORAGE_WORKER_PASSWORD", "worker_password")
	dbName := "chainstorage_solana_mainnet"

	unique := time.Now().UnixNano()
	migrationUser := fmt.Sprintf("migration_admin_test_%d", unique)
	migrationPassword := fmt.Sprintf("migration-password-%d", unique)
	tableName := fmt.Sprintf("migration_owner_test_%d", unique)
	indexName := fmt.Sprintf("migration_owner_idx_%d", unique)

	masterDB := openIntegrationPostgres(t, host, port, "postgres", masterUser, masterPassword)
	workerDB := openIntegrationPostgres(t, host, port, dbName, workerUser, workerPassword)

	createRole := fmt.Sprintf(
		"CREATE ROLE %s WITH LOGIN INHERIT NOSUPERUSER CREATEDB CREATEROLE PASSWORD %s",
		pq.QuoteIdentifier(migrationUser),
		pq.QuoteLiteral(migrationPassword),
	)
	_, err := masterDB.ExecContext(context.Background(), createRole)
	require.NoError(t, err)

	migrationDB := openIntegrationPostgres(t, host, port, dbName, migrationUser, migrationPassword)
	t.Cleanup(func() {
		_ = migrationDB.Close()
		_, _ = workerDB.ExecContext(
			context.Background(),
			"DROP TABLE IF EXISTS public."+pq.QuoteIdentifier(tableName),
		)
		_, _ = masterDB.ExecContext(
			context.Background(),
			fmt.Sprintf("REVOKE %s FROM %s", pq.QuoteIdentifier(workerUser), pq.QuoteIdentifier(migrationUser)),
		)
		_, _ = masterDB.ExecContext(
			context.Background(),
			"DROP ROLE IF EXISTS "+pq.QuoteIdentifier(migrationUser),
		)
		_ = workerDB.Close()
		_ = masterDB.Close()
	})

	_, err = workerDB.ExecContext(
		context.Background(),
		"CREATE TABLE public."+pq.QuoteIdentifier(tableName)+" (id BIGINT)",
	)
	require.NoError(t, err)

	createIndex := fmt.Sprintf(
		"CREATE INDEX %s ON public.%s (id)",
		pq.QuoteIdentifier(indexName),
		pq.QuoteIdentifier(tableName),
	)
	_, err = migrationDB.ExecContext(context.Background(), createIndex)
	require.Error(t, err, "a non-owner migration role must not alter a worker-owned table")

	require.NoError(t, ensureMigrationRoleMembership(
		context.Background(),
		migrationDB,
		migrationUser,
		workerUser,
	))
	_, err = migrationDB.ExecContext(context.Background(), createIndex)
	require.NoError(t, err, "worker-role membership must authorize owner-only migration DDL")
}

func openIntegrationPostgres(t *testing.T, host string, port int, dbName, user, password string) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=require connect_timeout=10",
		host,
		port,
		dbName,
		user,
		password,
	)
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(context.Background()))
	return db
}
