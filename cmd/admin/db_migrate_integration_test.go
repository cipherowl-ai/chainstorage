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

	unique := time.Now().UnixNano()
	workerUser := fmt.Sprintf("migration_worker_test_%d", unique)
	workerPassword := fmt.Sprintf("worker-password-%d", unique)
	serverUser := fmt.Sprintf("migration_server_test_%d", unique)
	serverPassword := fmt.Sprintf("server-password-%d", unique)
	migrationUser := fmt.Sprintf("migration_admin_test_%d", unique)
	migrationPassword := fmt.Sprintf("migration-password-%d", unique)
	dbName := fmt.Sprintf("migration_database_test_%d", unique)
	workerTable := "worker_owned_before_migration"
	adminTable := "admin_owned_during_migration"
	workerFutureTable := "worker_owned_after_migration"
	adminFutureTable := "admin_owned_after_migration"
	adminSequence := "admin_owned_sequence"
	triggerFunction := "admin_owned_trigger_function"
	triggerName := "admin_owned_trigger"

	masterDB := openIntegrationPostgres(t, host, port, "postgres", masterUser, masterPassword)
	var workerDB *sql.DB
	var serverDB *sql.DB
	var migrationDB *sql.DB
	t.Cleanup(func() {
		if serverDB != nil {
			_ = serverDB.Close()
		}
		if workerDB != nil {
			_ = workerDB.Close()
		}
		if migrationDB != nil {
			_ = migrationDB.Close()
		}
		_, _ = masterDB.ExecContext(
			context.Background(),
			"DROP DATABASE IF EXISTS "+pq.QuoteIdentifier(dbName),
		)
		_, _ = masterDB.ExecContext(
			context.Background(),
			fmt.Sprintf("REVOKE %s FROM %s", pq.QuoteIdentifier(workerUser), pq.QuoteIdentifier(migrationUser)),
		)
		_, _ = masterDB.ExecContext(
			context.Background(),
			"DROP ROLE IF EXISTS "+pq.QuoteIdentifier(migrationUser),
		)
		_, _ = masterDB.ExecContext(
			context.Background(),
			"DROP ROLE IF EXISTS "+pq.QuoteIdentifier(serverUser),
		)
		_, _ = masterDB.ExecContext(
			context.Background(),
			"DROP ROLE IF EXISTS "+pq.QuoteIdentifier(workerUser),
		)
		_ = masterDB.Close()
	})

	createWorkerRole := fmt.Sprintf(
		"CREATE ROLE %s WITH LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %s",
		pq.QuoteIdentifier(workerUser),
		pq.QuoteLiteral(workerPassword),
	)
	_, err := masterDB.ExecContext(context.Background(), createWorkerRole)
	require.NoError(t, err)

	createServerRole := fmt.Sprintf(
		"CREATE ROLE %s WITH LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %s",
		pq.QuoteIdentifier(serverUser),
		pq.QuoteLiteral(serverPassword),
	)
	_, err = masterDB.ExecContext(context.Background(), createServerRole)
	require.NoError(t, err)

	createRole := fmt.Sprintf(
		"CREATE ROLE %s WITH LOGIN INHERIT NOSUPERUSER CREATEDB CREATEROLE PASSWORD %s",
		pq.QuoteIdentifier(migrationUser),
		pq.QuoteLiteral(migrationPassword),
	)
	_, err = masterDB.ExecContext(context.Background(), createRole)
	require.NoError(t, err)

	_, err = masterDB.ExecContext(
		context.Background(),
		fmt.Sprintf(
			"CREATE DATABASE %s OWNER %s",
			pq.QuoteIdentifier(dbName),
			pq.QuoteIdentifier(workerUser),
		),
	)
	require.NoError(t, err)

	workerDB = openIntegrationPostgres(t, host, port, dbName, workerUser, workerPassword)
	migrationDB = openIntegrationPostgres(t, host, port, dbName, migrationUser, migrationPassword)
	serverDB = openIntegrationPostgres(t, host, port, dbName, serverUser, serverPassword)

	_, err = workerDB.ExecContext(
		context.Background(),
		fmt.Sprintf(
			"CREATE TABLE public.%s (id BIGINT, touched BOOLEAN NOT NULL DEFAULT FALSE)",
			pq.QuoteIdentifier(workerTable),
		),
	)
	require.NoError(t, err)

	alterWorkerTable := fmt.Sprintf(
		"ALTER TABLE public.%s ADD COLUMN migrated BOOLEAN NOT NULL DEFAULT TRUE",
		pq.QuoteIdentifier(workerTable),
	)
	_, err = migrationDB.ExecContext(context.Background(), alterWorkerTable)
	require.Error(t, err, "a non-owner migration role must not alter a worker-owned table")

	require.NoError(t, ensureMigrationRoleMembership(
		context.Background(),
		migrationDB,
		migrationUser,
		workerUser,
	))
	_, err = migrationDB.ExecContext(context.Background(), alterWorkerTable)
	require.NoError(t, err, "worker-role membership must authorize owner-only migration DDL")

	_, err = migrationDB.ExecContext(
		context.Background(),
		fmt.Sprintf("CREATE TABLE public.%s (id BIGINT)", pq.QuoteIdentifier(adminTable)),
	)
	require.NoError(t, err, "the migration role must create tables in the worker-owned database")
	_, err = migrationDB.ExecContext(
		context.Background(),
		fmt.Sprintf("CREATE SEQUENCE public.%s", pq.QuoteIdentifier(adminSequence)),
	)
	require.NoError(t, err, "the migration role must create sequences")
	_, err = migrationDB.ExecContext(
		context.Background(),
		fmt.Sprintf(`
CREATE FUNCTION public.%s()
RETURNS TRIGGER AS $$
BEGIN
    NEW.touched := TRUE;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql`, pq.QuoteIdentifier(triggerFunction)),
	)
	require.NoError(t, err, "the migration role must create trigger functions")
	_, err = migrationDB.ExecContext(
		context.Background(),
		fmt.Sprintf(
			"CREATE TRIGGER %s BEFORE INSERT ON public.%s FOR EACH ROW EXECUTE FUNCTION public.%s()",
			pq.QuoteIdentifier(triggerName),
			pq.QuoteIdentifier(workerTable),
			pq.QuoteIdentifier(triggerFunction),
		),
	)
	require.NoError(t, err, "the migration role must create triggers on worker-owned tables")

	require.NoError(t, grantMigrationPrivileges(
		context.Background(),
		migrationDB,
		migrationUser,
		workerUser,
		serverUser,
		dbName,
	))

	_, err = workerDB.ExecContext(
		context.Background(),
		fmt.Sprintf("INSERT INTO public.%s (id) VALUES (1)", pq.QuoteIdentifier(adminTable)),
	)
	require.NoError(t, err, "worker must receive write access to migration-created tables")
	_, err = workerDB.ExecContext(
		context.Background(),
		fmt.Sprintf("SELECT nextval('public.%s')", pq.QuoteIdentifier(adminSequence)),
	)
	require.NoError(t, err, "worker must receive usage access to migration-created sequences")
	_, err = workerDB.ExecContext(
		context.Background(),
		fmt.Sprintf("INSERT INTO public.%s (id) VALUES (1)", pq.QuoteIdentifier(workerTable)),
	)
	require.NoError(t, err)
	var touched bool
	require.NoError(t, workerDB.QueryRowContext(
		context.Background(),
		fmt.Sprintf("SELECT touched FROM public.%s WHERE id = 1", pq.QuoteIdentifier(workerTable)),
	).Scan(&touched))
	require.True(t, touched, "the migration-created trigger must run for the worker role")

	_, err = workerDB.ExecContext(
		context.Background(),
		fmt.Sprintf("CREATE TABLE public.%s (id BIGINT)", pq.QuoteIdentifier(workerFutureTable)),
	)
	require.NoError(t, err)
	_, err = migrationDB.ExecContext(
		context.Background(),
		fmt.Sprintf("CREATE TABLE public.%s (id BIGINT)", pq.QuoteIdentifier(adminFutureTable)),
	)
	require.NoError(t, err)

	for _, tableName := range []string{workerTable, adminTable, workerFutureTable, adminFutureTable} {
		_, err = serverDB.ExecContext(
			context.Background(),
			fmt.Sprintf("SELECT * FROM public.%s LIMIT 1", pq.QuoteIdentifier(tableName)),
		)
		require.NoError(t, err, "server must receive read access to %s", tableName)
	}
	_, err = serverDB.ExecContext(
		context.Background(),
		fmt.Sprintf("INSERT INTO public.%s (id) VALUES (2)", pq.QuoteIdentifier(adminFutureTable)),
	)
	require.Error(t, err, "server must remain read-only")
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
