package main

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
)

type recordingMigrationExecer struct {
	queries []string
	failAt  int
}

func (e *recordingMigrationExecer) ExecContext(_ context.Context, query string, _ ...interface{}) (sql.Result, error) {
	e.queries = append(e.queries, query)
	if e.failAt >= 0 && len(e.queries)-1 == e.failAt {
		return nil, errors.New("injected grant failure")
	}
	return nil, nil
}

func TestCollectDBMigrationsIncludesTimestampMigrations(t *testing.T) {
	goose.SetBaseFS(postgres.GetEmbeddedMigrations())
	t.Cleanup(func() {
		goose.SetBaseFS(nil)
	})

	migrations, err := collectDBMigrations()
	require.NoError(t, err)

	var hasRetirementMigration bool
	for _, migration := range migrations {
		if migration.Version == 20260703000001 {
			hasRetirementMigration = true
			break
		}
	}

	require.True(t, hasRetirementMigration)
}

func TestPendingConcurrentIndexNamesOnlyIncludesPendingMigrations(t *testing.T) {
	goose.SetBaseFS(postgres.GetEmbeddedMigrations())
	t.Cleanup(func() {
		goose.SetBaseFS(nil)
	})

	indexNames, err := pendingConcurrentIndexNames(20260714000001)
	require.NoError(t, err)
	require.Equal(t, []string{
		"idx_block_consolidation_shadow_object_key_reference",
		"idx_block_metadata_cscb_repair_candidate",
		"idx_block_metadata_object_key_reference",
	}, indexNames)

	indexNames, err = pendingConcurrentIndexNames(maxMigrationVersion)
	require.NoError(t, err)
	require.Empty(t, indexNames)
}

func TestRunDBMigrateRequiresRuntimeUsers(t *testing.T) {
	err := runDBMigrate(
		"master",
		"password",
		"",
		"server",
		"localhost",
		5432,
		"chainstorage_ethereum_mainnet",
		"disable",
		30*time.Second,
		time.Hour,
		time.Minute,
		false,
	)
	require.ErrorContains(t, err, "--worker-user and --server-user are required")
}

func TestMigrationPrivilegeQueriesCoverMasterOwnedObjects(t *testing.T) {
	queries := migrationPrivilegeQueries("master-role", "worker-role", "server-role", "chainstorage-db")

	require.Len(t, queries, 16)
	require.Contains(t, queries, `GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "worker-role"`)
	require.Contains(t, queries, `GRANT SELECT ON ALL TABLES IN SCHEMA public TO "server-role"`)
	require.Contains(t, queries, `ALTER DEFAULT PRIVILEGES FOR USER "master-role" IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO "worker-role"`)
	require.Contains(t, queries, `ALTER DEFAULT PRIVILEGES FOR USER "master-role" IN SCHEMA public GRANT SELECT ON TABLES TO "server-role"`)
	require.Contains(t, queries, `ALTER DEFAULT PRIVILEGES FOR USER "worker-role" IN SCHEMA public GRANT SELECT ON TABLES TO "server-role"`)
}

func TestGrantMigrationPrivilegesFailsClosed(t *testing.T) {
	db := &recordingMigrationExecer{failAt: 4}

	err := grantMigrationPrivileges(context.Background(), db, "master", "worker", "server", "chainstorage")

	require.ErrorContains(t, err, "injected grant failure")
	require.Len(t, db.queries, 5)
}

func TestEnsureMigrationRoleMembershipQuotesRoleNames(t *testing.T) {
	db := &recordingMigrationExecer{failAt: -1}

	err := ensureMigrationRoleMembership(context.Background(), db, `master"role`, `worker"role`)

	require.NoError(t, err)
	require.Equal(t, []string{`GRANT "worker""role" TO "master""role"`}, db.queries)
}

func TestRunMigrationStepsFailsClosedOnPrivilegeGrant(t *testing.T) {
	db := &recordingMigrationExecer{failAt: 1}
	migrated := false

	_, err := runMigrationSteps(
		context.Background(),
		db,
		"master",
		"worker",
		"server",
		"chainstorage",
		func() (int64, error) {
			migrated = true
			return 42, nil
		},
	)

	require.True(t, migrated)
	require.ErrorContains(t, err, "failed to reconcile runtime role privileges")
	require.ErrorContains(t, err, "injected grant failure")
	require.Equal(t, []string{
		`GRANT "worker" TO "master"`,
		`GRANT ALL PRIVILEGES ON DATABASE "chainstorage" TO "worker"`,
	}, db.queries)
}
