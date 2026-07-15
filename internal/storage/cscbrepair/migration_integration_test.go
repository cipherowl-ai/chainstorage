package cscbrepair_test

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/coinbase/chainstorage/internal/config"
	metapostgres "github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

func TestIntegrationCSCBRepairReferenceIndexMigration(t *testing.T) {
	if os.Getenv("TEST_TYPE") != "integration" {
		t.Skip("integration test")
	}
	require := require.New(t)
	ctx := context.Background()

	cfg, err := config.New(
		config.WithEnvironment(config.EnvLocal),
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_SOLANA),
		config.WithNetwork(common.Network_NETWORK_SOLANA_MAINNET),
	)
	require.NoError(err)
	require.NotNil(cfg.AWS.Postgres)
	configureRepairTestEnvironment(t, cfg.AWS.Postgres)

	db, err := openRepairDB(ctx, cfg.AWS.Postgres)
	require.NoError(err)
	defer func() { _ = db.Close() }()

	goose.SetBaseFS(metapostgres.GetEmbeddedMigrations())
	require.NoError(goose.SetDialect("postgres"))
	require.NoError(goose.UpContext(ctx, db, "db/migrations"))
	requireReferenceIndexes(t, db, "hash")
	requireReferenceIndexPlans(t, db)

	// Rewind only the index migration and recreate the shape left by the
	// original B-tree migration. Applying the migration again must replace it,
	// just as it replaces an invalid concurrent index left by a failed build.
	require.NoError(goose.DownToContext(ctx, db, "db/migrations", 20260714000001))
	_, err = db.ExecContext(ctx, `
		CREATE INDEX idx_block_metadata_object_key_reference
		ON block_metadata (object_key_main, id)
		WHERE object_key_main IS NOT NULL AND object_key_main <> '';

		CREATE INDEX idx_block_consolidation_shadow_object_key_reference
		ON block_consolidation_shadow (consolidated_object_key_main, block_metadata_id)
		WHERE consolidated_object_key_main IS NOT NULL AND consolidated_object_key_main <> ''`)
	require.NoError(err)
	requireReferenceIndexes(t, db, "btree")

	require.NoError(goose.UpContext(ctx, db, "db/migrations"))
	requireReferenceIndexes(t, db, "hash")
	requireReferenceIndexPlans(t, db)
}

func requireReferenceIndexes(t *testing.T, db *sql.DB, expectedMethod string) {
	t.Helper()
	indexes := []struct {
		name   string
		column string
	}{
		{name: "idx_block_metadata_object_key_reference", column: "object_key_main"},
		{name: "idx_block_consolidation_shadow_object_key_reference", column: "consolidated_object_key_main"},
	}
	for _, index := range indexes {
		requireReferenceIndex(t, db, index.name, index.column, expectedMethod)
	}
}

func requireReferenceIndex(t *testing.T, db *sql.DB, indexName string, column string, expectedMethod string) {
	t.Helper()
	var method, definition string
	var valid, ready bool
	err := db.QueryRow(`
		SELECT access_method.amname, index_state.indisvalid, index_state.indisready,
			pg_get_indexdef(index_state.indexrelid)
		FROM pg_index index_state
		JOIN pg_class index_class ON index_class.oid = index_state.indexrelid
		JOIN pg_namespace index_namespace ON index_namespace.oid = index_class.relnamespace
		JOIN pg_am access_method ON access_method.oid = index_class.relam
		WHERE index_namespace.nspname = current_schema()
			AND index_class.relname = $1`, indexName).Scan(
		&method,
		&valid,
		&ready,
		&definition,
	)
	require.NoError(t, err)
	require.Equal(t, expectedMethod, method)
	require.True(t, valid)
	require.True(t, ready)
	if expectedMethod == "hash" {
		require.Contains(t, definition, "USING hash ("+column+")")
	}
}

func requireReferenceIndexPlans(t *testing.T, db *sql.DB) {
	t.Helper()
	requireReferenceIndexPlan(t, db, `
		SELECT COUNT(*)
		FROM block_metadata
		WHERE object_key_main = 'consolidated/test.cscb'
			AND object_key_main <> ''
			AND NOT (id = ANY(ARRAY[1]::BIGINT[]))`, "idx_block_metadata_object_key_reference")
	requireReferenceIndexPlan(t, db, `
		SELECT COUNT(*)
		FROM block_consolidation_shadow
		WHERE consolidated_object_key_main = 'consolidated/test.cscb'
			AND consolidated_object_key_main <> ''
			AND NOT (block_metadata_id = ANY(ARRAY[1]::BIGINT[]))`, "idx_block_consolidation_shadow_object_key_reference")
}

func requireReferenceIndexPlan(t *testing.T, db *sql.DB, query string, indexName string) {
	t.Helper()
	tx, err := db.Begin()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.Exec(`SET LOCAL enable_seqscan = off`)
	require.NoError(t, err)

	rows, err := tx.Query("EXPLAIN (COSTS OFF) " + query)
	require.NoError(t, err)
	defer rows.Close()

	var plan strings.Builder
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		plan.WriteString(line)
		plan.WriteByte('\n')
	}
	require.NoError(t, rows.Err())
	require.Contains(t, plan.String(), indexName)
}
