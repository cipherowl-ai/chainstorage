package main

import (
	"testing"

	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
)

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
