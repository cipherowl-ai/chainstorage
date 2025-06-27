package postgres

import (
	"context"
	"database/sql"
	"embed"

	"github.com/pressly/goose/v3"
	"golang.org/x/xerrors"
)

//go:embed db/migrations/*.sql
var embedMigrations embed.FS

func runMigrations(ctx context.Context, db *sql.DB) error {
	goose.SetBaseFS(embedMigrations)

	if err := goose.SetDialect("postgres"); err != nil {
		return xerrors.Errorf("failed to set goose dialect: %w", err)
	}

	if err := goose.UpContext(ctx, db, "db/migrations"); err != nil {
		return xerrors.Errorf("failed to run migrations: %w", err)
	}

	return nil
}
