package postgres

import (
	"context"

	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
)

type (
	metaStorageImpl struct {
		internal.BlockStorage
		internal.EventStorage
		internal.TransactionStorage
	}

	Params struct {
		fx.In
		fxparams.Params
	}

	metaStorageFactory struct {
		params Params
	}
)

func NewMetaStorage(params Params) (internal.Result, error) {
	// Validate Postgres config when actually creating the storage
	if err := validatePostgresConfig(params.Config.AWS.Postgres); err != nil {
		return internal.Result{}, xerrors.Errorf("invalid postgres config: %w", err)
	}

	// Use shared connection pool instead of creating new connections
	pool, err := GetConnectionPool(context.Background(), params.Config.AWS.Postgres)
	if err != nil {
		return internal.Result{}, err
	}

	db := pool.DB()
	if db == nil {
		return internal.Result{}, xerrors.New("connection pool returned nil database connection")
	}
	// Create storage implementations with database connection
	blockStorage, err := newBlockStorage(db, params)
	if err != nil {
		return internal.Result{}, err
	}

	eventStorage, err := newEventStorage(db, params)
	if err != nil {
		return internal.Result{}, err
	}

	transactionStorage, err := newTransactionStorage(db, params)
	if err != nil {
		return internal.Result{}, err
	}

	// Combine into meta storage
	metaStorage := &metaStorageImpl{
		BlockStorage:       blockStorage,
		EventStorage:       eventStorage,
		TransactionStorage: transactionStorage,
	}

	return internal.Result{
		BlockStorage:       blockStorage,
		EventStorage:       eventStorage,
		TransactionStorage: transactionStorage,
		MetaStorage:        metaStorage,
	}, nil
}

func (f *metaStorageFactory) Create() (internal.Result, error) {
	return NewMetaStorage(f.params)
}

func NewFactory(params Params) internal.MetaStorageFactory {
	return &metaStorageFactory{params}
}

// validatePostgresConfig validates required postgres fields when actually using postgres storage
func validatePostgresConfig(cfg *config.PostgresConfig) error {
	if cfg == nil {
		return xerrors.New("postgres config is nil")
	}
	if cfg.Host == "" {
		return xerrors.New("postgres host is required")
	}
	if cfg.Port == 0 {
		return xerrors.New("postgres port is required")
	}
	if cfg.Database == "" {
		return xerrors.New("postgres database is required")
	}
	if cfg.SSLMode == "" {
		return xerrors.New("postgres ssl_mode is required")
	}
	return nil
}
