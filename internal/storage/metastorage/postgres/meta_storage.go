package postgres

import (
	"context"

	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"go.uber.org/fx"
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
	db, err := newDBConnection(context.Background(), &params.Config.AWS.Postgres)
	if err != nil {
		return internal.Result{}, err
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
