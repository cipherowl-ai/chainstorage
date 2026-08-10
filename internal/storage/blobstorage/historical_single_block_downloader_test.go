package blobstorage

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/coinbase/chainstorage/internal/config"
	storageinternal "github.com/coinbase/chainstorage/internal/storage/blobstorage/internal"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type historicalDownloaderTestStorage struct{}

func (*historicalDownloaderTestStorage) UploadConsolidated(context.Context, []ConsolidatedBlockPayload) (string, []BlockPlacement, error) {
	return "", nil, nil
}

func (*historicalDownloaderTestStorage) Download(context.Context, *api.BlockMetadata) (*api.Block, error) {
	return nil, nil
}

func (*historicalDownloaderTestStorage) DownloadMany(context.Context, []*api.BlockMetadata) ([]*api.Block, error) {
	return nil, nil
}

func (*historicalDownloaderTestStorage) PreSign(context.Context, string) (string, error) {
	return "", nil
}

type historicalDownloaderTestFactory struct {
	createdBucket string
	downloader    storageinternal.HistoricalSingleBlockDownloader
	err           error
}

func (f *historicalDownloaderTestFactory) Create(bucket string) (storageinternal.HistoricalSingleBlockDownloader, error) {
	f.createdBucket = bucket
	return f.downloader, f.err
}

func TestHistoricalSingleBlockDownloaderDefaultsToActiveStorage(t *testing.T) {
	cfg := &config.Config{}
	active := &historicalDownloaderTestStorage{}
	factory := &historicalDownloaderTestFactory{}

	downloader, err := withHistoricalSingleBlockDownloader(historicalSingleBlockDownloaderParams{
		Params:      fxparams.Params{Config: cfg},
		BlobStorage: active,
		S3Factory:   factory,
	})
	require.NoError(t, err)
	require.Same(t, active, downloader)
	require.Empty(t, factory.createdBucket)
}

func TestHistoricalSingleBlockDownloaderUsesConfiguredS3Source(t *testing.T) {
	const sourceBucket = "legacy-solana-blocks"

	cfg := &config.Config{}
	cfg.StorageType.BlobStorageType = config.BlobStorageType_S3
	cfg.AWS.Storage.Consolidation.HistoricalSourceBucket = sourceBucket
	active := &historicalDownloaderTestStorage{}
	historical := &historicalDownloaderTestStorage{}
	factory := &historicalDownloaderTestFactory{downloader: historical}

	downloader, err := withHistoricalSingleBlockDownloader(historicalSingleBlockDownloaderParams{
		Params:      fxparams.Params{Config: cfg},
		BlobStorage: active,
		S3Factory:   factory,
	})
	require.NoError(t, err)
	require.Same(t, historical, downloader)
	require.Equal(t, sourceBucket, factory.createdBucket)
}

func TestHistoricalSingleBlockDownloaderRejectsNonS3Source(t *testing.T) {
	cfg := &config.Config{}
	cfg.StorageType.BlobStorageType = config.BlobStorageType_GCS
	cfg.AWS.Storage.Consolidation.HistoricalSourceBucket = "legacy-solana-blocks"
	factory := &historicalDownloaderTestFactory{}

	downloader, err := withHistoricalSingleBlockDownloader(historicalSingleBlockDownloaderParams{
		Params:      fxparams.Params{Config: cfg},
		BlobStorage: &historicalDownloaderTestStorage{},
		S3Factory:   factory,
	})
	require.Error(t, err)
	require.Nil(t, downloader)
	require.Contains(t, err.Error(), "requires S3 blob storage")
	require.Empty(t, factory.createdBucket)
}

func TestHistoricalSingleBlockDownloaderWrapsFactoryFailure(t *testing.T) {
	cfg := &config.Config{}
	cfg.StorageType.BlobStorageType = config.BlobStorageType_S3
	cfg.AWS.Storage.Consolidation.HistoricalSourceBucket = "legacy-solana-blocks"
	factoryErr := errors.New("factory failed")
	factory := &historicalDownloaderTestFactory{err: factoryErr}

	downloader, err := withHistoricalSingleBlockDownloader(historicalSingleBlockDownloaderParams{
		Params:      fxparams.Params{Config: cfg},
		BlobStorage: &historicalDownloaderTestStorage{},
		S3Factory:   factory,
	})
	require.ErrorIs(t, err, factoryErr)
	require.Nil(t, downloader)
	require.Contains(t, err.Error(), "failed to create historical single-block downloader")
}
