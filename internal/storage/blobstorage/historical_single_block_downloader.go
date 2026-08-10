package blobstorage

import (
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	storageinternal "github.com/coinbase/chainstorage/internal/storage/blobstorage/internal"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
)

type historicalSingleBlockDownloaderParams struct {
	fx.In
	fxparams.Params
	BlobStorage BlobStorage
	S3Factory   storageinternal.HistoricalSingleBlockDownloaderFactory `name:"blobstorage/s3/historical-single-block-downloader"`
}

func withHistoricalSingleBlockDownloader(params historicalSingleBlockDownloaderParams) (HistoricalSingleBlockDownloader, error) {
	sourceBucket := params.Config.AWS.Storage.Consolidation.HistoricalSourceBucket
	if sourceBucket == "" {
		return params.BlobStorage, nil
	}

	switch params.Config.StorageType.BlobStorageType {
	case config.BlobStorageType_UNSPECIFIED, config.BlobStorageType_S3:
	default:
		return nil, xerrors.Errorf(
			"historical single-block downloader requires S3 blob storage, got %v",
			params.Config.StorageType.BlobStorageType,
		)
	}

	downloader, err := params.S3Factory.Create(sourceBucket)
	if err != nil {
		return nil, xerrors.Errorf("failed to create historical single-block downloader: %w", err)
	}
	return downloader, nil
}
