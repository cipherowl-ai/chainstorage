package s3

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/xerrors"

	chains3 "github.com/coinbase/chainstorage/internal/s3"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/internal"
	storageerrors "github.com/coinbase/chainstorage/internal/storage/internal/errors"
	storage_utils "github.com/coinbase/chainstorage/internal/storage/utils"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	historicalSingleBlockDownloaderFactory struct {
		params BlobStorageParams
	}

	historicalSingleBlockDownloader struct {
		bucket             string
		downloader         chains3.Downloader
		blobStorageMetrics *blobStorageMetrics
		instrumentDownload instrument.InstrumentWithResult[*api.Block]
	}
)

var (
	_ internal.HistoricalSingleBlockDownloaderFactory = (*historicalSingleBlockDownloaderFactory)(nil)
	_ internal.HistoricalSingleBlockDownloader        = (*historicalSingleBlockDownloader)(nil)
)

func newHistoricalSingleBlockDownloaderFactory(params BlobStorageParams) internal.HistoricalSingleBlockDownloaderFactory {
	return &historicalSingleBlockDownloaderFactory{params: params}
}

func (f *historicalSingleBlockDownloaderFactory) Create(bucket string) (internal.HistoricalSingleBlockDownloader, error) {
	if bucket == "" {
		return nil, xerrors.New("historical source bucket is required")
	}
	if strings.TrimSpace(bucket) != bucket {
		return nil, xerrors.New("historical source bucket must not contain surrounding whitespace")
	}
	if bucket == f.params.Config.AWS.Bucket {
		return nil, xerrors.New("historical source bucket must differ from active bucket")
	}

	metrics := f.params.Metrics.SubScope("blob_storage").Tagged(map[string]string{
		"storage_type": "s3",
		"source":       "historical_single_block",
	})
	return &historicalSingleBlockDownloader{
		bucket:             bucket,
		downloader:         f.params.Downloader,
		blobStorageMetrics: newBlobStorageMetrics(metrics),
		instrumentDownload: instrument.NewWithResult[*api.Block](metrics, "download"),
	}, nil
}

func (d *historicalSingleBlockDownloader) Download(ctx context.Context, metadata *api.BlockMetadata) (*api.Block, error) {
	return d.instrumentDownload.Instrument(ctx, func(ctx context.Context) (*api.Block, error) {
		if err := validateHistoricalSingleBlockMetadata(metadata); err != nil {
			return nil, err
		}
		return downloadSingleBlockFromBucket(ctx, d.bucket, d.downloader, d.blobStorageMetrics, metadata)
	})
}

func validateHistoricalSingleBlockMetadata(metadata *api.BlockMetadata) error {
	if metadata == nil {
		return xerrors.New("historical single-block metadata is required")
	}
	if metadata.GetSkipped() {
		return xerrors.Errorf("historical single-block metadata is skipped at height %d", metadata.GetHeight())
	}
	if metadata.GetObjectKeyMain() == "" {
		return xerrors.Errorf("historical single-block object key is required at height %d", metadata.GetHeight())
	}
	if metadata.GetObjectFormat() != api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK {
		return xerrors.Errorf(
			"historical source requires single-block format at height %d, got %s",
			metadata.GetHeight(),
			metadata.GetObjectFormat(),
		)
	}
	if metadata.GetByteOffset() != 0 || metadata.GetByteLength() != 0 || metadata.GetUncompressedLength() != 0 {
		return xerrors.Errorf(
			"historical single-block metadata must not contain CSCB placement at height %d: offset=%d length=%d uncompressed_length=%d",
			metadata.GetHeight(),
			metadata.GetByteOffset(),
			metadata.GetByteLength(),
			metadata.GetUncompressedLength(),
		)
	}
	return nil
}

func downloadSingleBlockFromBucket(
	ctx context.Context,
	bucket string,
	downloader chains3.Downloader,
	metrics *blobStorageMetrics,
	metadata *api.BlockMetadata,
) (*api.Block, error) {
	key := metadata.GetObjectKeyMain()
	buf := manager.NewWriteAtBuffer([]byte{})

	size, err := downloader.Download(ctx, buf, &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, storageerrors.ErrRequestCanceled
		}
		return nil, xerrors.Errorf("failed to download from s3 (bucket=%s, key=%s): %w", bucket, key, err)
	}

	// Record bytes through the existing blob-storage size timer.
	metrics.blobDownloadedSize.Record(time.Duration(size) * time.Millisecond)

	compression := storage_utils.GetCompressionType(key)
	blockData, err := storage_utils.Decompress(buf.Bytes(), compression)
	if err != nil {
		return nil, xerrors.Errorf("failed to decompress block data with type %v: %w", compression.String(), err)
	}

	return unmarshalBlockData(bucket, key, metadata, blockData)
}
