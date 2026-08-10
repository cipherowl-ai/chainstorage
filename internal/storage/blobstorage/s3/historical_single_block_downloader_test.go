package s3

import (
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/config"
	s3mocks "github.com/coinbase/chainstorage/internal/s3/mocks"
	storageinternal "github.com/coinbase/chainstorage/internal/storage/blobstorage/internal"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestHistoricalSingleBlockDownloaderFactoryRejectsInvalidBucket(t *testing.T) {
	cfg := &config.Config{}
	cfg.AWS.Bucket = "active-blocks"
	factory := newHistoricalSingleBlockDownloaderFactory(BlobStorageParams{
		Params: fxparams.Params{
			Config:  cfg,
			Logger:  zap.NewNop(),
			Metrics: tally.NoopScope,
		},
	})

	tests := []struct {
		name        string
		bucket      string
		expectedErr string
	}{
		{name: "empty", bucket: "", expectedErr: "is required"},
		{name: "surrounding whitespace", bucket: " legacy-blocks ", expectedErr: "surrounding whitespace"},
		{name: "active bucket", bucket: cfg.AWS.Bucket, expectedErr: "must differ from active bucket"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			downloader, err := factory.Create(test.bucket)
			require.Error(t, err)
			require.Nil(t, downloader)
			require.Contains(t, err.Error(), test.expectedErr)
		})
	}
}

func TestHistoricalSingleBlockDownloaderReadsOnlyConfiguredBucket(t *testing.T) {
	const (
		activeBucket     = "active-blocks"
		historicalBucket = "legacy-blocks"
		objectKey        = "BLOCKCHAIN_SOLANA/NETWORK_SOLANA_MAINNET/2/420000000/hash"
	)

	ctrl := gomock.NewController(t)
	downloader := s3mocks.NewMockDownloader(ctrl)
	cfg := &config.Config{}
	cfg.AWS.Bucket = activeBucket
	factory := newHistoricalSingleBlockDownloaderFactory(BlobStorageParams{
		Params: fxparams.Params{
			Config:  cfg,
			Logger:  zap.NewNop(),
			Metrics: tally.NoopScope,
		},
		Downloader: downloader,
	})

	metadata := &api.BlockMetadata{
		Tag:           2,
		Height:        420_000_000,
		Hash:          "hash",
		ObjectKeyMain: objectKey,
		ObjectFormat:  api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK,
	}
	storedBlock := &api.Block{
		Metadata: &api.BlockMetadata{Tag: 2, Height: metadata.Height, Hash: metadata.Hash},
		Blobdata: &api.Block_Solana{Solana: &api.SolanaBlobdata{Header: []byte("header")}},
	}
	payload, err := proto.Marshal(storedBlock)
	require.NoError(t, err)

	downloader.EXPECT().
		Download(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, writer io.WriterAt, input *awss3.GetObjectInput, _ ...func(*manager.Downloader)) (int64, error) {
			require.Equal(t, historicalBucket, *input.Bucket)
			require.Equal(t, objectKey, *input.Key)
			written, writeErr := writer.WriteAt(payload, 0)
			require.NoError(t, writeErr)
			require.Equal(t, len(payload), written)
			return int64(len(payload)), nil
		})

	historical, err := factory.Create(historicalBucket)
	require.NoError(t, err)
	block, err := historical.Download(context.Background(), metadata)
	require.NoError(t, err)
	require.Same(t, metadata, block.Metadata)
	require.True(t, proto.Equal(storedBlock.GetSolana(), block.GetSolana()))

	_, exposesUploads := historical.(storageinternal.SingleBlockUploader)
	require.False(t, exposesUploads)
	_, exposesActiveStorage := historical.(storageinternal.BlobStorage)
	require.False(t, exposesActiveStorage)
}

func TestHistoricalSingleBlockDownloaderRejectsUnsafeMetadataBeforeS3(t *testing.T) {
	ctrl := gomock.NewController(t)
	downloader := s3mocks.NewMockDownloader(ctrl)
	cfg := &config.Config{}
	cfg.AWS.Bucket = "active-blocks"
	factory := newHistoricalSingleBlockDownloaderFactory(BlobStorageParams{
		Params: fxparams.Params{
			Config:  cfg,
			Logger:  zap.NewNop(),
			Metrics: tally.NoopScope,
		},
		Downloader: downloader,
	})
	historical, err := factory.Create("legacy-blocks")
	require.NoError(t, err)

	valid := func() *api.BlockMetadata {
		return &api.BlockMetadata{
			Tag:           2,
			Height:        420_000_000,
			Hash:          "hash",
			ObjectKeyMain: "single-block-key",
			ObjectFormat:  api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK,
		}
	}
	tests := []struct {
		name        string
		metadata    *api.BlockMetadata
		expectedErr string
	}{
		{name: "nil", metadata: nil, expectedErr: "metadata is required"},
		{name: "skipped", metadata: func() *api.BlockMetadata { value := valid(); value.Skipped = true; return value }(), expectedErr: "is skipped"},
		{name: "missing object key", metadata: func() *api.BlockMetadata { value := valid(); value.ObjectKeyMain = ""; return value }(), expectedErr: "object key is required"},
		{name: "consolidated format", metadata: func() *api.BlockMetadata {
			value := valid()
			value.ObjectFormat = api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH
			return value
		}(), expectedErr: "requires single-block format"},
		{name: "byte offset", metadata: func() *api.BlockMetadata { value := valid(); value.ByteOffset = 1; return value }(), expectedErr: "must not contain CSCB placement"},
		{name: "byte length", metadata: func() *api.BlockMetadata { value := valid(); value.ByteLength = 1; return value }(), expectedErr: "must not contain CSCB placement"},
		{name: "uncompressed length", metadata: func() *api.BlockMetadata { value := valid(); value.UncompressedLength = 1; return value }(), expectedErr: "must not contain CSCB placement"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			block, err := historical.Download(context.Background(), test.metadata)
			require.Error(t, err)
			require.Nil(t, block)
			require.Contains(t, err.Error(), test.expectedErr)
		})
	}
}
