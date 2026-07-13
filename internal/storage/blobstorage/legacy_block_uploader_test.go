package blobstorage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"

	storageinternal "github.com/coinbase/chainstorage/internal/storage/blobstorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	metastoragemocks "github.com/coinbase/chainstorage/internal/storage/metastorage/mocks"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type legacyUploadTestRaw struct {
	uploadCalls    int
	uploadRawCalls int
}

func (u *legacyUploadTestRaw) Upload(context.Context, *api.Block, api.Compression) (string, error) {
	u.uploadCalls++
	return "legacy-key", nil
}

func (u *legacyUploadTestRaw) UploadRaw(context.Context, *storageinternal.RawBlockData) (string, error) {
	u.uploadRawCalls++
	return "legacy-raw-key", nil
}

type fencedLegacyUploadTestGuard struct{}

func (fencedLegacyUploadTestGuard) RetirementFenced() bool {
	return true
}

func (fencedLegacyUploadTestGuard) Release() error {
	return nil
}

func TestLegacyBlockUploaderRejectsFencedUploadBeforeRawStorage(t *testing.T) {
	controller := gomock.NewController(t)
	metaStorage := metastoragemocks.NewMockMetaStorage(controller)
	raw := &legacyUploadTestRaw{}
	uploader := &guardedLegacyBlockUploader{raw: raw, metaStorage: metaStorage}
	block := &api.Block{Metadata: &api.BlockMetadata{Tag: 2, Height: 123, Hash: "hash"}}
	metaStorage.EXPECT().AcquireLegacyObjectUploadGuard(gomock.Any(), uint32(2), uint64(123), "hash").
		Return(fencedLegacyUploadTestGuard{}, nil)

	_, err := uploader.Upload(context.Background(), block, api.Compression_GZIP)
	require.ErrorIs(t, err, metastorage.ErrLegacyObjectRetirementFenced)
	require.Zero(t, raw.uploadCalls)
}

func TestLegacyBlockUploaderGuardsRawUpload(t *testing.T) {
	controller := gomock.NewController(t)
	metaStorage := metastoragemocks.NewMockMetaStorage(controller)
	raw := &legacyUploadTestRaw{}
	uploader := &guardedLegacyBlockUploader{raw: raw, metaStorage: metaStorage}
	rawBlock := &RawBlockData{BlockMetadata: &api.BlockMetadata{Tag: 2, Height: 124, Hash: "hash-raw"}}
	metaStorage.EXPECT().AcquireLegacyObjectUploadGuard(gomock.Any(), uint32(2), uint64(124), "hash-raw").
		Return(metastorage.NewUnfencedLegacyObjectUploadGuard(), nil)

	key, err := uploader.UploadRaw(context.Background(), rawBlock)
	require.NoError(t, err)
	require.Equal(t, "legacy-raw-key", key)
	require.Equal(t, 1, raw.uploadRawCalls)
}

func TestSafeBlobStorageDoesNotExposeLegacyUploads(t *testing.T) {
	core := &legacyUploadTestCore{}
	var storage BlobStorage = &safeBlobStorage{storage: core}

	_, exposesRawUpload := storage.(LegacyBlockUploader)
	require.False(t, exposesRawUpload)
}

func TestModuleDoesNotExposeRawBlobStorageFactory(t *testing.T) {
	type rawFactoryParams struct {
		fx.In
		S3 storageinternal.BlobStorageFactory `name:"blobstorage/s3"`
	}

	app := fx.New(
		Module,
		fx.NopLogger,
		fx.Invoke(func(rawFactoryParams) {}),
	)
	require.Error(t, app.Err())
	require.Contains(t, app.Err().Error(), "blobstorage/s3")
}

type legacyUploadTestCore struct {
	legacyUploadTestRaw
}

func (*legacyUploadTestCore) UploadConsolidated(context.Context, []storageinternal.ConsolidatedBlockPayload) (string, []storageinternal.BlockPlacement, error) {
	return "", nil, nil
}

func (*legacyUploadTestCore) Download(context.Context, *api.BlockMetadata) (*api.Block, error) {
	return nil, nil
}

func (*legacyUploadTestCore) DownloadMany(context.Context, []*api.BlockMetadata) ([]*api.Block, error) {
	return nil, nil
}

func (*legacyUploadTestCore) PreSign(context.Context, string) (string, error) {
	return "", nil
}
