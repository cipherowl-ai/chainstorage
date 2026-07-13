package metastorage

import (
	"context"
	"errors"

	"golang.org/x/xerrors"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

var ErrLegacyObjectRetirementFenced = errors.New("legacy object upload blocked by retirement fence")

type legacyObjectUploadStorage interface {
	GetBlockByHash(ctx context.Context, tag uint32, height uint64, blockHash string) (*api.BlockMetadata, error)
	LegacyObjectUploadGuardStorage
}

// UploadLegacyBlockObject serializes a legacy single-block PUT with retirement
// preparation. Storage implementations must return a non-nil guard even when
// canonical metadata does not exist yet.
func UploadLegacyBlockObject(
	ctx context.Context,
	metadataStorage legacyObjectUploadStorage,
	metadata *api.BlockMetadata,
	upload func() (string, error),
) (objectKey string, err error) {
	if metadata == nil {
		return "", xerrors.New("block metadata is required for guarded upload")
	}
	if metadataStorage == nil {
		return "", xerrors.New("metadata storage is required for guarded upload")
	}
	guard, err := metadataStorage.AcquireLegacyObjectUploadGuard(ctx, metadata.Tag, metadata.Height, metadata.Hash)
	if err != nil {
		return "", err
	}
	if guard == nil {
		return "", xerrors.New("legacy object upload guard is required")
	}
	defer func() {
		if releaseErr := guard.Release(); releaseErr != nil {
			err = errors.Join(err, releaseErr)
		}
	}()

	if !guard.RetirementFenced() {
		objectKey, err = upload()
		return objectKey, err
	}
	return "", xerrors.Errorf(
		"%w: tag=%d height=%d hash=%s",
		ErrLegacyObjectRetirementFenced,
		metadata.Tag,
		metadata.Height,
		metadata.Hash,
	)
}
