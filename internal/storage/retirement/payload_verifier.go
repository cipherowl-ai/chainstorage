package retirement

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"

	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/storage/blobstorage/cscb"
	storageutils "github.com/coinbase/chainstorage/internal/storage/utils"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const retirementInitialIndexReadSize = 64 * 1024

type blockPayloadVerifier interface {
	Verify(ctx context.Context, candidate Candidate) (string, error)
	VerifyConsolidated(ctx context.Context, candidate Candidate) (string, error)
}

type payloadVerifier struct {
	store ObjectStore

	indexes       map[string]*cscb.Index
	lastChunkKey  string
	lastChunkData []byte
}

func newPayloadVerifier(store ObjectStore) *payloadVerifier {
	return &payloadVerifier{
		store:   store,
		indexes: make(map[string]*cscb.Index),
	}
}

func (v *payloadVerifier) Verify(ctx context.Context, candidate Candidate) (string, error) {
	legacyCompressed, err := v.store.ReadObjectVersion(ctx, candidate.Bucket, candidate.Key, candidate.VersionID)
	if err != nil {
		return "", err
	}
	legacyPayload, err := storageutils.Decompress(legacyCompressed, storageutils.GetCompressionType(candidate.Key))
	if err != nil {
		return "", xerrors.Errorf("failed to decompress pinned legacy object: %w", err)
	}
	var legacyBlock api.Block
	if err := proto.Unmarshal(legacyPayload, &legacyBlock); err != nil {
		return "", xerrors.Errorf("failed to parse pinned legacy block payload: %w", err)
	}
	if err := validatePayloadIdentity(&legacyBlock, candidate); err != nil {
		return "", xerrors.Errorf("pinned legacy block identity mismatch: %w", err)
	}

	_, cscbBlock, err := v.readConsolidatedPayload(ctx, candidate)
	if err != nil {
		return "", err
	}
	if err := validatePayloadIdentity(cscbBlock, candidate); err != nil {
		return "", xerrors.Errorf("pinned CSCB block identity mismatch: %w", err)
	}
	legacyComparable := normalizeStoragePlacement(&legacyBlock)
	cscbComparable := normalizeStoragePlacement(cscbBlock)
	if !proto.Equal(legacyComparable, cscbComparable) {
		return "", xerrors.Errorf("legacy and CSCB block payloads differ at height %d", candidate.Height)
	}
	legacyDigest, err := canonicalBlockDigest(legacyComparable)
	if err != nil {
		return "", err
	}
	cscbDigest, err := canonicalBlockDigest(cscbComparable)
	if err != nil {
		return "", err
	}
	if legacyDigest != cscbDigest {
		return "", xerrors.Errorf("legacy and CSCB canonical payload digests differ at height %d", candidate.Height)
	}
	return legacyDigest, nil
}

func (v *payloadVerifier) VerifyConsolidated(ctx context.Context, candidate Candidate) (string, error) {
	_, block, err := v.readConsolidatedPayload(ctx, candidate)
	if err != nil {
		return "", err
	}
	if err := validatePayloadIdentity(block, candidate); err != nil {
		return "", xerrors.Errorf("pinned CSCB block identity mismatch: %w", err)
	}
	return canonicalBlockDigest(normalizeStoragePlacement(block))
}

func normalizeStoragePlacement(block *api.Block) *api.Block {
	clone := proto.Clone(block).(*api.Block)
	if clone.Metadata == nil {
		return clone
	}
	clone.Metadata.ObjectKeyMain = ""
	clone.Metadata.ByteOffset = 0
	clone.Metadata.ByteLength = 0
	clone.Metadata.UncompressedLength = 0
	clone.Metadata.ObjectFormat = api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_LEGACY_SINGLE_BLOCK
	return clone
}

func canonicalBlockDigest(block *api.Block) (string, error) {
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(block)
	if err != nil {
		return "", xerrors.Errorf("failed to marshal canonical block payload: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func validatePayloadIdentity(block *api.Block, candidate Candidate) error {
	metadata := block.GetMetadata()
	if metadata == nil {
		return xerrors.New("block metadata is missing")
	}
	if metadata.GetTag() != candidate.Tag || metadata.GetHeight() != candidate.Height || metadata.GetHash() != candidate.Hash {
		return xerrors.Errorf(
			"got tag=%d height=%d hash=%q; want tag=%d height=%d hash=%q",
			metadata.GetTag(),
			metadata.GetHeight(),
			metadata.GetHash(),
			candidate.Tag,
			candidate.Height,
			candidate.Hash,
		)
	}
	return nil
}

func (v *payloadVerifier) readConsolidatedPayload(ctx context.Context, candidate Candidate) ([]byte, *api.Block, error) {
	index, err := v.readIndex(ctx, candidate)
	if err != nil {
		return nil, nil, err
	}
	block, chunk, err := index.LookupBlock(&api.BlockMetadata{
		Tag:                candidate.Tag,
		Height:             candidate.Height,
		Hash:               candidate.Hash,
		ObjectFormat:       api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		ByteOffset:         candidate.ByteOffset,
		ByteLength:         candidate.ByteLength,
		UncompressedLength: candidate.UncompressedLength,
	})
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to resolve pinned CSCB placement: %w", err)
	}
	chunkData, err := v.readChunk(ctx, candidate, index, chunk)
	if err != nil {
		return nil, nil, err
	}
	payload, err := cscb.ExtractBlockPayload(chunkData, block)
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to extract pinned CSCB block payload: %w", err)
	}
	var parsed api.Block
	if err := proto.Unmarshal(payload, &parsed); err != nil {
		return nil, nil, xerrors.Errorf("failed to parse pinned CSCB block payload: %w", err)
	}
	return payload, &parsed, nil
}

func (v *payloadVerifier) readIndex(ctx context.Context, candidate Candidate) (*cscb.Index, error) {
	cacheKey := candidate.Bucket + "\x00" + candidate.ConsolidatedKey + "\x00" + candidate.CSCBVersionID
	if index, ok := v.indexes[cacheKey]; ok {
		return index, nil
	}
	first, err := v.store.ReadObjectVersionRange(
		ctx,
		candidate.Bucket,
		candidate.ConsolidatedKey,
		candidate.CSCBVersionID,
		0,
		retirementInitialIndexReadSize,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to read pinned CSCB index prefix: %w", err)
	}
	required, err := cscb.HeaderEnvelopeLength(first)
	if err != nil {
		return nil, err
	}
	data := first
	if required > uint64(len(first)) {
		remaining, err := v.store.ReadObjectVersionRange(
			ctx,
			candidate.Bucket,
			candidate.ConsolidatedKey,
			candidate.CSCBVersionID,
			uint64(len(first)),
			required-uint64(len(first)),
		)
		if err != nil {
			return nil, xerrors.Errorf("failed to read pinned CSCB index remainder: %w", err)
		}
		data = make([]byte, 0, required)
		data = append(data, first...)
		data = append(data, remaining...)
	}
	index, err := cscb.ParseIndex(data)
	if err != nil {
		return nil, err
	}
	v.indexes[cacheKey] = index
	return index, nil
}

func (v *payloadVerifier) readChunk(
	ctx context.Context,
	candidate Candidate,
	index *cscb.Index,
	chunk *cscb.ChunkDescriptor,
) ([]byte, error) {
	cacheKey := candidate.Bucket + "\x00" + candidate.ConsolidatedKey + "\x00" + candidate.CSCBVersionID + "\x00" + strconv.FormatUint(uint64(chunk.Index), 10)
	if cacheKey == v.lastChunkKey {
		return v.lastChunkData, nil
	}
	frame, err := v.store.ReadObjectVersionRange(
		ctx,
		candidate.Bucket,
		candidate.ConsolidatedKey,
		candidate.CSCBVersionID,
		chunk.CompressedPayloadOffset,
		chunk.CompressedLength,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to read pinned CSCB chunk: %w", err)
	}
	data, err := cscb.DecodeChunkFrame(bytes.NewReader(frame), index.Header.Codec)
	if err != nil {
		return nil, err
	}
	if err := cscb.ValidateChunkPayload(data, chunk); err != nil {
		return nil, err
	}
	v.lastChunkKey = cacheKey
	v.lastChunkData = data
	return data, nil
}
