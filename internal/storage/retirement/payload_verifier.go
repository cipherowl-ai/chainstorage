package retirement

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"strconv"

	"github.com/mr-tron/base58"
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

type (
	PinnedPayloadInspection struct {
		CanonicalSHA256     string
		SemanticSHA256      string
		HasStoragePlacement bool
	}

	PinnedPayloadVerifier interface {
		ReadSingleBlock(ctx context.Context, candidate Candidate) (*api.Block, PinnedPayloadInspection, error)
		InspectSingleBlock(ctx context.Context, candidate Candidate) (PinnedPayloadInspection, error)
		InspectConsolidated(ctx context.Context, candidate Candidate) (PinnedPayloadInspection, error)
	}
)

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

func NewPinnedPayloadVerifier(store ObjectStore) PinnedPayloadVerifier {
	return newPayloadVerifier(store)
}

func (v *payloadVerifier) Verify(ctx context.Context, candidate Candidate) (string, error) {
	singleBlockInspection, err := v.InspectSingleBlock(ctx, candidate)
	if err != nil {
		return "", err
	}
	consolidatedInspection, err := v.InspectConsolidated(ctx, candidate)
	if err != nil {
		return "", err
	}
	if consolidatedInspection.HasStoragePlacement {
		return "", xerrors.Errorf("pinned CSCB block payload retains storage placement metadata at height %d", candidate.Height)
	}
	if singleBlockInspection.CanonicalSHA256 != consolidatedInspection.CanonicalSHA256 {
		return "", xerrors.Errorf("single-block and CSCB block payloads differ at height %d", candidate.Height)
	}
	return singleBlockInspection.CanonicalSHA256, nil
}

func (v *payloadVerifier) InspectSingleBlock(ctx context.Context, candidate Candidate) (PinnedPayloadInspection, error) {
	_, inspection, err := v.ReadSingleBlock(ctx, candidate)
	return inspection, err
}

func (v *payloadVerifier) ReadSingleBlock(
	ctx context.Context,
	candidate Candidate,
) (*api.Block, PinnedPayloadInspection, error) {
	singleBlockCompressed, err := v.store.ReadObjectVersion(ctx, candidate.Bucket, candidate.Key, candidate.VersionID)
	if err != nil {
		return nil, PinnedPayloadInspection{}, err
	}
	singleBlockPayload, err := storageutils.Decompress(singleBlockCompressed, storageutils.GetCompressionType(candidate.Key))
	if err != nil {
		return nil, PinnedPayloadInspection{}, xerrors.Errorf("failed to decompress pinned single-block object: %w", err)
	}
	var singleBlockBlock api.Block
	if err := proto.Unmarshal(singleBlockPayload, &singleBlockBlock); err != nil {
		return nil, PinnedPayloadInspection{}, xerrors.Errorf("failed to parse pinned single-block block payload: %w", err)
	}
	if err := validatePayloadIdentity(&singleBlockBlock, candidate); err != nil {
		return nil, PinnedPayloadInspection{}, xerrors.Errorf("pinned single-block block identity mismatch: %w", err)
	}
	normalized := storageutils.CloneBlockWithoutStoragePlacement(&singleBlockBlock)
	digest, err := canonicalBlockDigest(normalized)
	if err != nil {
		return nil, PinnedPayloadInspection{}, err
	}
	semanticDigest, err := semanticBlockDigest(normalized)
	if err != nil {
		return nil, PinnedPayloadInspection{}, err
	}
	return &singleBlockBlock, PinnedPayloadInspection{
		CanonicalSHA256:     digest,
		SemanticSHA256:      semanticDigest,
		HasStoragePlacement: storageutils.HasBlockStoragePlacement(&singleBlockBlock),
	}, nil
}

func (v *payloadVerifier) InspectConsolidated(ctx context.Context, candidate Candidate) (PinnedPayloadInspection, error) {
	_, cscbBlock, err := v.readConsolidatedPayload(ctx, candidate)
	if err != nil {
		return PinnedPayloadInspection{}, err
	}
	if err := validatePayloadIdentity(cscbBlock, candidate); err != nil {
		return PinnedPayloadInspection{}, xerrors.Errorf("pinned CSCB block identity mismatch: %w", err)
	}
	normalized := storageutils.CloneBlockWithoutStoragePlacement(cscbBlock)
	digest, err := canonicalBlockDigest(normalized)
	if err != nil {
		return PinnedPayloadInspection{}, err
	}
	semanticDigest, err := semanticBlockDigest(normalized)
	if err != nil {
		return PinnedPayloadInspection{}, err
	}
	return PinnedPayloadInspection{
		CanonicalSHA256:     digest,
		SemanticSHA256:      semanticDigest,
		HasStoragePlacement: storageutils.HasBlockStoragePlacement(cscbBlock),
	}, nil
}

func (v *payloadVerifier) VerifyConsolidated(ctx context.Context, candidate Candidate) (string, error) {
	inspection, err := v.InspectConsolidated(ctx, candidate)
	if err != nil {
		return "", err
	}
	if inspection.HasStoragePlacement {
		return "", xerrors.Errorf("pinned CSCB block payload retains storage placement metadata at height %d", candidate.Height)
	}
	return inspection.CanonicalSHA256, nil
}

func canonicalBlockDigest(block *api.Block) (string, error) {
	// This digest is persisted in retirement and repair manifests. Keep its
	// deterministic-protobuf algorithm stable across upgrades and rollbacks.
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(block)
	if err != nil {
		return "", xerrors.Errorf("failed to marshal canonical block payload: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func semanticBlockDigest(block *api.Block) (string, error) {
	canonical, err := canonicalizeEmbeddedBlockPayload(block)
	if err != nil {
		return "", err
	}
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(canonical)
	if err != nil {
		return "", xerrors.Errorf("failed to marshal canonical block payload: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func canonicalizeEmbeddedBlockPayload(block *api.Block) (*api.Block, error) {
	solana := block.GetSolana()
	if solana == nil || len(solana.Header) == 0 {
		return block, nil
	}

	decoder := json.NewDecoder(bytes.NewReader(solana.Header))
	decoder.UseNumber()
	value, err := decodeUniqueJSONValue(decoder)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse embedded Solana block JSON: %w", err)
	}
	if token, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return nil, xerrors.Errorf("embedded Solana block JSON contains multiple values: trailing=%v", token)
		}
		return nil, xerrors.Errorf("failed to finish parsing embedded Solana block JSON: %w", err)
	}
	normalizeSolanaOptionalTransactionMetadata(value)
	normalizeSolanaVoteAuthorizeCheckedBLSInstructions(value)
	canonicalHeader, err := json.Marshal(value)
	if err != nil {
		return nil, xerrors.Errorf("failed to canonicalize embedded Solana block JSON: %w", err)
	}
	canonical := proto.Clone(block).(*api.Block)
	canonical.GetSolana().Header = canonicalHeader
	return canonical, nil
}

func normalizeSolanaVoteAuthorizeCheckedBLSInstructions(value any) {
	const (
		voteProgramID                        = "Vote111111111111111111111111111111111111111"
		voteAuthorizeChecked                 = uint32(7)
		voteAuthorizeVoterWithBLS            = uint32(2)
		voteAuthorizeCheckedBLSPayloadLength = 8 + 48 + 96
	)

	block, ok := value.(map[string]any)
	if !ok {
		return
	}
	transactions, ok := block["transactions"].([]any)
	if !ok {
		return
	}
	for _, transactionValue := range transactions {
		transaction, ok := transactionValue.(map[string]any)
		if !ok {
			continue
		}
		transactionBody, ok := transaction["transaction"].(map[string]any)
		if !ok {
			continue
		}
		message, ok := transactionBody["message"].(map[string]any)
		if !ok {
			continue
		}
		instructions, ok := message["instructions"].([]any)
		if !ok {
			continue
		}
		for index, instructionValue := range instructions {
			instruction, ok := instructionValue.(map[string]any)
			if !ok || len(instruction) != 4 || instruction["programId"] != voteProgramID {
				continue
			}
			stackHeight, hasStackHeight := instruction["stackHeight"]
			accounts, accountsOK := solanaInstructionAccounts(instruction["accounts"], 4)
			data, dataOK := instruction["data"].(string)
			if !hasStackHeight || !validSolanaStackHeight(stackHeight) || !accountsOK || !dataOK {
				continue
			}
			decoded, err := base58.Decode(data)
			if err != nil || len(decoded) != voteAuthorizeCheckedBLSPayloadLength {
				continue
			}
			// Older RPC nodes emitted the raw bincode instruction before their
			// JSON parser learned the VoterWithBLS authorization variant.
			if binary.LittleEndian.Uint32(decoded[0:4]) != voteAuthorizeChecked || binary.LittleEndian.Uint32(decoded[4:8]) != voteAuthorizeVoterWithBLS {
				continue
			}
			authorityType := map[string]any{
				"VoterWithBLS": map[string]any{
					"bls_pubkey":              solanaByteNumbers(decoded[8:56]),
					"bls_proof_of_possession": solanaByteNumbers(decoded[56:voteAuthorizeCheckedBLSPayloadLength]),
				},
			}
			instructions[index] = map[string]any{
				"parsed": map[string]any{
					"info": map[string]any{
						"authority":     accounts[2],
						"authorityType": authorityType,
						"clockSysvar":   accounts[1],
						"newAuthority":  accounts[3],
						"voteAccount":   accounts[0],
					},
					"type": "authorizeChecked",
				},
				"program":     "vote",
				"programId":   voteProgramID,
				"stackHeight": stackHeight,
			}
		}
	}
}

func solanaInstructionAccounts(value any, expectedLength int) ([]string, bool) {
	items, ok := value.([]any)
	if !ok || len(items) != expectedLength {
		return nil, false
	}
	accounts := make([]string, len(items))
	for index, item := range items {
		account, ok := item.(string)
		if !ok {
			return nil, false
		}
		accounts[index] = account
	}
	return accounts, true
}

func validSolanaStackHeight(value any) bool {
	if value == nil {
		return true
	}
	number, ok := value.(json.Number)
	if !ok {
		return false
	}
	_, err := strconv.ParseUint(string(number), 10, 32)
	return err == nil
}

func solanaByteNumbers(value []byte) []any {
	numbers := make([]any, len(value))
	for index, item := range value {
		numbers[index] = json.Number(strconv.FormatUint(uint64(item), 10))
	}
	return numbers
}

func normalizeSolanaOptionalTransactionMetadata(value any) {
	// Solana RPC providers omit these optional collections or emit them as []
	// or null. Chainstorage's native parser treats all three as zero entries.
	block, ok := value.(map[string]any)
	if !ok {
		return
	}
	transactions, ok := block["transactions"].([]any)
	if !ok {
		return
	}
	for _, transactionValue := range transactions {
		transaction, ok := transactionValue.(map[string]any)
		if !ok {
			continue
		}
		metadata, ok := transaction["meta"].(map[string]any)
		if !ok {
			continue
		}
		for _, field := range []string{"innerInstructions", "logMessages"} {
			value, exists := metadata[field]
			items, isArray := value.([]any)
			if !exists || value == nil || (isArray && len(items) == 0) {
				metadata[field] = nil
			}
		}
	}
}

func decodeUniqueJSONValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return token, nil
	}

	switch delim {
	case '{':
		object := make(map[string]any)
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, xerrors.Errorf("JSON object key is not a string: %v", keyToken)
			}
			if _, exists := object[key]; exists {
				return nil, xerrors.Errorf("duplicate JSON object key %q", key)
			}
			value, err := decodeUniqueJSONValue(decoder)
			if err != nil {
				return nil, err
			}
			object[key] = value
		}
		if err := requireJSONClosingDelimiter(decoder, '}'); err != nil {
			return nil, err
		}
		return object, nil
	case '[':
		array := make([]any, 0)
		for decoder.More() {
			value, err := decodeUniqueJSONValue(decoder)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		if err := requireJSONClosingDelimiter(decoder, ']'); err != nil {
			return nil, err
		}
		return array, nil
	default:
		return nil, xerrors.Errorf("unexpected JSON delimiter %q", delim)
	}
}

func requireJSONClosingDelimiter(decoder *json.Decoder, expected json.Delim) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token != expected {
		return xerrors.Errorf("unexpected JSON closing delimiter %v; expected %q", token, expected)
	}
	return nil
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
