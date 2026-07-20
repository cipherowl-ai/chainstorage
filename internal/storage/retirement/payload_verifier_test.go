package retirement

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/storage/blobstorage/cscb"
	storageutils "github.com/coinbase/chainstorage/internal/storage/utils"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestPayloadVerifier_ParsesAndMatchesExactPinnedVersions(t *testing.T) {
	require := require.New(t)
	candidate, store, payload := retirementPayloadFixture(t)
	verifier := newPayloadVerifier(store)

	digest, err := verifier.Verify(context.Background(), candidate)
	require.NoError(err)
	var block api.Block
	require.NoError(proto.Unmarshal(payload, &block))
	expected, err := canonicalBlockDigest(storageutils.CloneBlockWithoutStoragePlacement(&block))
	require.NoError(err)
	require.Equal(expected, digest)

	delete(store.objects, versionObjectKey(candidate.Key, candidate.VersionID))
	digest, err = verifier.VerifyConsolidated(context.Background(), candidate)
	require.NoError(err)
	require.Equal(expected, digest)
}

func TestPayloadVerifier_RejectsCSCBPayloadWithStoragePlacementMetadata(t *testing.T) {
	require := require.New(t)
	candidate, store, payload := retirementPayloadFixture(t)

	var consolidated api.Block
	require.NoError(proto.Unmarshal(payload, &consolidated))
	consolidated.Metadata.ObjectKeyMain = candidate.Key
	consolidated.Metadata.ObjectFormat = api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH
	consolidated.Metadata.ByteOffset = 123
	consolidated.Metadata.ByteLength = 456
	consolidated.Metadata.UncompressedLength = 789
	consolidatedPayload, err := proto.Marshal(&consolidated)
	require.NoError(err)
	candidate.ByteLength = uint64(len(consolidatedPayload))
	candidate.UncompressedLength = uint64(len(consolidatedPayload))
	store.objects[versionObjectKey(candidate.ConsolidatedKey, candidate.CSCBVersionID)] = buildSingleBlockCSCB(t, candidate, consolidatedPayload)

	_, err = newPayloadVerifier(store).Verify(context.Background(), candidate)
	require.Error(err)
	require.Contains(err.Error(), "retains storage placement metadata")

	_, err = newPayloadVerifier(store).VerifyConsolidated(context.Background(), candidate)
	require.Error(err)
	require.Contains(err.Error(), "retains storage placement metadata")
}

func TestPayloadVerifier_FreshInstanceDoesNotReuseCachedCSCBBytes(t *testing.T) {
	require := require.New(t)
	candidate, store, _ := retirementPayloadFixture(t)
	verifier := newPayloadVerifier(store)

	_, err := verifier.VerifyConsolidated(context.Background(), candidate)
	require.NoError(err)
	delete(store.objects, versionObjectKey(candidate.ConsolidatedKey, candidate.CSCBVersionID))

	_, err = verifier.VerifyConsolidated(context.Background(), candidate)
	require.NoError(err, "the same verifier intentionally caches its parsed index and chunk")
	_, err = newPayloadVerifier(store).VerifyConsolidated(context.Background(), candidate)
	require.Error(err)
}

func TestPayloadVerifier_ParsesSupportedSingleBlockCompression(t *testing.T) {
	tests := []struct {
		name        string
		suffix      string
		compression api.Compression
	}{
		{name: "none", compression: api.Compression_NONE},
		{name: "gzip", suffix: storageutils.GzipFileSuffix, compression: api.Compression_GZIP},
		{name: "zstd", suffix: storageutils.ZstdFileSuffix, compression: api.Compression_ZSTD},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			candidate, store, payload := retirementPayloadFixture(t)
			delete(store.objects, versionObjectKey(candidate.Key, candidate.VersionID))
			candidate.Key = "single-block/429600000" + test.suffix
			compressed, err := storageutils.Compress(payload, test.compression)
			require.NoError(err)
			store.objects[versionObjectKey(candidate.Key, candidate.VersionID)] = compressed

			_, err = newPayloadVerifier(store).Verify(context.Background(), candidate)
			require.NoError(err)
		})
	}
}

func TestPayloadDigestsPreservePersistedFormatAndCanonicalizeSemanticComparison(t *testing.T) {
	candidate := Candidate{Tag: 2, Height: 429600000, Hash: "block-hash"}
	first := solanaPayloadBlock(candidate, `{"blockhash":"block-hash","transactions":[],"slot":429600000}`)
	second := solanaPayloadBlock(candidate, "{\n  \"slot\": 429600000,\n  \"transactions\": [],\n  \"blockhash\": \"block-hash\"\n}")
	require.NotEqual(t, first.GetSolana().GetHeader(), second.GetSolana().GetHeader())

	firstPersistedDigest, err := canonicalBlockDigest(first)
	require.NoError(t, err)
	secondPersistedDigest, err := canonicalBlockDigest(second)
	require.NoError(t, err)
	require.NotEqual(t, firstPersistedDigest, secondPersistedDigest)

	firstSemanticDigest, err := semanticBlockDigest(first)
	require.NoError(t, err)
	secondSemanticDigest, err := semanticBlockDigest(second)
	require.NoError(t, err)
	require.Equal(t, firstSemanticDigest, secondSemanticDigest)
}

func TestSemanticBlockDigestCanonicalizesEmptyOptionalSolanaTransactionMetadata(t *testing.T) {
	candidate := Candidate{Tag: 2, Height: 429600000, Hash: "block-hash"}
	emptyArrays := solanaPayloadBlock(candidate, `{
		"blockhash":"block-hash",
		"transactions":[{"meta":{"innerInstructions":[],"logMessages":[]}}]
	}`)
	nulls := solanaPayloadBlock(candidate, `{
		"blockhash":"block-hash",
		"transactions":[{"meta":{"innerInstructions":null,"logMessages":null}}]
	}`)
	omitted := solanaPayloadBlock(candidate, `{
		"blockhash":"block-hash",
		"transactions":[{"meta":{}}]
	}`)

	emptyArrayDigest, err := semanticBlockDigest(emptyArrays)
	require.NoError(t, err)
	nullDigest, err := semanticBlockDigest(nulls)
	require.NoError(t, err)
	omittedDigest, err := semanticBlockDigest(omitted)
	require.NoError(t, err)
	require.Equal(t, emptyArrayDigest, nullDigest)
	require.Equal(t, emptyArrayDigest, omittedDigest)
}

func TestSemanticBlockDigestCanonicalizesSolanaVoteAuthorizeCheckedBLSRendering(t *testing.T) {
	candidate := Candidate{Tag: 2, Height: 431635272, Hash: "block-hash"}
	raw := solanaPayloadBlock(candidate, solanaHeaderWithInstruction(solanaVoteAuthorizeCheckedBLSRawInstruction))
	parsed := solanaPayloadBlock(candidate, solanaHeaderWithInstruction(solanaVoteAuthorizeCheckedBLSParsedInstruction))

	rawPersistedDigest, err := canonicalBlockDigest(raw)
	require.NoError(t, err)
	parsedPersistedDigest, err := canonicalBlockDigest(parsed)
	require.NoError(t, err)
	require.NotEqual(t, rawPersistedDigest, parsedPersistedDigest)

	rawSemanticDigest, err := semanticBlockDigest(raw)
	require.NoError(t, err)
	parsedSemanticDigest, err := semanticBlockDigest(parsed)
	require.NoError(t, err)
	require.Equal(t, rawSemanticDigest, parsedSemanticDigest)
}

func TestSemanticBlockDigestRejectsNonEquivalentSolanaVoteAuthorizeCheckedBLSRendering(t *testing.T) {
	candidate := Candidate{Tag: 2, Height: 431635272, Hash: "block-hash"}
	tests := map[string]struct {
		raw    string
		parsed string
	}{
		"account mismatch": {
			raw:    solanaVoteAuthorizeCheckedBLSRawInstruction,
			parsed: strings.Replace(solanaVoteAuthorizeCheckedBLSParsedInstruction, `"authority":"CpuDNi3iVoHXbaT8gHpzKe6rqeBasoYjEKi21q7NRVJS"`, `"authority":"different"`, 1),
		},
		"BLS public key mismatch": {
			raw:    solanaVoteAuthorizeCheckedBLSRawInstruction,
			parsed: strings.Replace(solanaVoteAuthorizeCheckedBLSParsedInstruction, `"bls_pubkey":[144,`, `"bls_pubkey":[145,`, 1),
		},
		"parsed type mismatch": {
			raw:    solanaVoteAuthorizeCheckedBLSRawInstruction,
			parsed: strings.Replace(solanaVoteAuthorizeCheckedBLSParsedInstruction, `"type":"authorizeChecked"`, `"type":"authorize"`, 1),
		},
		"stack height mismatch": {
			raw:    solanaVoteAuthorizeCheckedBLSRawInstruction,
			parsed: strings.Replace(solanaVoteAuthorizeCheckedBLSParsedInstruction, `"stackHeight":1`, `"stackHeight":2`, 1),
		},
		"unsupported raw discriminator": {
			raw:    strings.Replace(solanaVoteAuthorizeCheckedBLSRawInstruction, `"data":"Hibz`, `"data":"Jibz`, 1),
			parsed: solanaVoteAuthorizeCheckedBLSParsedInstruction,
		},
		"unexpected raw field": {
			raw:    strings.Replace(solanaVoteAuthorizeCheckedBLSRawInstruction, `"stackHeight":1}`, `"stackHeight":1,"unexpected":true}`, 1),
			parsed: solanaVoteAuthorizeCheckedBLSParsedInstruction,
		},
		"non-numeric raw stack height": {
			raw:    strings.Replace(solanaVoteAuthorizeCheckedBLSRawInstruction, `"stackHeight":1`, `"stackHeight":"1"`, 1),
			parsed: solanaVoteAuthorizeCheckedBLSParsedInstruction,
		},
		"oversized raw data": {
			raw:    strings.Replace(solanaVoteAuthorizeCheckedBLSRawInstruction, solanaVoteAuthorizeCheckedBLSRawData, strings.Repeat("1", voteAuthorizeCheckedBLSMaxBase58DataLength+1), 1),
			parsed: solanaVoteAuthorizeCheckedBLSParsedInstruction,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			rawDigest, err := semanticBlockDigest(solanaPayloadBlock(candidate, solanaHeaderWithInstruction(test.raw)))
			require.NoError(t, err)
			parsedDigest, err := semanticBlockDigest(solanaPayloadBlock(candidate, solanaHeaderWithInstruction(test.parsed)))
			require.NoError(t, err)
			require.NotEqual(t, rawDigest, parsedDigest)
		})
	}
}

func TestSemanticBlockDigestPreservesMeaningfulSolanaJSONDifferences(t *testing.T) {
	candidate := Candidate{Tag: 2, Height: 429600000, Hash: "block-hash"}
	tests := map[string]struct {
		first  string
		second string
	}{
		"non-empty log messages": {
			first:  `{"transactions":[{"meta":{"logMessages":["program success"]}}]}`,
			second: `{"transactions":[{"meta":{"logMessages":null}}]}`,
		},
		"non-empty inner instructions": {
			first:  `{"transactions":[{"meta":{"innerInstructions":[{"index":1,"instructions":[]}]}}]}`,
			second: `{"transactions":[{"meta":{"innerInstructions":null}}]}`,
		},
		"unrelated null and empty array": {
			first:  `{"transactions":[],"unknown":[]}`,
			second: `{"transactions":[],"unknown":null}`,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			firstDigest, err := semanticBlockDigest(solanaPayloadBlock(candidate, test.first))
			require.NoError(t, err)
			secondDigest, err := semanticBlockDigest(solanaPayloadBlock(candidate, test.second))
			require.NoError(t, err)
			require.NotEqual(t, firstDigest, secondDigest)
		})
	}
}

func TestSemanticBlockDigestRejectsDifferentSolanaJSON(t *testing.T) {
	candidate := Candidate{Tag: 2, Height: 429600000, Hash: "block-hash"}
	first := solanaPayloadBlock(candidate, `{"blockhash":"block-hash","slot":429600000}`)
	second := solanaPayloadBlock(candidate, `{"blockhash":"block-hash","slot":429600001}`)

	firstDigest, err := semanticBlockDigest(first)
	require.NoError(t, err)
	secondDigest, err := semanticBlockDigest(second)
	require.NoError(t, err)
	require.NotEqual(t, firstDigest, secondDigest)
}

func TestSemanticBlockDigestRejectsMalformedSolanaJSON(t *testing.T) {
	candidate := Candidate{Tag: 2, Height: 429600000, Hash: "block-hash"}
	_, err := semanticBlockDigest(solanaPayloadBlock(candidate, `{"blockhash":`))
	require.ErrorContains(t, err, "failed to parse embedded Solana block JSON")

	_, err = semanticBlockDigest(solanaPayloadBlock(candidate, `{"blockhash":"block-hash"} {}`))
	require.ErrorContains(t, err, "contains multiple values")
}

func TestSemanticBlockDigestRejectsDuplicateSolanaJSONKeys(t *testing.T) {
	candidate := Candidate{Tag: 2, Height: 429600000, Hash: "block-hash"}
	tests := map[string]string{
		"top level": `{"slot":429600000,"slot":429600001}`,
		"nested":    `{"meta":{"slot":429600000,"slot":429600001}}`,
	}
	for name, header := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := semanticBlockDigest(solanaPayloadBlock(candidate, header))
			require.ErrorContains(t, err, `duplicate JSON object key "slot"`)
		})
	}
}

func TestPayloadVerifier_RejectsSemanticAndSerializedMismatch(t *testing.T) {
	require := require.New(t)
	candidate, store, _ := retirementPayloadFixture(t)
	different := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:    candidate.Tag,
			Height: candidate.Height,
			Hash:   candidate.Hash,
		},
	}
	differentPayload, err := proto.Marshal(different)
	require.NoError(err)
	store.objects[versionObjectKey(candidate.Key, candidate.VersionID)] = gzipPayload(t, differentPayload)

	_, err = newPayloadVerifier(store).Verify(context.Background(), candidate)
	require.Error(err)
	require.Contains(err.Error(), "payloads differ")
}

func TestPayloadVerifier_RejectsMatchingPayloadWithWrongBlockIdentity(t *testing.T) {
	require := require.New(t)
	candidate, store, _ := retirementPayloadFixture(t)
	wrong := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:    candidate.Tag,
			Height: candidate.Height + 1,
			Hash:   candidate.Hash,
		},
	}
	wrongPayload, err := proto.Marshal(wrong)
	require.NoError(err)
	store.objects[versionObjectKey(candidate.Key, candidate.VersionID)] = gzipPayload(t, wrongPayload)
	store.objects[versionObjectKey(candidate.ConsolidatedKey, candidate.CSCBVersionID)] = buildSingleBlockCSCB(t, candidate, wrongPayload)

	_, err = newPayloadVerifier(store).Verify(context.Background(), candidate)
	require.Error(err)
	require.Contains(err.Error(), "identity mismatch")
}

func TestPayloadVerifier_RejectsMalformedSingleBlockAndCSCBPayloads(t *testing.T) {
	t.Run("single-block protobuf", func(t *testing.T) {
		require := require.New(t)
		candidate, store, _ := retirementPayloadFixture(t)
		store.objects[versionObjectKey(candidate.Key, candidate.VersionID)] = gzipPayload(t, []byte("not-a-protobuf"))
		_, err := newPayloadVerifier(store).Verify(context.Background(), candidate)
		require.Error(err)
		require.Contains(err.Error(), "parse pinned single-block")
	})

	t.Run("CSCB chunk checksum", func(t *testing.T) {
		require := require.New(t)
		candidate, store, _ := retirementPayloadFixture(t)
		key := versionObjectKey(candidate.ConsolidatedKey, candidate.CSCBVersionID)
		object := append([]byte(nil), store.objects[key]...)
		object[len(object)-1] ^= 0xff
		store.objects[key] = object
		_, err := newPayloadVerifier(store).Verify(context.Background(), candidate)
		require.Error(err)
	})
}

func retirementPayloadFixture(t *testing.T) (Candidate, *fakeStore, []byte) {
	t.Helper()
	require := require.New(t)
	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:    2,
			Height: 429600000,
			Hash:   "solana-hash-429600000",
		},
	}
	payload, err := proto.Marshal(block)
	require.NoError(err)
	candidate := Candidate{
		Bucket:             "bucket",
		Key:                "single-block/429600000.gzip",
		VersionID:          "single-block-v1",
		Height:             block.Metadata.Height,
		Hash:               block.Metadata.Hash,
		BlockMetadataID:    9001,
		Tag:                block.Metadata.Tag,
		ConsolidatedKey:    "consolidated/canary.cscb.gzip",
		CSCBVersionID:      "cscb-v1",
		ByteOffset:         0,
		ByteLength:         uint64(len(payload)),
		UncompressedLength: uint64(len(payload)),
	}
	store := newFakeStore()
	store.objects[versionObjectKey(candidate.Key, candidate.VersionID)] = gzipPayload(t, payload)
	store.objects[versionObjectKey(candidate.ConsolidatedKey, candidate.CSCBVersionID)] = buildSingleBlockCSCB(t, candidate, payload)
	return candidate, store, payload
}

func solanaPayloadBlock(candidate Candidate, header string) *api.Block {
	return &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:    candidate.Tag,
			Height: candidate.Height,
			Hash:   candidate.Hash,
		},
		Blobdata: &api.Block_Solana{
			Solana: &api.SolanaBlobdata{Header: []byte(header)},
		},
	}
}

func solanaHeaderWithInstruction(instruction string) string {
	return `{"transactions":[{"transaction":{"message":{"instructions":[` + instruction + `]}}}]}`
}

const solanaVoteAuthorizeCheckedBLSRawData = `Hibzn6uTxrGAfqM93TpBdjnU15WCMYg6hLtmTcgq2jgx612W4YQxZF4EaT2RVuj2qkGtt4c78UB87YrjZ1Dxisjf5ty6Kzf4qNP7kmb8WSRt93SqLSGMD8o2p9zkPw1iPMuTPFNjo5VsiL4tDpgjNP7iHv6KEHcsKYn39Ep9nK9SgVNysWk353DVyyxGdTEQ3NRBTg5Bz6f6RS7`

const solanaVoteAuthorizeCheckedBLSRawInstruction = `{"accounts":["2QE9X9X4tdDUTYic1DgBBJjU7cWUNPbKYGerCb9KqDQN","SysvarC1ock11111111111111111111111111111111","CpuDNi3iVoHXbaT8gHpzKe6rqeBasoYjEKi21q7NRVJS","CpuDNi3iVoHXbaT8gHpzKe6rqeBasoYjEKi21q7NRVJS"],"data":"` + solanaVoteAuthorizeCheckedBLSRawData + `","programId":"Vote111111111111111111111111111111111111111","stackHeight":1}`

const solanaVoteAuthorizeCheckedBLSParsedInstruction = `{"parsed":{"info":{"authority":"CpuDNi3iVoHXbaT8gHpzKe6rqeBasoYjEKi21q7NRVJS","authorityType":{"VoterWithBLS":{"bls_proof_of_possession":[180,123,132,218,19,74,247,109,172,185,179,183,89,174,66,194,100,162,53,178,158,172,52,137,3,167,252,252,81,135,232,197,100,16,169,222,92,252,231,46,34,13,216,155,132,104,84,154,21,129,53,88,5,58,170,228,211,249,243,2,0,34,47,74,34,84,200,184,141,44,112,46,90,124,225,216,238,193,61,74,194,190,153,82,235,63,132,195,18,156,200,175,0,140,255,152],"bls_pubkey":[144,192,39,209,42,213,86,74,191,12,62,255,177,202,140,100,243,204,42,184,172,91,103,140,240,201,35,55,76,69,51,189,44,54,29,114,245,142,102,181,141,0,134,121,193,3,188,254]}},"clockSysvar":"SysvarC1ock11111111111111111111111111111111","newAuthority":"CpuDNi3iVoHXbaT8gHpzKe6rqeBasoYjEKi21q7NRVJS","voteAccount":"2QE9X9X4tdDUTYic1DgBBJjU7cWUNPbKYGerCb9KqDQN"},"type":"authorizeChecked"},"program":"vote","programId":"Vote111111111111111111111111111111111111111","stackHeight":1}`

func buildSingleBlockCSCB(t *testing.T, candidate Candidate, payload []byte) []byte {
	t.Helper()
	compressed := gzipPayload(t, payload)
	envelope := make([]byte, cscb.EnvelopeHeaderSize+cscb.BlockIndexRecordSize+cscb.ChunkIndexRecordSize)
	copy(envelope[0:4], []byte("ENV1"))
	binary.LittleEndian.PutUint64(envelope[8:16], 1)
	binary.LittleEndian.PutUint64(envelope[16:24], 1)
	binary.LittleEndian.PutUint64(envelope[24:32], candidate.Height)
	binary.LittleEndian.PutUint64(envelope[32:40], candidate.Height+1)
	binary.LittleEndian.PutUint64(envelope[40:48], cscb.EnvelopeHeaderSize)
	binary.LittleEndian.PutUint64(envelope[48:56], cscb.BlockIndexRecordSize)
	chunkOffset := cscb.EnvelopeHeaderSize + cscb.BlockIndexRecordSize
	binary.LittleEndian.PutUint64(envelope[56:64], uint64(chunkOffset))
	binary.LittleEndian.PutUint64(envelope[64:72], cscb.ChunkIndexRecordSize)

	blockRecord := envelope[cscb.EnvelopeHeaderSize:chunkOffset]
	binary.LittleEndian.PutUint64(blockRecord[0:8], candidate.Height)
	binary.LittleEndian.PutUint64(blockRecord[8:16], candidate.ByteOffset)
	binary.LittleEndian.PutUint64(blockRecord[16:24], uint64(len(payload)))
	binary.LittleEndian.PutUint32(blockRecord[24:28], crc32.ChecksumIEEE(payload))
	binary.LittleEndian.PutUint32(blockRecord[28:32], 0)
	binary.LittleEndian.PutUint64(blockRecord[32:40], 0)
	hash := sha256.Sum256([]byte(candidate.Hash))
	copy(blockRecord[48:80], hash[:])
	binary.LittleEndian.PutUint64(blockRecord[80:88], uint64(candidate.BlockMetadataID))

	payloadOffset := uint64(cscb.HeaderSize + len(envelope))
	chunkRecord := envelope[chunkOffset:]
	binary.LittleEndian.PutUint32(chunkRecord[0:4], 0)
	binary.LittleEndian.PutUint64(chunkRecord[8:16], candidate.Height)
	binary.LittleEndian.PutUint64(chunkRecord[16:24], candidate.Height+1)
	binary.LittleEndian.PutUint64(chunkRecord[24:32], payloadOffset)
	binary.LittleEndian.PutUint64(chunkRecord[32:40], uint64(len(compressed)))
	binary.LittleEndian.PutUint64(chunkRecord[40:48], 0)
	binary.LittleEndian.PutUint64(chunkRecord[48:56], uint64(len(payload)))
	binary.LittleEndian.PutUint32(chunkRecord[56:60], crc32.ChecksumIEEE(payload))
	binary.LittleEndian.PutUint32(chunkRecord[60:64], 1)

	header := make([]byte, cscb.HeaderSize)
	copy(header[0:4], []byte("CSCB"))
	header[4] = 1
	header[5] = 1
	header[6] = 2
	binary.LittleEndian.PutUint32(header[8:12], 1)
	binary.LittleEndian.PutUint32(header[12:16], 1)
	binary.LittleEndian.PutUint64(header[16:24], candidate.Height)
	binary.LittleEndian.PutUint64(header[24:32], candidate.Height+1)
	binary.LittleEndian.PutUint64(header[32:40], cscb.HeaderSize)
	binary.LittleEndian.PutUint64(header[40:48], uint64(len(envelope)))
	binary.LittleEndian.PutUint32(header[48:52], crc32.ChecksumIEEE(envelope))
	binary.LittleEndian.PutUint32(header[52:56], cscb.BlockIndexRecordSize)
	binary.LittleEndian.PutUint32(header[56:60], cscb.ChunkIndexRecordSize)
	binary.LittleEndian.PutUint64(header[60:68], payloadOffset)
	binary.LittleEndian.PutUint64(header[68:76], uint64(len(compressed)))
	binary.LittleEndian.PutUint64(header[76:84], uint64(len(payload)))
	binary.LittleEndian.PutUint32(header[84:88], crc32.ChecksumIEEE(payload))

	object := make([]byte, 0, len(header)+len(envelope)+len(compressed))
	object = append(object, header...)
	object = append(object, envelope...)
	object = append(object, compressed...)
	return object
}

func gzipPayload(t *testing.T, payload []byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	_, err := writer.Write(payload)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return buffer.Bytes()
}
