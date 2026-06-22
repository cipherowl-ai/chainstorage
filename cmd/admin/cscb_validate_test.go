package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestBuildCSCBValidationCasesIncludesRequiredCoverage(t *testing.T) {
	require := require.New(t)
	cfg := cscbValidationRunConfig{
		startHeight:          1000,
		endHeight:            1100,
		chunkBlocks:          25,
		randomSamples:        2,
		randomSeed:           914,
		iterations:           3,
		fullObjectIterations: 1,
	}

	cases := buildCSCBValidationCases(cfg, 999)
	names := make(map[string]cscbValidationCase, len(cases))
	for _, validationCase := range cases {
		names[validationCase.Name] = validationCase
	}

	require.Contains(names, "single_first_block")
	require.Contains(names, "single_last_block")
	require.Contains(names, "range_within_one_chunk")
	require.Contains(names, "range_across_chunk_boundary")
	require.Contains(names, "range_full_object")
	require.Contains(names, "single_consolidated_miss_fallback")
	require.True(names["single_consolidated_miss_fallback"].ExpectFallback)
	require.Equal(1, names["range_full_object"].Iterations)
	require.Equal(3, names["range_across_chunk_boundary"].Iterations)
	require.NotEmpty(chunkBoundaryHeights(1000, 1100, 25))
	require.Len(randomSampleHeights(1000, 1100, 2, 914), 2)
}

func TestCompareCSCBBlocksIgnoresSourceSpecificMetadata(t *testing.T) {
	require := require.New(t)
	legacy := &api.Block{
		Metadata: &api.BlockMetadata{
			Tag:           1,
			Height:        42,
			Hash:          "hash",
			ParentHash:    "parent",
			ObjectKeyMain: "legacy/object",
		},
	}
	consolidated := &api.Block{
		Metadata: &api.BlockMetadata{
			Tag:                1,
			Height:             42,
			Hash:               "hash",
			ParentHash:         "parent",
			ObjectFormat:       api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			ByteOffset:         1024,
			ByteLength:         2048,
			UncompressedLength: 4096,
		},
	}

	comparison := compareCSCBBlocks([]*api.Block{legacy}, []*api.Block{consolidated})
	require.True(comparison.correct)
	require.Empty(comparison.mismatchHeights)
	require.Equal(1, comparison.hashesCompared)
	require.Equal(comparison.firstLegacyHash, comparison.firstConsolidatedHash)
}

func TestCompareCSCBBlocksDetectsMismatch(t *testing.T) {
	require := require.New(t)
	legacy := &api.Block{
		Metadata: &api.BlockMetadata{Height: 42, Hash: "hash"},
	}
	consolidated := &api.Block{
		Metadata: &api.BlockMetadata{Height: 42, Hash: "different"},
	}

	comparison := compareCSCBBlocks([]*api.Block{legacy}, []*api.Block{consolidated})
	require.False(comparison.correct)
	require.Equal([]uint64{42}, comparison.mismatchHeights)
}

func TestSummarizeDurationsUsesNearestRankPercentile(t *testing.T) {
	require := require.New(t)
	summary := summarizeDurations([]time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		30 * time.Millisecond,
		40 * time.Millisecond,
		50 * time.Millisecond,
	})

	require.Equal(5, summary.Samples)
	require.Equal(float64(30), summary.P50Ms)
	require.Equal(float64(50), summary.P95Ms)
	require.Equal(float64(10), summary.MinMs)
	require.Equal(float64(50), summary.MaxMs)
}

func TestSanitizeCSCBStringRedactsEmbeddedURLs(t *testing.T) {
	require := require.New(t)
	errText := `Download failed: blockFile={FileUrl:"https://bucket.s3.amazonaws.com/key?X-Amz-Signature=secret", Height:42}`

	sanitized := sanitizeCSCBString(errText)
	require.NotContains(sanitized, "X-Amz-Signature")
	require.NotContains(sanitized, "secret")
	require.Contains(sanitized, "<redacted-url>")
}
