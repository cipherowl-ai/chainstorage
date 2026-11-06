package main

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	parityTestFirstHeight = uint64(1000)
	parityTestHeights     = uint64(300)
	parityTestTipHeight   = parityTestFirstHeight + parityTestHeights - 1
)

// fakeParityStore implements the reads the checker makes. Its embedded interface is nil, so any
// write panics, which also pins the command as read-only.
type fakeParityStore struct {
	metastorage.MetaStorage
	blocks map[uint64]*api.BlockMetadata
	events map[int64]*model.EventEntry
	latest *api.BlockMetadata
	maxId  int64
	reads  atomic.Int64
}

func newFakeParityStore() *fakeParityStore {
	store := &fakeParityStore{
		blocks: make(map[uint64]*api.BlockMetadata),
		events: make(map[int64]*model.EventEntry),
	}
	for height := parityTestFirstHeight; height <= parityTestTipHeight; height++ {
		block := parityTestBlock(height, "a")
		store.blocks[height] = block
		store.maxId++
		store.events[store.maxId] = parityTestEvent(store.maxId, block)
	}
	store.latest = store.blocks[parityTestTipHeight]
	return store
}

func parityTestBlock(height uint64, fork string) *api.BlockMetadata {
	return &api.BlockMetadata{
		Tag:           1,
		Height:        height,
		Hash:          fmt.Sprintf("0x%d%s", height, fork),
		ParentHash:    fmt.Sprintf("0x%d%s", height-1, fork),
		ParentHeight:  height - 1,
		ObjectKeyMain: fmt.Sprintf("1/%d/0x%d%s", height, height, fork),
		Timestamp:     &timestamppb.Timestamp{Seconds: int64(height)},
	}
}

func parityTestEvent(id int64, block *api.BlockMetadata) *model.EventEntry {
	return &model.EventEntry{
		EventId:     id,
		EventType:   api.BlockchainEvent_BLOCK_ADDED,
		BlockHeight: block.Height,
		BlockHash:   block.Hash,
		ParentHash:  block.ParentHash,
		Tag:         block.Tag,
	}
}

// grow extends the store by count blocks that the other store does not have.
func (s *fakeParityStore) grow(count uint64) {
	for height := s.latest.Height + 1; count > 0; count-- {
		block := parityTestBlock(height, "a")
		s.blocks[height] = block
		s.maxId++
		s.events[s.maxId] = parityTestEvent(s.maxId, block)
		s.latest = block
		height++
	}
}

func (s *fakeParityStore) GetMaxEventId(ctx context.Context, eventTag uint32) (int64, error) {
	return s.maxId, nil
}

func (s *fakeParityStore) GetLatestBlock(ctx context.Context, tag uint32) (*api.BlockMetadata, error) {
	if s.latest == nil {
		return nil, xerrors.Errorf("no latest block: %w", storage.ErrItemNotFound)
	}
	return s.latest, nil
}

func (s *fakeParityStore) GetEventsByEventIdRange(ctx context.Context, eventTag uint32, minEventId int64, maxEventId int64) ([]*model.EventEntry, error) {
	s.reads.Add(1)
	events := make([]*model.EventEntry, 0, maxEventId-minEventId)
	for id := minEventId; id < maxEventId; id++ {
		event, ok := s.events[id]
		if !ok {
			return nil, xerrors.Errorf("event %d: %w", id, storage.ErrItemNotFound)
		}
		events = append(events, event)
	}
	return events, nil
}

func (s *fakeParityStore) GetBlocksByHeightRange(ctx context.Context, tag uint32, startHeight, endHeight uint64) ([]*api.BlockMetadata, error) {
	s.reads.Add(1)
	blocks := make([]*api.BlockMetadata, 0, endHeight-startHeight)
	for height := startHeight; height < endHeight; height++ {
		block, ok := s.blocks[height]
		if !ok {
			return nil, xerrors.Errorf("height %d: %w", height, storage.ErrItemNotFound)
		}
		blocks = append(blocks, block)
	}
	if err := parser.ValidateChain(blocks, nil); err != nil {
		return nil, xerrors.Errorf("failed to validate chain: %w", err)
	}
	return blocks, nil
}

func (s *fakeParityStore) GetBlocksByHeights(ctx context.Context, tag uint32, heights []uint64) ([]*api.BlockMetadata, error) {
	s.reads.Add(1)
	blocks := make([]*api.BlockMetadata, len(heights))
	for i, height := range heights {
		block, ok := s.blocks[height]
		if !ok {
			return nil, xerrors.Errorf("height %d: %w", height, storage.ErrItemNotFound)
		}
		blocks[i] = block
	}
	return blocks, nil
}

func parityTestOptions() migrateParityOptions {
	return migrateParityOptions{
		tag:              1,
		eventTag:         1,
		blockStartHeight: parityTestFirstHeight,
		blockSamples:     parityTestHeights,
		eventSamples:     parityTestHeights,
		seed:             defaultParitySeed,
		walk:             true,
		walkChunkSize:    50,
		walkParallelism:  2,
	}
}

func runParity(t *testing.T, source, dest *fakeParityStore, options migrateParityOptions) *migrateParityReport {
	t.Helper()
	report, err := newMigrateParityChecker(source, dest, options).run(context.Background())
	require.NoError(t, err)
	return report
}

func TestMigrateParity_IdenticalStoresPass(t *testing.T) {
	require := require.New(t)

	report := runParity(t, newFakeParityStore(), newFakeParityStore(), parityTestOptions())

	require.True(report.Passed, "%v", report.Problems)
	require.Equal(parityTestHeights, report.HeightsWalked)
	require.Equal(parityTestHeights, report.EventsWalked)
	require.Equal(parityTestHeights, report.BlocksCompared)
	require.Equal(parityTestHeights, report.EventsCompared)
	require.Equal(parityTestTipHeight, report.Postgres.TipHeight)
	require.Equal(int64(parityTestHeights), report.DynamoDB.MaxEventId)
	require.Equal(parityTestFirstHeight, report.FirstHeight)
}

func TestMigrateParity_FirstEventAboveBlockStartHeight(t *testing.T) {
	require := require.New(t)

	options := parityTestOptions()
	options.blockStartHeight = parityTestFirstHeight - 1
	report := runParity(t, newFakeParityStore(), newFakeParityStore(), options)

	require.False(report.Passed)
	require.Equal(1, report.ProblemCount)
	require.Contains(report.Problems[0], "nothing below it was copied")
}

func TestMigrateParity_DetectsDifferentBlockFields(t *testing.T) {
	height := parityTestFirstHeight + 10
	for field, change := range map[string]func(block *api.BlockMetadata){
		"hash":            func(block *api.BlockMetadata) { block.Hash = "0xother" },
		"parent_hash":     func(block *api.BlockMetadata) { block.ParentHash = "0xother" },
		"parent_height":   func(block *api.BlockMetadata) { block.ParentHeight = 1 },
		"object_key_main": func(block *api.BlockMetadata) { block.ObjectKeyMain = "other" },
		"timestamp":       func(block *api.BlockMetadata) { block.Timestamp = &timestamppb.Timestamp{Seconds: 1} },
		"skipped":         func(block *api.BlockMetadata) { block.Skipped = true },
	} {
		t.Run(field, func(t *testing.T) {
			require := require.New(t)
			source, dest := newFakeParityStore(), newFakeParityStore()
			block := proto.Clone(dest.blocks[height]).(*api.BlockMetadata)
			change(block)
			dest.blocks[height] = block

			options := parityTestOptions()
			options.walk = false
			report := runParity(t, source, dest, options)

			require.False(report.Passed)
			require.Equal(1, report.ProblemCount)
			require.Contains(report.Problems[0], fmt.Sprintf("height %d", height))
			require.Contains(report.Problems[0], field)
		})
	}
}

func TestMigrateParity_SkippedBlockComparesOnlyTheFlag(t *testing.T) {
	require := require.New(t)
	source, dest := newFakeParityStore(), newFakeParityStore()
	height := parityTestFirstHeight + 10
	source.blocks[height] = &api.BlockMetadata{Tag: 1, Height: height, Skipped: true, ParentHeight: height - 1}
	dest.blocks[height] = &api.BlockMetadata{Tag: 1, Height: height, Skipped: true}

	options := parityTestOptions()
	options.walk = false
	report := runParity(t, source, dest, options)

	require.True(report.Passed, "%v", report.Problems)
}

func TestMigrateParity_DetectsDifferentEventFields(t *testing.T) {
	id := int64(10)
	for field, change := range map[string]func(event *model.EventEntry){
		"event_type":    func(event *model.EventEntry) { event.EventType = api.BlockchainEvent_BLOCK_REMOVED },
		"block_height":  func(event *model.EventEntry) { event.BlockHeight = 1 },
		"block_hash":    func(event *model.EventEntry) { event.BlockHash = "0xother" },
		"block_skipped": func(event *model.EventEntry) { event.BlockSkipped = true },
	} {
		t.Run(field, func(t *testing.T) {
			require := require.New(t)
			source, dest := newFakeParityStore(), newFakeParityStore()
			event := *dest.events[id]
			change(&event)
			dest.events[id] = &event

			options := parityTestOptions()
			options.walk = false
			report := runParity(t, source, dest, options)

			require.False(report.Passed)
			require.Equal(1, report.ProblemCount)
			require.Contains(report.Problems[0], "event 10")
			require.Contains(report.Problems[0], field)
		})
	}
}

func TestMigrateParity_WalkFindsMissingHeight(t *testing.T) {
	require := require.New(t)
	source, dest := newFakeParityStore(), newFakeParityStore()
	delete(dest.blocks, parityTestFirstHeight+120)

	options := parityTestOptions()
	options.blockSamples = 0
	options.eventSamples = 0
	report := runParity(t, source, dest, options)

	require.False(report.Passed)
	require.Equal(1, report.ProblemCount)
	require.Contains(report.Problems[0], "postgres blocks")
	require.Contains(report.Problems[0], "item not found")
	require.Equal(parityTestHeights-50, report.HeightsWalked)
}

func TestMigrateParity_WalkFindsBrokenLinkBetweenChunks(t *testing.T) {
	require := require.New(t)
	source, dest := newFakeParityStore(), newFakeParityStore()
	// Chunks start at multiples of 50, so this link only shows if the chunks overlap.
	height := parityTestFirstHeight + 100
	block := proto.Clone(dest.blocks[height]).(*api.BlockMetadata)
	block.ParentHash = "0xother"
	dest.blocks[height] = block

	options := parityTestOptions()
	options.blockSamples = 0
	options.eventSamples = 0
	report := runParity(t, source, dest, options)

	require.False(report.Passed)
	require.Equal(1, report.ProblemCount)
	require.Contains(report.Problems[0], "chain is not continuous")
}

func TestMigrateParity_WalkFindsMissingEvent(t *testing.T) {
	require := require.New(t)
	source, dest := newFakeParityStore(), newFakeParityStore()
	delete(dest.events, 120)

	options := parityTestOptions()
	options.blockSamples = 0
	options.eventSamples = 0
	report := runParity(t, source, dest, options)

	require.False(report.Passed)
	require.Equal(1, report.ProblemCount)
	require.Contains(report.Problems[0], "postgres events")
	require.Contains(report.Problems[0], "item not found")
}

func TestMigrateParity_WalkFindsBrokenEventChainBetweenChunks(t *testing.T) {
	require := require.New(t)
	source, dest := newFakeParityStore(), newFakeParityStore()
	// Chunks start at 1, 51, 101, so this link only shows if the chunks overlap.
	event := *dest.events[101]
	event.ParentHash = "0xother"
	dest.events[101] = &event

	options := parityTestOptions()
	options.blockSamples = 0
	options.eventSamples = 0
	report := runParity(t, source, dest, options)

	require.False(report.Passed)
	require.Equal(1, report.ProblemCount)
	require.Contains(report.Problems[0], "not continuous")
}

func TestMigrateParity_WalkOffDoesNotReadTheWholeChain(t *testing.T) {
	require := require.New(t)
	source, dest := newFakeParityStore(), newFakeParityStore()

	options := parityTestOptions()
	options.walk = false
	options.blockSamples = 0
	options.eventSamples = 0
	report := runParity(t, source, dest, options)

	require.True(report.Passed, "%v", report.Problems)
	require.Zero(report.HeightsWalked)
	require.Zero(report.EventsWalked)
	require.Zero(report.BlocksCompared)
	require.Zero(report.EventsCompared)
	// One read for the tip, one for the first event.
	require.Equal(int64(2), dest.reads.Load())
}

func TestMigrateParity_LiveSourceAheadPasses(t *testing.T) {
	require := require.New(t)
	source, dest := newFakeParityStore(), newFakeParityStore()
	source.grow(5)
	dest.latest = nil

	report := runParity(t, source, dest, parityTestOptions())

	require.True(report.Passed, "%v", report.Problems)
	require.Equal(parityTestHeights, report.BlocksCompared)
	require.Equal(int64(parityTestHeights)+5, report.DynamoDB.MaxEventId)
	require.Equal(int64(parityTestHeights), report.Postgres.MaxEventId)
	require.False(report.Postgres.LatestBlockFound)
}

func TestMigrateParity_RequireCaughtUp(t *testing.T) {
	testCases := []struct {
		name     string
		mutate   func(source, dest *fakeParityStore)
		problems []string
	}{
		{name: "caught up", mutate: func(source, dest *fakeParityStore) {}},
		{
			name:     "postgres behind",
			mutate:   func(source, dest *fakeParityStore) { source.grow(1) },
			problems: []string{"max event id", "event tip"},
		},
		{
			name:     "streamer behind",
			mutate:   func(source, dest *fakeParityStore) { source.latest = parityTestBlock(parityTestTipHeight+1, "a") },
			problems: []string{"the streamer has not caught up"},
		},
		{
			name:     "no watermark",
			mutate:   func(source, dest *fakeParityStore) { dest.latest = nil },
			problems: []string{"postgres watermark"},
		},
		{
			name: "watermark below the tip",
			mutate: func(source, dest *fakeParityStore) {
				dest.latest = dest.blocks[parityTestTipHeight-1]
			},
			problems: []string{"postgres watermark"},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require := require.New(t)
			source, dest := newFakeParityStore(), newFakeParityStore()
			testCase.mutate(source, dest)

			options := parityTestOptions()
			options.requireCaughtUp = true
			options.walk = false
			report := runParity(t, source, dest, options)

			require.Equal(len(testCase.problems) == 0, report.Passed, "%v", report.Problems)
			require.Len(report.Problems, len(testCase.problems))
			for i, problem := range testCase.problems {
				require.Contains(report.Problems[i], problem)
			}
		})
	}
}

func TestMigrateParity_ProblemListIsCapped(t *testing.T) {
	require := require.New(t)
	source, dest := newFakeParityStore(), newFakeParityStore()
	for height := parityTestFirstHeight; height <= parityTestTipHeight; height++ {
		block := proto.Clone(dest.blocks[height]).(*api.BlockMetadata)
		block.ObjectKeyMain = "other"
		dest.blocks[height] = block
	}

	options := parityTestOptions()
	options.walk = false
	report := runParity(t, source, dest, options)

	require.Equal(int(parityTestHeights), report.ProblemCount)
	require.Len(report.Problems, parityMaxReportedProblems)
}

func TestSampleParityWindows_IsDeterministicSortedAndAligned(t *testing.T) {
	require := require.New(t)

	first := sampleParityWindows(rand.New(rand.NewSource(1)), 100, 1000099, 2500, 100)
	second := sampleParityWindows(rand.New(rand.NewSource(1)), 100, 1000099, 2500, 100)

	require.Equal(first, second)
	require.True(sort.SliceIsSorted(first, func(i, j int) bool { return first[i] < first[j] }))
	for _, start := range first {
		require.Zero((start - 100) % parityWindowSize)
	}
}

func TestSampleParityWindows_CountsAndTail(t *testing.T) {
	require := require.New(t)

	windows := sampleParityWindows(rand.New(rand.NewSource(1)), 0, 999999, 2500, 100)

	// 2500 samples are 100 random windows. The last 100 items are the last 4 windows.
	require.Len(windows, 104)
	require.Equal([]uint64{999900, 999925, 999950, 999975}, windows[100:])
}

func TestSampleParityWindows_TailReachesPastAWindowBoundary(t *testing.T) {
	require := require.New(t)

	windows := sampleParityWindows(rand.New(rand.NewSource(1)), 0, 104, 0, 10)

	require.Equal([]uint64{75, 100}, windows)
}

func TestSampleParityWindows_CoversEverythingWhenSamplesExceedTheRange(t *testing.T) {
	require := require.New(t)

	for _, testCase := range []struct{ samples, tail uint64 }{{1000, 0}, {0, 1000}, {60, 60}} {
		windows := sampleParityWindows(rand.New(rand.NewSource(1)), 10, 109, testCase.samples, testCase.tail)
		require.Equal([]uint64{10, 35, 60, 85}, windows)
	}
}

func TestSampleParityWindows_NothingAsked(t *testing.T) {
	require := require.New(t)

	require.Empty(sampleParityWindows(rand.New(rand.NewSource(1)), 0, 1000, 0, 0))
}
