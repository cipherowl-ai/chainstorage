package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/aws"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	dynamodb_storage "github.com/coinbase/chainstorage/internal/storage/metastorage/dynamodb"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	postgres_storage "github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/ratelimiter"
	"github.com/coinbase/chainstorage/internal/utils/syncgroup"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	// A window of this size is one DynamoDB transaction.
	parityWindowSize          = uint64(25)
	parityWindowsPerLog       = 1000
	parityWalkChunksPerLog    = 100
	parityMaxReportedProblems = 100
	defaultParitySeed         = int64(2275)
)

type (
	migrateParityOptions struct {
		tag                    uint32
		eventTag               uint32
		blockStartHeight       uint64
		blockSamples           uint64
		blockTail              uint64
		eventSamples           uint64
		eventTail              uint64
		seed                   int64
		walk                   bool
		walkChunkSize          uint64
		walkParallelism        int
		dynamoDBReadsPerSecond int
		requireCaughtUp        bool
		reportJSON             string
	}

	migrateParitySide struct {
		MaxEventId        int64  `json:"max_event_id"`
		TipHeight         uint64 `json:"tip_height"`
		TipHash           string `json:"tip_hash"`
		LatestBlockFound  bool   `json:"latest_block_found"`
		LatestBlockHeight uint64 `json:"latest_block_height"`
		LatestBlockHash   string `json:"latest_block_hash"`
	}

	migrateParityReport struct {
		GeneratedAt    string            `json:"generated_at"`
		Environment    string            `json:"environment"`
		Blockchain     string            `json:"blockchain"`
		Network        string            `json:"network"`
		Tag            uint32            `json:"tag"`
		EventTag       uint32            `json:"event_tag"`
		Passed         bool              `json:"passed"`
		DynamoDB       migrateParitySide `json:"dynamodb"`
		Postgres       migrateParitySide `json:"postgres"`
		FirstHeight    uint64            `json:"first_height"`
		HeightsWalked  uint64            `json:"heights_walked"`
		EventsWalked   uint64            `json:"events_walked"`
		BlocksCompared uint64            `json:"blocks_compared"`
		EventsCompared uint64            `json:"events_compared"`
		ProblemCount   int               `json:"problem_count"`
		Problems       []string          `json:"problems,omitempty"`
	}

	// migrateParityChecker only ever reads from its stores.
	migrateParityChecker struct {
		source  metastorage.MetaStorage
		dest    metastorage.MetaStorage
		options migrateParityOptions
		limiter *ratelimiter.RateLimiter

		mu     sync.Mutex
		report *migrateParityReport
	}
)

var (
	parityFlags migrateParityOptions

	migrateParityCmd = &cobra.Command{
		Use:   "migrate-parity",
		Short: "Compare a chain's migrated PostgreSQL metadata with DynamoDB. Read-only.",
		Long: `Compare a chain's migrated PostgreSQL metadata with DynamoDB. Read-only.

The walk reads all of PostgreSQL up to the tip of its event stream: every canonical height must
exist and link to its parent, and every event id must exist and continue the chain. Chunks overlap
by one height or event, so the links between chunks are checked too. The first event must be at
the chain's block start height, because the migrator only copies blocks it reaches through events.

The samples compare random windows of heights and event ids, plus the last ones, with DynamoDB:
hash, parent hash, parent height, object key, timestamp and skipped for blocks, and id, type,
height, hash and skipped for events. Near the tip of a live chain a sample can differ until the
migrator copies the next reorg. Run it again before treating that as a failure.

The walk loads the database, so point it at the reader: the admin console's PostgreSQL host is the
writer. --dynamodb-reads-per-second limits what the samples add to the DynamoDB tables, and each
read costs 2 RCU.

Examples:
  # While the migrator copies: walk PostgreSQL and sample both stores
  CHAINSTORAGE_AWS_POSTGRES_HOST=<reader endpoint> admin migrate-parity \
    --env=production --blockchain=base --network=mainnet

  # With the writers stopped and the last bounded migrator run finished: compare the tips
  CHAINSTORAGE_AWS_POSTGRES_HOST=<reader endpoint> admin migrate-parity \
    --env=production --blockchain=base --network=mainnet \
    --walk=false --block-samples=0 --event-samples=0 \
    --block-tail=10000 --event-tail=10000 --require-caught-up`,
		RunE: runMigrateParity,
	}
)

func init() {
	flags := migrateParityCmd.Flags()
	flags.Uint32Var(&parityFlags.tag, "tag", 0, "block tag; 0 uses the chain's stable tag")
	flags.Uint32Var(&parityFlags.eventTag, "event-tag", 0, "event tag; 0 uses the chain's stable event tag")
	flags.Uint64Var(&parityFlags.blockSamples, "block-samples", 1000000, "heights to compare at random, in windows of 25")
	flags.Uint64Var(&parityFlags.blockTail, "block-tail", 100000, "last heights to compare")
	flags.Uint64Var(&parityFlags.eventSamples, "event-samples", 1000000, "event ids to compare at random, in windows of 25")
	flags.Uint64Var(&parityFlags.eventTail, "event-tail", 100000, "last event ids to compare")
	flags.Int64Var(&parityFlags.seed, "seed", defaultParitySeed, "random seed for the samples")
	flags.BoolVar(&parityFlags.walk, "walk", true, "walk every canonical height and event in PostgreSQL")
	flags.Uint64Var(&parityFlags.walkChunkSize, "walk-chunk-size", 10000, "heights or events per PostgreSQL read during the walk")
	flags.IntVar(&parityFlags.walkParallelism, "walk-parallelism", 2, "concurrent PostgreSQL reads during the walk")
	flags.IntVar(&parityFlags.dynamoDBReadsPerSecond, "dynamodb-reads-per-second", 100, "items per second the samples read from each DynamoDB table; 0 means no limit")
	flags.BoolVar(&parityFlags.requireCaughtUp, "require-caught-up", false, "fail unless both stores end at the same event and block, and PostgreSQL has its watermark there")
	flags.StringVar(&parityFlags.reportJSON, "report-json", "", "write the JSON report to this path")

	rootCmd.AddCommand(migrateParityCmd)
}

func runMigrateParity(cmd *cobra.Command, args []string) error {
	if parityFlags.walkChunkSize == 0 || parityFlags.walkParallelism <= 0 {
		return xerrors.New("walk-chunk-size and walk-parallelism must be positive")
	}

	var deps struct {
		fx.In
		Config    *config.Config
		AWSConfig awssdk.Config
		Params    fxparams.Params
	}

	app := startApp(
		aws.Module,
		storage.Module,
		fx.Populate(&deps),
	)
	defer app.Close()

	sourceResult, err := dynamodb_storage.NewMetaStorage(dynamodb_storage.Params{
		Params:    deps.Params,
		AWSConfig: deps.AWSConfig,
	})
	if err != nil {
		return xerrors.Errorf("failed to create DynamoDB storage: %w", err)
	}
	destResult, err := postgres_storage.NewMetaStorage(postgres_storage.Params{
		Params: deps.Params,
	})
	if err != nil {
		return xerrors.Errorf("failed to create PostgreSQL storage: %w", err)
	}

	options := parityFlags
	options.tag = deps.Config.GetEffectiveBlockTag(options.tag)
	options.eventTag = deps.Config.GetEffectiveEventTag(options.eventTag)
	options.blockStartHeight = deps.Config.Chain.BlockStartHeight

	checker := newMigrateParityChecker(sourceResult.MetaStorage, destResult.MetaStorage, options)
	report, err := checker.run(context.Background())
	if err != nil {
		return xerrors.Errorf("failed to compare stores: %w", err)
	}
	report.Environment = commonFlags.env
	report.Blockchain = commonFlags.blockchain
	report.Network = commonFlags.network

	logger.Info("migrate parity finished", zap.Reflect("report", report))
	if options.reportJSON != "" {
		if err := writeMigrateParityReport(options.reportJSON, report); err != nil {
			return err
		}
	}
	if !report.Passed {
		return xerrors.Errorf("parity failed with %d problems", report.ProblemCount)
	}
	return nil
}

func writeMigrateParityReport(path string, report *migrateParityReport) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return xerrors.Errorf("failed to create report directory: %w", err)
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return xerrors.Errorf("failed to marshal JSON report: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return xerrors.Errorf("failed to write JSON report: %w", err)
	}
	return nil
}

func newMigrateParityChecker(source, dest metastorage.MetaStorage, options migrateParityOptions) *migrateParityChecker {
	return &migrateParityChecker{
		source:  source,
		dest:    dest,
		options: options,
		limiter: ratelimiter.New(options.dynamoDBReadsPerSecond),
		report: &migrateParityReport{
			GeneratedAt: time.Now().UTC().Format(time.RFC3339),
			Tag:         options.tag,
			EventTag:    options.eventTag,
		},
	}
}

func (c *migrateParityChecker) run(ctx context.Context) (*migrateParityReport, error) {
	var err error
	if c.report.DynamoDB, err = c.readSide(ctx, c.source); err != nil {
		return nil, xerrors.Errorf("failed to read DynamoDB: %w", err)
	}
	if c.report.Postgres, err = c.readSide(ctx, c.dest); err != nil {
		return nil, xerrors.Errorf("failed to read PostgreSQL: %w", err)
	}
	firstEvents, err := c.dest.GetEventsByEventIdRange(ctx, c.options.eventTag, metastorage.EventIdStartValue, metastorage.EventIdStartValue+1)
	if err != nil {
		return nil, xerrors.Errorf("failed to read the first event from PostgreSQL: %w", err)
	}
	firstHeight := firstEvents[0].BlockHeight
	tipHeight := c.report.Postgres.TipHeight
	maxEventId := uint64(c.report.Postgres.MaxEventId)
	c.report.FirstHeight = firstHeight

	if firstHeight > c.options.blockStartHeight {
		c.addProblem("the first event is at height %d, above the block start height %d: nothing below it was copied",
			firstHeight, c.options.blockStartHeight)
	}
	if c.options.requireCaughtUp {
		c.checkCaughtUp()
	}
	if c.options.walk {
		if err := c.walkBlocks(ctx, firstHeight, tipHeight); err != nil {
			return nil, xerrors.Errorf("failed to walk blocks: %w", err)
		}
		if err := c.walkEvents(ctx, maxEventId); err != nil {
			return nil, xerrors.Errorf("failed to walk events: %w", err)
		}
	}

	rng := rand.New(rand.NewSource(c.options.seed))
	if err := c.compareBlocks(ctx, rng, firstHeight, tipHeight); err != nil {
		return nil, xerrors.Errorf("failed to compare blocks: %w", err)
	}
	if err := c.compareEvents(ctx, rng, maxEventId); err != nil {
		return nil, xerrors.Errorf("failed to compare events: %w", err)
	}

	c.report.Passed = c.report.ProblemCount == 0
	return c.report, nil
}

func (c *migrateParityChecker) readSide(ctx context.Context, store metastorage.MetaStorage) (migrateParitySide, error) {
	var side migrateParitySide
	maxEventId, err := store.GetMaxEventId(ctx, c.options.eventTag)
	if err != nil {
		return side, xerrors.Errorf("failed to get max event id: %w", err)
	}
	tip, err := metastorage.GetTipEvent(ctx, store, c.options.eventTag, maxEventId)
	if err != nil {
		return side, xerrors.Errorf("failed to get tip event: %w", err)
	}
	side.MaxEventId = maxEventId
	side.TipHeight = tip.BlockHeight
	side.TipHash = tip.BlockHash

	latest, err := store.GetLatestBlock(ctx, c.options.tag)
	if err != nil {
		// A PostgreSQL chain has no latest block until a bounded migrator run sets the watermark.
		if xerrors.Is(err, storage.ErrItemNotFound) {
			return side, nil
		}
		return side, xerrors.Errorf("failed to get latest block: %w", err)
	}
	side.LatestBlockFound = true
	side.LatestBlockHeight = latest.Height
	side.LatestBlockHash = latest.Hash
	return side, nil
}

func (c *migrateParityChecker) checkCaughtUp() {
	source, dest := c.report.DynamoDB, c.report.Postgres
	if source.MaxEventId != dest.MaxEventId {
		c.addProblem("max event id: dynamodb=%d, postgres=%d", source.MaxEventId, dest.MaxEventId)
	}
	if source.TipHeight != dest.TipHeight || source.TipHash != dest.TipHash {
		c.addProblem("event tip: dynamodb=(%d, %s), postgres=(%d, %s)",
			source.TipHeight, source.TipHash, dest.TipHeight, dest.TipHash)
	}
	// The migrator follows events, so it never copies a block the streamer has not reached.
	if !source.LatestBlockFound || source.LatestBlockHeight != source.TipHeight || source.LatestBlockHash != source.TipHash {
		c.addProblem("dynamodb latest block (%d, %s) is not its event tip (%d, %s): the streamer has not caught up",
			source.LatestBlockHeight, source.LatestBlockHash, source.TipHeight, source.TipHash)
	}
	if !dest.LatestBlockFound || dest.LatestBlockHeight != dest.TipHeight || dest.LatestBlockHash != dest.TipHash {
		c.addProblem("postgres watermark (%d, %s) is not its event tip (%d, %s)",
			dest.LatestBlockHeight, dest.LatestBlockHash, dest.TipHeight, dest.TipHash)
	}
}

func (c *migrateParityChecker) walkBlocks(ctx context.Context, firstHeight, tipHeight uint64) error {
	group, groupCtx := syncgroup.New(ctx, syncgroup.WithThrottling(c.options.walkParallelism))
	chunks := 0
	for start := firstHeight; start <= tipHeight; start += c.options.walkChunkSize {
		end := min(start+c.options.walkChunkSize, tipHeight+1)
		readStart := start
		if start > firstHeight {
			readStart--
		}
		group.Go(func() error {
			// This read fails on a missing height or a broken parent link.
			if _, err := c.dest.GetBlocksByHeightRange(groupCtx, c.options.tag, readStart, end); err != nil {
				if groupCtx.Err() != nil {
					return groupCtx.Err()
				}
				c.addProblem("postgres blocks [%d, %d): %v", readStart, end, err)
				return nil
			}
			c.mu.Lock()
			defer c.mu.Unlock()
			c.report.HeightsWalked += end - start
			return nil
		})
		if chunks++; chunks%parityWalkChunksPerLog == 0 {
			logger.Info("walking postgres blocks", zap.Uint64("height", start), zap.Uint64("tip_height", tipHeight))
		}
	}
	return group.Wait()
}

func (c *migrateParityChecker) walkEvents(ctx context.Context, maxEventId uint64) error {
	firstEventId := uint64(metastorage.EventIdStartValue)
	group, groupCtx := syncgroup.New(ctx, syncgroup.WithThrottling(c.options.walkParallelism))
	chunks := 0
	for start := firstEventId; start <= maxEventId; start += c.options.walkChunkSize {
		end := min(start+c.options.walkChunkSize, maxEventId+1)
		readStart := start
		if start > firstEventId {
			readStart--
		}
		group.Go(func() error {
			// This read fails on a missing event id.
			events, err := c.dest.GetEventsByEventIdRange(groupCtx, c.options.eventTag, int64(readStart), int64(end))
			if err != nil {
				if groupCtx.Err() != nil {
					return groupCtx.Err()
				}
				c.addProblem("postgres events [%d, %d): %v", readStart, end, err)
				return nil
			}
			if err := metastorage.ValidateEvents(events); err != nil {
				c.addProblem("postgres events [%d, %d): %v", readStart, end, err)
				return nil
			}
			c.mu.Lock()
			defer c.mu.Unlock()
			c.report.EventsWalked += end - start
			return nil
		})
		if chunks++; chunks%parityWalkChunksPerLog == 0 {
			logger.Info("walking postgres events", zap.Uint64("event_id", start), zap.Uint64("max_event_id", maxEventId))
		}
	}
	return group.Wait()
}

func (c *migrateParityChecker) compareBlocks(ctx context.Context, rng *rand.Rand, firstHeight, tipHeight uint64) error {
	windows := sampleParityWindows(rng, firstHeight, tipHeight, c.options.blockSamples, c.options.blockTail)
	for i, start := range windows {
		heights := make([]uint64, 0, parityWindowSize)
		for height := start; height < start+parityWindowSize && height <= tipHeight; height++ {
			heights = append(heights, height)
		}
		if err := c.waitForDynamoDB(ctx, len(heights)); err != nil {
			return err
		}
		sourceBlocks, err := c.source.GetBlocksByHeights(ctx, c.options.tag, heights)
		if err != nil {
			c.addProblem("dynamodb blocks at heights [%d, %d]: %v", heights[0], heights[len(heights)-1], err)
			continue
		}
		destBlocks, err := c.dest.GetBlocksByHeights(ctx, c.options.tag, heights)
		if err != nil {
			c.addProblem("postgres blocks at heights [%d, %d]: %v", heights[0], heights[len(heights)-1], err)
			continue
		}
		for j, height := range heights {
			if fields := diffBlockFields(sourceBlocks[j], destBlocks[j]); len(fields) > 0 {
				c.addProblem("block at height %d differs in %v (dynamodb=%v, postgres=%v)", height, fields, sourceBlocks[j], destBlocks[j])
			}
		}
		c.report.BlocksCompared += uint64(len(heights))
		if (i+1)%parityWindowsPerLog == 0 {
			logger.Info("comparing blocks", zap.Int("window", i+1), zap.Int("windows", len(windows)))
		}
	}
	return nil
}

func (c *migrateParityChecker) compareEvents(ctx context.Context, rng *rand.Rand, maxEventId uint64) error {
	windows := sampleParityWindows(rng, uint64(metastorage.EventIdStartValue), maxEventId, c.options.eventSamples, c.options.eventTail)
	for i, start := range windows {
		end := min(start+parityWindowSize, maxEventId+1)
		if err := c.waitForDynamoDB(ctx, int(end-start)); err != nil {
			return err
		}
		sourceEvents, err := c.source.GetEventsByEventIdRange(ctx, c.options.eventTag, int64(start), int64(end))
		if err != nil {
			c.addProblem("dynamodb events [%d, %d): %v", start, end, err)
			continue
		}
		destEvents, err := c.dest.GetEventsByEventIdRange(ctx, c.options.eventTag, int64(start), int64(end))
		if err != nil {
			c.addProblem("postgres events [%d, %d): %v", start, end, err)
			continue
		}
		if len(sourceEvents) != len(destEvents) {
			c.addProblem("events [%d, %d): dynamodb has %d, postgres has %d", start, end, len(sourceEvents), len(destEvents))
			continue
		}
		for j, event := range sourceEvents {
			if fields := diffEventFields(event, destEvents[j]); len(fields) > 0 {
				c.addProblem("event %d differs in %v (dynamodb=%+v, postgres=%+v)", event.EventId, fields, event, destEvents[j])
			}
		}
		c.report.EventsCompared += end - start
		if (i+1)%parityWindowsPerLog == 0 {
			logger.Info("comparing events", zap.Int("window", i+1), zap.Int("windows", len(windows)))
		}
	}
	return nil
}

func (c *migrateParityChecker) waitForDynamoDB(ctx context.Context, items int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return c.limiter.WaitWeight(ctx, items)
}

func (c *migrateParityChecker) addProblem(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.report.ProblemCount++
	if len(c.report.Problems) < parityMaxReportedProblems {
		c.report.Problems = append(c.report.Problems, fmt.Sprintf(format, args...))
	}
}

// sampleParityWindows returns the first item of each window to compare, sorted: enough random
// windows over [first, last] to hold samples items, plus every window holding the last tail items.
func sampleParityWindows(rng *rand.Rand, first, last, samples, tail uint64) []uint64 {
	total := (last-first)/parityWindowSize + 1
	picked := make(map[uint64]struct{})
	if tail > 0 {
		tailStart := first
		if tail <= last-first {
			tailStart = last - tail + 1
		}
		for window := (tailStart - first) / parityWindowSize; window < total; window++ {
			picked[window] = struct{}{}
		}
	}

	randomWindows := (samples + parityWindowSize - 1) / parityWindowSize
	if uint64(len(picked))+randomWindows >= total {
		for window := uint64(0); window < total; window++ {
			picked[window] = struct{}{}
		}
	} else {
		for want := uint64(len(picked)) + randomWindows; uint64(len(picked)) < want; {
			picked[rng.Uint64()%total] = struct{}{}
		}
	}

	starts := make([]uint64, 0, len(picked))
	for window := range picked {
		starts = append(starts, first+window*parityWindowSize)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	return starts
}

// diffEventFields names the fields in which the two events differ. It compares the event itself,
// not the block details copied onto it, which older events can lack.
func diffEventFields(source, dest *model.EventEntry) []string {
	var fields []string
	if source.EventId != dest.EventId {
		fields = append(fields, "event_id")
	}
	if source.EventType != dest.EventType {
		fields = append(fields, "event_type")
	}
	if source.BlockHeight != dest.BlockHeight {
		fields = append(fields, "block_height")
	}
	if source.BlockHash != dest.BlockHash {
		fields = append(fields, "block_hash")
	}
	if source.BlockSkipped != dest.BlockSkipped {
		fields = append(fields, "block_skipped")
	}
	return fields
}

// diffBlockFields names the fields in which the two blocks differ. The migrator rebuilds a skipped
// block from its event, so only the flag itself carries over.
func diffBlockFields(source, dest *api.BlockMetadata) []string {
	var fields []string
	if source.GetHeight() != dest.GetHeight() {
		fields = append(fields, "height")
	}
	if source.GetSkipped() != dest.GetSkipped() {
		fields = append(fields, "skipped")
	}
	if source.GetSkipped() {
		return fields
	}
	if source.GetHash() != dest.GetHash() {
		fields = append(fields, "hash")
	}
	if source.GetParentHash() != dest.GetParentHash() {
		fields = append(fields, "parent_hash")
	}
	if source.GetParentHeight() != dest.GetParentHeight() {
		fields = append(fields, "parent_height")
	}
	if source.GetObjectKeyMain() != dest.GetObjectKeyMain() {
		fields = append(fields, "object_key_main")
	}
	if source.GetTimestamp().GetSeconds() != dest.GetTimestamp().GetSeconds() {
		fields = append(fields, "timestamp")
	}
	return fields
}
