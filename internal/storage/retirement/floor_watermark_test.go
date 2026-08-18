package retirement

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// watermarkIndexMigration defines the partial index the watermark lookup relies
// on to stay cheap as retired history accumulates.
const watermarkIndexMigration = "../metastorage/postgres/db/migrations/20260818000001_add_retention_floor_watermark_index.sql"

// TestFloorWatermarkNeverDropsBelowTheApprovedFloor pins the safety property
// that separates a performance floor from an authorization floor. Retention may
// not delete below approved_start_height, so work discovered underneath it —
// a repair promoted into an old height, for instance — must not drag the probe
// down there.
func TestFloorWatermarkNeverDropsBelowTheApprovedFloor(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()

	repo := &fakeCohortRepository{watermark: 100, watermarkFound: true}
	selector := NewSelector(repo)

	got, err := selector.FloorWatermark(ctx, "", 2, 500)
	require.NoError(err)
	require.Equal(uint64(500), got, "a watermark below the approved floor must not lower the probe start")
	require.Equal(uint64(500), repo.watermarkMinArg, "the approved floor bounds the lookup")
}

// TestFloorWatermarkAdvancesPastRetiredHistory is the whole point of the
// mechanism: everything under the watermark is already retired, so the probe
// starts there and the height range stops growing with the chain.
func TestFloorWatermarkAdvancesPastRetiredHistory(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()

	repo := &fakeCohortRepository{watermark: 439_331_000, watermarkFound: true}
	selector := NewSelector(repo)

	got, err := selector.FloorWatermark(ctx, "", 2, 439_000_000)
	require.NoError(err)
	require.Equal(uint64(439_331_000), got)
}

// TestFloorWatermarkHoldsWhenNoWorkRemains guards the stranding case. With no
// outstanding work the floor stays put rather than jumping to some higher
// height, because rows consolidated moments ago are not due yet and advancing
// past them would put them permanently below the floor.
func TestFloorWatermarkHoldsWhenNoWorkRemains(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()

	repo := &fakeCohortRepository{watermarkFound: false}
	selector := NewSelector(repo)

	got, err := selector.FloorWatermark(ctx, "", 2, 439_000_000)
	require.NoError(err)
	require.Equal(uint64(439_000_000), got)
}

func TestFloorWatermarkRejectsBadInput(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()

	repo := &fakeCohortRepository{watermarkErr: errors.New("boom")}
	_, err := NewSelector(repo).FloorWatermark(ctx, "", 2, 1)
	require.Error(err)
	require.Contains(err.Error(), "boom")

	var nilSelector *Selector
	_, err = nilSelector.FloorWatermark(ctx, "", 2, 1)
	require.Error(err)

	_, err = NewSelector(&fakeCohortRepository{}).FloorWatermark(ctx, "nope", 2, 1)
	require.Error(err, "an unsupported generation must not silently return the approved floor")
}

// TestRetentionFloorWatermarkSQLIsIndexable pins the three properties the
// partial watermark index depends on. Any of them silently regressing turns a
// front-of-index lookup back into a walk over every already-retired row, which
// is invisible until the probe budget is gone.
func TestRetentionFloorWatermarkSQLIsIndexable(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()

	recorder := &recordingCohortQuerier{}
	_, _, err := retentionFloorWatermark(ctx, recorder, "v2", 2, 439_000_000)
	require.ErrorIs(err, errRecordingQuerier)

	// 1. The height bound is present, so the lookup is cheap even before the
	//    migration is applied. This is what makes the code safe to deploy ahead
	//    of its index.
	require.Contains(recorder.query, "shadow.height >= $2")

	// 2. Generation is matched with equality, never the null-safe form that
	//    cannot use a btree index at all.
	require.Contains(recorder.query, "shadow.single_block_storage_generation = $3")
	require.NotContains(recorder.query, "IS NOT DISTINCT FROM")

	// 3. The partial predicate's IS NOT NULL clause is repeated verbatim so the
	//    planner matches the index syntactically instead of having to infer it
	//    from the equality test.
	require.Contains(recorder.query, "shadow.single_block_storage_generation IS NOT NULL")

	require.Equal([]any{uint32(2), uint64(439_000_000), "v2"}, recorder.args)
}

// TestRetentionFloorWatermarkSQLLegacyGenerationBindsNoArgument mirrors the
// selector's handling of the legacy generation, which is stored as NULL. It
// binds no generation argument, so a mismatch here surfaces as a driver-level
// "bind message supplies N parameters" error rather than a wrong result.
func TestRetentionFloorWatermarkSQLLegacyGenerationBindsNoArgument(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()

	recorder := &recordingCohortQuerier{}
	_, _, err := retentionFloorWatermark(ctx, recorder, "", 2, 10)
	require.ErrorIs(err, errRecordingQuerier)

	require.Contains(recorder.query, "shadow.single_block_storage_generation IS NULL")
	// The partial index deliberately excludes legacy rows, so this form must not
	// claim to match it.
	require.NotContains(recorder.query, "shadow.single_block_storage_generation IS NOT NULL")
	require.Equal([]any{uint32(2), uint64(10)}, recorder.args)

	highest := 0
	for _, match := range regexp.MustCompile(`\$(\d+)`).FindAllStringSubmatch(recorder.query, -1) {
		if n := len(match[1]); n > 0 {
			switch match[1] {
			case "1":
				highest = max(highest, 1)
			case "2":
				highest = max(highest, 2)
			case "3":
				highest = max(highest, 3)
			}
		}
	}
	require.Equal(highest, len(recorder.args),
		"the legacy form must bind exactly the placeholders it references")
}

// TestWatermarkIndexMatchesTheWatermarkQuery keeps the migration and the query
// describing the same row set. They are separate files and drift between them
// is silent: the index simply stops being chosen.
func TestWatermarkIndexMatchesTheWatermarkQuery(t *testing.T) {
	require := require.New(t)

	source, err := os.ReadFile(filepath.Clean(watermarkIndexMigration))
	require.NoError(err)
	migration := string(source)

	require.Contains(migration, "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_block_consolidation_shadow_retention_watermark")
	require.Contains(migration, "-- +goose NO TRANSACTION",
		"CREATE INDEX CONCURRENTLY cannot run inside goose's transaction")

	// Key order matters: tag and generation are equality-matched, height is the
	// ordered column the MIN() is answered from.
	keyStart := strings.Index(migration, "ON block_consolidation_shadow (")
	require.Positive(keyStart)
	keys := migration[keyStart : strings.Index(migration[keyStart:], ")")+keyStart]
	for _, column := range []string{"tag", "single_block_storage_generation", "height"} {
		require.Contains(keys, column)
	}
	require.Less(strings.Index(keys, "tag"), strings.Index(keys, "single_block_storage_generation"))
	require.Less(strings.Index(keys, "single_block_storage_generation"), strings.Index(keys, "height"))

	// Every clause of the partial predicate must also appear in the query, or
	// the planner cannot prove the index covers it.
	recorder := &recordingCohortQuerier{}
	_, _, err = retentionFloorWatermark(context.Background(), recorder, "v2", 2, 1)
	require.ErrorIs(err, errRecordingQuerier)
	for _, clause := range []string{
		"single_block_object_deleted_at IS NULL",
		"single_block_object_key_main IS NOT NULL",
		"single_block_object_key_main <> ''",
		"single_block_storage_generation IS NOT NULL",
	} {
		require.Contains(migration, clause, "migration predicate")
		require.Contains(recorder.query, clause, "query must repeat the index predicate clause %q", clause)
	}

	// The watermark must not filter on due time or validation state. Both are
	// reasons a row is not deleted *yet*, so including them would let the floor
	// step over live work.
	require.NotContains(migration, "single_block_delete_after")
	require.NotContains(recorder.query, "single_block_delete_after")
	require.NotContains(recorder.query, "validated_at")
}
