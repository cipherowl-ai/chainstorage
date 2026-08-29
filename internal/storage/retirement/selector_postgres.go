package retirement

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"golang.org/x/xerrors"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type retentionCohortQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// storageGenerationMatch renders an indexable generation match for each column.
//
// The null-safe IS NOT DISTINCT FROM form these queries used, against a
// NULLIF of the generation parameter, cannot use a btree index at all —
// verified with EXPLAIN against production:
// `height = 439200000` plans an Index Only Scan while
// `height IS NOT DISTINCT FROM 439200000` falls back to a parallel sequential
// scan. Because the generation value is known when the query is built, the
// same semantics are expressible as plain equality (concrete generation) or an
// IS NULL test (the legacy generation, stored as NULL), both of which the
// planner can seek on. Adding a generation column to an index is worthless
// until the query is written this way (INF-1330).
//
// placeholder is referenced only for a concrete generation; callers must bind
// the generation argument exactly when storageGenerationIsBound reports true.
func storageGenerationMatch(generation string, placeholder string, columns ...string) string {
	predicates := make([]string, 0, len(columns))
	for _, column := range columns {
		if generation == "" {
			predicates = append(predicates, column+" IS NULL")
			continue
		}
		predicates = append(predicates, column+" = "+placeholder)
	}
	return strings.Join(predicates, "\n\t\t\tAND ")
}

// storageGenerationIsBound reports whether storageGenerationMatch referenced
// its placeholder, so callers append the argument only when the query uses it.
func storageGenerationIsBound(generation string) bool {
	return generation != ""
}

// ListRetentionCohorts reads pending and newly due work from one repeatable-read
// snapshot. Without the shared snapshot, a manifest inserted between the two
// reads can disappear from both result sets and falsely signal sweep completion.
func (r *PostgresRepository) ListRetentionCohorts(
	ctx context.Context,
	bucket string,
	storageGeneration string,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
	eligibilityCutoff time.Time,
	limit int,
) ([]RetentionCohort, []RetentionCohort, error) {
	if r == nil || r.db == nil {
		return nil, nil, xerrors.New("postgres db is required")
	}
	if limit <= 0 {
		return nil, nil, nil
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to begin retention cohort snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	pending, err := listPendingRetentionCohorts(
		ctx,
		tx,
		bucket,
		storageGeneration,
		tag,
		startHeight,
		endHeight,
		eligibilityCutoff,
		limit,
	)
	if err != nil {
		return nil, nil, err
	}
	due, err := listDueRetentionCohorts(
		ctx,
		tx,
		storageGeneration,
		tag,
		startHeight,
		endHeight,
		eligibilityCutoff,
		limit,
	)
	if err != nil {
		return nil, nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, nil, xerrors.Errorf("failed to commit retention cohort snapshot: %w", err)
	}
	return pending, due, nil
}

func (r *PostgresRepository) ListPendingRetentionCohorts(
	ctx context.Context,
	bucket string,
	storageGeneration string,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
	eligibilityCutoff time.Time,
	limit int,
) ([]RetentionCohort, error) {
	if r == nil || r.db == nil {
		return nil, xerrors.New("postgres db is required")
	}
	return listPendingRetentionCohorts(
		ctx,
		r.db,
		bucket,
		storageGeneration,
		tag,
		startHeight,
		endHeight,
		eligibilityCutoff,
		limit,
	)
}

func listPendingRetentionCohorts(
	ctx context.Context,
	db retentionCohortQuerier,
	bucket string,
	storageGeneration string,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
	eligibilityCutoff time.Time,
	limit int,
) ([]RetentionCohort, error) {
	if limit <= 0 {
		return nil, nil
	}
	query := `
		SELECT
			retention.consolidated_object_key_main,
			MIN(retention.height),
			MAX(retention.height) + 1,
			COUNT(*),
			MAX(shadow.single_block_delete_after)
		FROM block_single_block_retention retention
		JOIN block_consolidation_shadow shadow
			ON shadow.block_metadata_id = retention.block_metadata_id
			AND shadow.tag = retention.tag
			AND shadow.height = retention.height
		JOIN block_metadata metadata
			ON metadata.id = retention.block_metadata_id
			AND metadata.tag = retention.tag
			AND metadata.height = retention.height
		WHERE retention.tag = $1
			AND retention.state IN (` + pendingRetirementStatesSQL + `)
			AND ($3::BIGINT = 0 OR (retention.height >= $2 AND retention.height < $3))
			AND shadow.single_block_delete_after <= $4
			AND retention.bucket = $6
			AND ` + storageGenerationMatch(
		storageGeneration,
		"$7",
		"metadata.storage_generation",
		"shadow.single_block_storage_generation",
		"shadow.consolidated_storage_generation",
	) + `
		GROUP BY retention.consolidated_object_key_main
		ORDER BY MIN(retention.prepared_at), MIN(retention.height), retention.consolidated_object_key_main
		LIMIT $5`
	args := []any{tag, startHeight, endHeight, eligibilityCutoff, limit, bucket}
	if storageGenerationIsBound(storageGeneration) {
		args = append(args, storageGeneration)
	}
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, xerrors.Errorf("failed to query pending retention cohorts: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRetentionCohorts(rows, true)
}

const (
	// dueRetentionCohortOrderingByDueTime retires whatever has been due longest
	// first. It is the right policy for an unbounded selection, where the caller
	// is asking "find the most overdue work anywhere".
	dueRetentionCohortOrderingByDueTime = "c.eligible_at, c.start_height, c.consolidated_object_key_main"
	// dueRetentionCohortOrderingByHeight walks a caller-supplied range in height
	// order so the selection is deterministic and prefix-shaped.
	dueRetentionCohortOrderingByHeight = "c.start_height, c.consolidated_object_key_main"
)

// dueRetentionCohortOrdering picks the cohort ordering for a selection. Both
// return values are compile-time constants and never caller input.
//
// When an explicit height range is supplied, selection must be deterministic:
// a run takes the lowest eligible cohorts in the range and repeated runs walk it
// monotonically until MoreEligibleRanges reports false. Ordering by eligible_at
// there would instead return a due-time-ordered *subset* of the range, which
// breaks both retention gates that operators rely on. First, an operator
// approving [start, end) cannot predict which cohorts a run will touch, which
// reintroduces the "approval does not match the selected work" problem. Second,
// eligible_at advances as retention clocks tick and as repairs re-stamp
// single_block_delete_after, so the same approved range can resolve to a
// different subset later — meaning a clean dry run would no longer predict what
// an execute run over that range deletes.
func dueRetentionCohortOrdering(endHeight uint64) string {
	if endHeight > 0 {
		return dueRetentionCohortOrderingByHeight
	}
	return dueRetentionCohortOrderingByDueTime
}

func (r *PostgresRepository) ListDueRetentionCohorts(
	ctx context.Context,
	storageGeneration string,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
	eligibilityCutoff time.Time,
	limit int,
) ([]RetentionCohort, error) {
	if r == nil || r.db == nil {
		return nil, xerrors.New("postgres db is required")
	}
	return listDueRetentionCohorts(
		ctx,
		r.db,
		storageGeneration,
		tag,
		startHeight,
		endHeight,
		eligibilityCutoff,
		limit,
	)
}

// listDueRetentionCohorts selects due cohorts at COHORT granularity: the
// shadow-only predicates aggregate the window first (index-driven, cheap), and
// the join-level checks — canonical membership, metadata pointing at the
// consolidated object, pending-retention exclusion, active-repair exclusion —
// run once per cohort against its start-height row, not once per shadow row.
//
// The per-row shape this replaces cost ~800k index lookups per 200k-block
// window (~200k rows x canonical pk + metadata pk + two anti-joins): measured
// 25.3s on robinhood-mainnet prod in the healthy case, blowing the 60s
// statement timeout whenever consolidation bulk-upserts ran concurrently
// (INF-1448; the retention cron failed three consecutive ticks during the
// 2026-08-29 ingestion catch-up). Restructuring the two-pass CTE into one pass
// changed nothing (27.0s measured) — the joins were the cost, not the CTE.
// This shape measured 0.54s on the same window under the same load, returning
// identical cohorts.
//
// Representative-row validation is sufficient because SELECTION NEVER DELETES:
// the sweep re-proves every row before anything irreversible — applyCandidate
// runs revalidateMetadataAndCSCB per row (canonical metadata + live pinned
// CSCB object, fresh current and versioned HEADs) before the retirement fence,
// and reconcileManifest handles cohorts left partially pending by a crash. A
// cohort whose interior holds a broken row is selected here and that row is
// rejected inside the sweep with a skip reason, exactly as a mid-sweep repair
// would be. Two deliberate semantic deltas from the per-row shape: RowCount
// counts every shadow row of the cohort rather than only join-validated ones
// (the planner re-derives per row anyway), and join-level exclusion keys off
// the start-height row.
func listDueRetentionCohorts(
	ctx context.Context,
	db retentionCohortQuerier,
	storageGeneration string,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
	eligibilityCutoff time.Time,
	limit int,
) ([]RetentionCohort, error) {
	if limit <= 0 {
		return nil, nil
	}
	queryTemplate := `
		WITH cohorts AS (
			SELECT
				shadow.consolidated_object_key_main,
				MIN(shadow.height) AS start_height,
				MAX(shadow.height) + 1 AS end_height,
				COUNT(*) AS row_count,
				MIN(shadow.single_block_delete_after) AS eligible_at,
				MAX(shadow.single_block_delete_after) AS latest_due_at
			FROM block_consolidation_shadow shadow
			WHERE shadow.tag = $1
				AND shadow.validated_at IS NOT NULL
				AND shadow.single_block_delete_after IS NOT NULL
				AND shadow.single_block_object_deleted_at IS NULL
				AND shadow.single_block_object_key_main IS NOT NULL
				AND shadow.single_block_object_key_main <> ''
				AND shadow.consolidated_object_key_main IS NOT NULL
				AND shadow.consolidated_object_key_main <> ''
				AND %s
				AND ($3::BIGINT = 0 OR (shadow.height >= $2 AND shadow.height < $3))
			GROUP BY shadow.consolidated_object_key_main
			HAVING MAX(shadow.single_block_delete_after) <= $6
		)
		SELECT
			c.consolidated_object_key_main,
			c.start_height,
			c.end_height,
			c.row_count,
			c.latest_due_at
		FROM cohorts c
		WHERE EXISTS (
				SELECT 1
				FROM block_consolidation_shadow s
				JOIN canonical_blocks canonical
					ON canonical.tag = s.tag
					AND canonical.height = s.height
					AND canonical.block_metadata_id = s.block_metadata_id
				JOIN block_metadata metadata
					ON metadata.id = canonical.block_metadata_id
					AND metadata.tag = canonical.tag
					AND metadata.height = canonical.height
				WHERE s.tag = $1
					AND s.height = c.start_height
					AND s.consolidated_object_key_main = c.consolidated_object_key_main
					AND metadata.skipped = FALSE
					AND metadata.object_format = $4
					AND metadata.object_key_main = c.consolidated_object_key_main
					AND %s
			)
			AND NOT EXISTS (
				SELECT 1
				FROM block_consolidation_shadow s2
				JOIN block_single_block_retention retention
					ON retention.block_metadata_id = s2.block_metadata_id
					AND retention.tag = s2.tag
				WHERE s2.tag = $1
					AND s2.height = c.start_height
					AND s2.consolidated_object_key_main = c.consolidated_object_key_main
					AND retention.state IN (` + pendingRetirementStatesSQL + `)
			)
			AND NOT EXISTS (
				SELECT 1
				FROM cscb_repair_manifest repair
				WHERE repair.tag = $1
					AND repair.state <> 'completed'
					AND (
						repair.old_consolidated_object_key_main = c.consolidated_object_key_main
						OR repair.new_consolidated_object_key_main = c.consolidated_object_key_main
					)
			)
		ORDER BY %s
		LIMIT $5`
	query := fmt.Sprintf(
		queryTemplate,
		storageGenerationMatch(
			storageGeneration,
			"$7",
			"shadow.single_block_storage_generation",
			"shadow.consolidated_storage_generation",
		),
		storageGenerationMatch(
			storageGeneration,
			"$7",
			"metadata.storage_generation",
		),
		dueRetentionCohortOrdering(endHeight),
	)
	args := []any{
		tag,
		startHeight,
		endHeight,
		api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		limit,
		eligibilityCutoff,
	}
	if storageGenerationIsBound(storageGeneration) {
		args = append(args, storageGeneration)
	}
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, xerrors.Errorf("failed to query due retention cohorts: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRetentionCohorts(rows, false)
}

func scanRetentionCohorts(rows *sql.Rows, pending bool) ([]RetentionCohort, error) {
	result := make([]RetentionCohort, 0)
	for rows.Next() {
		var (
			objectKey  string
			start      int64
			end        int64
			count      int64
			eligibleAt time.Time
		)
		if err := rows.Scan(&objectKey, &start, &end, &count, &eligibleAt); err != nil {
			return nil, xerrors.Errorf("failed to scan retention cohort: %w", err)
		}
		if start < 0 || end <= start || count <= 0 {
			return nil, xerrors.Errorf(
				"invalid retention cohort bounds: key=%q start=%d end=%d rows=%d",
				objectKey,
				start,
				end,
				count,
			)
		}
		result = append(result, RetentionCohort{
			ConsolidatedObjectKey: objectKey,
			StartHeight:           uint64(start),
			EndHeight:             uint64(end),
			RowCount:              uint64(count),
			EligibleAt:            eligibleAt,
			Pending:               pending,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("failed to iterate retention cohorts: %w", err)
	}
	return result, nil
}

// RetentionFloorWatermark returns the lowest height at or above minHeight that
// still holds an undeleted single-block object for storageGeneration.
//
// Everything below the returned height has already been retired, so the probe
// can start there instead of at the operator's approved_start_height. That
// matters because the due-cohort query's cohort-expansion join is bounded only
// by the height range: past roughly 1.8M heights of width the planner abandons
// idx_block_consolidation_shadow_tag_height for a sequential scan of the whole
// table, and the probe stops finishing inside its statement timeout. A fixed
// floor widens at the chain's block rate and reaches that point on its own; the
// watermark holds the range at the retention delay expressed in blocks.
//
// Measured on solana-mainnet prod: watermark 439,331,000 against an approval
// floor of 439,000,000 and a consolidation cursor of 439,880,000, so the probe
// range narrows from 880,000 to 549,000 — plan cost 1,004,709 rather than
// 1,695,921 — and stays there as the chain advances.
//
// The predicate is deliberately conservative by omission. It does not filter on
// due time, validated_at, skipped metadata, repair state or pending retention
// state, because each of those is a reason a row is *not deleted yet* and must
// therefore hold the floor down rather than let it pass. Anything that leaves
// an undeleted single-block object blocks the watermark — including a crash
// between the S3 delete and the database update, which stalls the floor instead
// of stepping over the row. It fails safe in the only direction that matters.
//
// found is false when no such row exists, meaning the generation has no
// outstanding single-block work at or above minHeight.
// RetentionDueFloor returns the lowest height in [minHeight, endHeight) that is
// DUE at eligibilityCutoff, and whether such a row exists.
//
// It answers a different question from RetentionFloorWatermark, and the
// difference is load-bearing once the probe is windowed (INF-1416). The
// watermark pins to any undeleted row, due or not; a window anchored there can
// be empty while due work sits above it, and because the watermark is
// recomputed identically every tick the cron would idle forever instead of
// finding that work.
//
// This is deliberately a single-table query — a floor CANDIDATE, not a
// selectability proof. It matches a superset of the rows the due-cohort query
// can return (every predicate here also appears there), so the returned floor
// is never above the earliest selectable row. It can, however, be below it: a
// due row excluded by the cohort query's join-level predicates (canonical
// membership, metadata match, pending-retention or active-repair exclusion)
// still matches here. The cron compensates by advancing the search window when
// a probe at this floor selects nothing — see maxRetentionProbeAdvances. The
// join-level predicates are deliberately NOT folded in: with them the planner
// abandons early termination (it estimates one surviving row, materializes
// every due row through the joins, and sorts — measured >60s on
// robinhood-mainnet prod), which would reintroduce the very timeout this
// query exists to avoid.
//
// Every predicate of idx_block_consolidation_shadow_retention_due_generation's
// WHERE clause is stated verbatim so the partial index is provable. That gives
// the planner two good regimes, both measured on robinhood-mainnet prod
// (47.8M-row shadow table, 6.7M-row due backlog):
//
//   - backlog due: ascending (tag, height) first-match — 2.7ms
//   - nothing due: partial due index finds the empty due set — 0.08ms
//
// The [minHeight, endHeight) bound keeps the worst case inside the approved
// range rather than the table tail.
func (r *PostgresRepository) RetentionDueFloor(
	ctx context.Context,
	storageGeneration string,
	tag uint32,
	minHeight uint64,
	endHeight uint64,
	eligibilityCutoff time.Time,
) (uint64, bool, error) {
	if r == nil || r.db == nil {
		return 0, false, xerrors.New("postgres db is required")
	}
	return retentionDueFloor(ctx, r.db, storageGeneration, tag, minHeight, endHeight, eligibilityCutoff)
}

func retentionDueFloor(
	ctx context.Context,
	db retentionCohortQuerier,
	storageGeneration string,
	tag uint32,
	minHeight uint64,
	endHeight uint64,
	eligibilityCutoff time.Time,
) (uint64, bool, error) {
	if endHeight <= minHeight {
		return 0, false, nil
	}
	query := `
		SELECT MIN(shadow.height)
		FROM block_consolidation_shadow shadow
		WHERE shadow.tag = $1
			AND shadow.height >= $2
			AND shadow.height < $3
			AND shadow.validated_at IS NOT NULL
			AND shadow.single_block_delete_after IS NOT NULL
			AND shadow.single_block_delete_after <= $4
			AND shadow.single_block_object_deleted_at IS NULL
			AND shadow.single_block_object_key_main IS NOT NULL
			AND shadow.single_block_object_key_main <> ''
			AND shadow.consolidated_object_key_main IS NOT NULL
			AND shadow.consolidated_object_key_main <> ''
			AND ` + storageGenerationMatch(
		storageGeneration,
		"$5",
		"shadow.single_block_storage_generation",
		"shadow.consolidated_storage_generation",
	)

	args := []any{tag, minHeight, endHeight, eligibilityCutoff.UTC()}
	if storageGenerationIsBound(storageGeneration) {
		args = append(args, storageGeneration)
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, false, xerrors.Errorf("failed to query retention due floor: %w", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, false, xerrors.Errorf("failed to iterate retention due floor: %w", err)
		}
		return 0, false, nil
	}
	var height sql.NullInt64
	if err := rows.Scan(&height); err != nil {
		return 0, false, xerrors.Errorf("failed to scan retention due floor: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, false, xerrors.Errorf("failed to iterate retention due floor: %w", err)
	}
	if !height.Valid || height.Int64 < 0 {
		return 0, false, nil
	}
	return uint64(height.Int64), true, nil
}

func (r *PostgresRepository) RetentionFloorWatermark(
	ctx context.Context,
	storageGeneration string,
	tag uint32,
	minHeight uint64,
) (uint64, bool, error) {
	if r == nil || r.db == nil {
		return 0, false, xerrors.New("postgres db is required")
	}
	return retentionFloorWatermark(ctx, r.db, storageGeneration, tag, minHeight)
}

func retentionFloorWatermark(
	ctx context.Context,
	db retentionCohortQuerier,
	storageGeneration string,
	tag uint32,
	minHeight uint64,
) (uint64, bool, error) {
	// minHeight is what keeps this lookup affordable, and it is the load-bearing
	// part of this query rather than an optimisation. Without it the scan walks
	// (tag, height) from the bottom of the table through every legacy and
	// already-retired row: 58s on solana-mainnet prod versus 97ms bounded at the
	// approval floor.
	//
	// The bound is also exactly right semantically — retention may not delete
	// below approved_start_height, so work underneath it can never change where
	// the probe should start. That is why no dedicated index is needed here: a
	// partial index for this query was tried (migration 20260818000001) and
	// dropped in 20260818000002 because the planner also applied it to the
	// due-cohort probe and made that 2.5x slower. See those migrations before
	// reaching for an index again.
	query := `
		SELECT MIN(shadow.height)
		FROM block_consolidation_shadow shadow
		WHERE shadow.tag = $1
			AND shadow.height >= $2
			AND shadow.single_block_object_deleted_at IS NULL
			AND shadow.single_block_object_key_main IS NOT NULL
			AND shadow.single_block_object_key_main <> ''
			AND ` + storageGenerationMatch(storageGeneration, "$3", "shadow.single_block_storage_generation")

	args := []any{tag, minHeight}
	if storageGenerationIsBound(storageGeneration) {
		args = append(args, storageGeneration)
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, false, xerrors.Errorf("failed to query retention floor watermark: %w", err)
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, false, xerrors.Errorf("failed to read retention floor watermark: %w", err)
		}
		return 0, false, nil
	}
	// MIN over an empty set is SQL NULL, which is the no-outstanding-work case.
	var watermark sql.NullInt64
	if err := rows.Scan(&watermark); err != nil {
		return 0, false, xerrors.Errorf("failed to scan retention floor watermark: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, false, xerrors.Errorf("failed to iterate retention floor watermark: %w", err)
	}
	if !watermark.Valid {
		return 0, false, nil
	}
	if watermark.Int64 < 0 {
		return 0, false, xerrors.Errorf("retention floor watermark returned a negative height %d", watermark.Int64)
	}
	return uint64(watermark.Int64), true, nil
}
