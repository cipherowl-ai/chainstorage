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
	dueRetentionCohortOrderingByDueTime = "due.eligible_at, MIN(shadow.height), due.consolidated_object_key_main"
	// dueRetentionCohortOrderingByHeight walks a caller-supplied range in height
	// order so the selection is deterministic and prefix-shaped.
	dueRetentionCohortOrderingByHeight = "MIN(shadow.height), due.consolidated_object_key_main"
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
		WITH due_keys AS (
			SELECT
				shadow.consolidated_object_key_main,
				MIN(shadow.single_block_delete_after) AS eligible_at
			FROM block_consolidation_shadow shadow
			JOIN canonical_blocks due_canonical
				ON due_canonical.tag = shadow.tag
				AND due_canonical.height = shadow.height
				AND due_canonical.block_metadata_id = shadow.block_metadata_id
			JOIN block_metadata due_metadata
				ON due_metadata.id = due_canonical.block_metadata_id
				AND due_metadata.tag = due_canonical.tag
				AND due_metadata.height = due_canonical.height
			WHERE shadow.tag = $1
				AND shadow.validated_at IS NOT NULL
				AND shadow.single_block_delete_after IS NOT NULL
				AND shadow.single_block_delete_after <= $6
				AND shadow.single_block_object_deleted_at IS NULL
				AND shadow.single_block_object_key_main IS NOT NULL
				AND shadow.single_block_object_key_main <> ''
				AND shadow.consolidated_object_key_main IS NOT NULL
				AND shadow.consolidated_object_key_main <> ''
				AND due_metadata.skipped = FALSE
				AND due_metadata.object_format = $4
				AND due_metadata.object_key_main = shadow.consolidated_object_key_main
				AND %s
				AND ($3::BIGINT = 0 OR (shadow.height >= $2 AND shadow.height < $3))
				AND NOT EXISTS (
					SELECT 1
					FROM block_single_block_retention retention
					WHERE retention.block_metadata_id = shadow.block_metadata_id
						AND retention.tag = shadow.tag
						AND retention.state IN (` + pendingRetirementStatesSQL + `)
				)
			GROUP BY shadow.consolidated_object_key_main
		)
		SELECT
			due.consolidated_object_key_main,
			MIN(shadow.height),
			MAX(shadow.height) + 1,
			COUNT(*),
			MAX(shadow.single_block_delete_after)
		FROM due_keys due
		JOIN block_consolidation_shadow shadow
			ON shadow.tag = $1
			AND shadow.consolidated_object_key_main = due.consolidated_object_key_main
		JOIN canonical_blocks canonical
			ON canonical.tag = shadow.tag
			AND canonical.height = shadow.height
			AND canonical.block_metadata_id = shadow.block_metadata_id
		JOIN block_metadata metadata
			ON metadata.id = canonical.block_metadata_id
			AND metadata.tag = canonical.tag
			AND metadata.height = canonical.height
		WHERE shadow.validated_at IS NOT NULL
			AND shadow.single_block_delete_after IS NOT NULL
			AND shadow.single_block_object_deleted_at IS NULL
			AND shadow.single_block_object_key_main IS NOT NULL
			AND shadow.single_block_object_key_main <> ''
			AND metadata.skipped = FALSE
			AND metadata.object_format = $4
			AND %s
			AND ($3::BIGINT = 0 OR (shadow.height >= $2 AND shadow.height < $3))
			AND metadata.object_key_main = shadow.consolidated_object_key_main
			AND NOT EXISTS (
				SELECT 1
				FROM block_single_block_retention retention
				WHERE retention.block_metadata_id = shadow.block_metadata_id
					AND retention.tag = shadow.tag
					AND retention.state IN (` + pendingRetirementStatesSQL + `)
			)
			AND NOT EXISTS (
				SELECT 1
				FROM cscb_repair_manifest repair
				WHERE repair.tag = shadow.tag
					AND repair.state <> 'completed'
					AND (
						repair.old_consolidated_object_key_main = shadow.consolidated_object_key_main
						OR repair.new_consolidated_object_key_main = shadow.consolidated_object_key_main
					)
			)
		GROUP BY due.consolidated_object_key_main, due.eligible_at
		HAVING MAX(shadow.single_block_delete_after) <= $6
		ORDER BY %s
		LIMIT $5`
	query := fmt.Sprintf(
		queryTemplate,
		storageGenerationMatch(
			storageGeneration,
			"$7",
			"due_metadata.storage_generation",
			"shadow.single_block_storage_generation",
			"shadow.consolidated_storage_generation",
		),
		storageGenerationMatch(
			storageGeneration,
			"$7",
			"metadata.storage_generation",
			"shadow.single_block_storage_generation",
			"shadow.consolidated_storage_generation",
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
