package retirement

import (
	"context"
	"math"
	"time"

	"golang.org/x/xerrors"
)

const MaxRetentionCohortsPerWorkflow = 250

type (
	RetentionCohort struct {
		ConsolidatedObjectKey string    `json:"consolidated_object_key"`
		StartHeight           uint64    `json:"start_height"`
		EndHeight             uint64    `json:"end_height"`
		RowCount              uint64    `json:"row_count"`
		EligibleAt            time.Time `json:"eligible_at"`
		Pending               bool      `json:"pending"`
	}

	// CohortRepository must return row-disjoint pending and due aggregates.
	// Both slices must come from one database snapshot. Selector merges
	// aggregates for the same consolidated object by summing their row counts.
	CohortRepository interface {
		ListRetentionCohorts(
			ctx context.Context,
			bucket string,
			storageGeneration string,
			tag uint32,
			startHeight uint64,
			endHeight uint64,
			eligibilityCutoff time.Time,
			limit int,
		) ([]RetentionCohort, []RetentionCohort, error)

		// RetentionFloorWatermark returns the lowest height at or above
		// minHeight that still holds an undeleted single-block object, and
		// whether such a row exists. It is part of this interface rather than
		// an optional one the implementation may satisfy: a missing watermark
		// silently stops the probe floor from advancing, and the resulting
		// range growth is exactly the failure this mechanism exists to
		// prevent. Compile-time enforcement is cheaper than discovering it in
		// production.
		RetentionFloorWatermark(
			ctx context.Context,
			storageGeneration string,
			tag uint32,
			minHeight uint64,
		) (uint64, bool, error)

		// RetentionDueFloor returns the lowest height in [minHeight, endHeight)
		// that is actually DUE at eligibilityCutoff, and whether such a row
		// exists.
		//
		// This is not the same question as RetentionFloorWatermark, and the
		// difference is load-bearing once the probe is windowed. The watermark
		// is deliberately conservative by omission and pins to any undeleted
		// row, due or not. A windowed probe anchored on the watermark can
		// therefore search [notDueRow, notDueRow+window) forever: it returns no
		// cohorts, the tick idles, the watermark is recomputed to the same
		// not-due row next tick, and due work above the window is never
		// discovered until the low row matures. Anchoring the window here
		// instead keeps the probe on due work.
		//
		// The returned floor is a CANDIDATE: never above the earliest
		// selectable row, but possibly below it, because join-level
		// selectability (canonical membership, metadata match, pending and
		// repair exclusion) is deliberately not evaluated here — see the
		// implementation for why folding it in reintroduces the timeout. The
		// cron advances the search window past floors that select nothing.
		RetentionDueFloor(
			ctx context.Context,
			storageGeneration string,
			tag uint32,
			minHeight uint64,
			endHeight uint64,
			eligibilityCutoff time.Time,
		) (uint64, bool, error)
	}

	Selector struct {
		repo CohortRepository
	}
)

func NewSelector(repo CohortRepository) *Selector {
	return &Selector{repo: repo}
}

func (s *Selector) Select(
	ctx context.Context,
	bucket string,
	storageGeneration string,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
	eligibilityCutoff time.Time,
	limit int,
) ([]RetentionCohort, bool, error) {
	if s == nil || s.repo == nil {
		return nil, false, xerrors.New("retention cohort repository is required")
	}
	if bucket == "" {
		return nil, false, xerrors.New("retention cohort bucket is required")
	}
	if !isValidStorageGeneration(storageGeneration) {
		return nil, false, xerrors.Errorf("unsupported retention cohort storage generation %q", storageGeneration)
	}
	if limit <= 0 || limit > MaxRetentionCohortsPerWorkflow {
		return nil, false, xerrors.Errorf(
			"retention cohort limit must be between 1 and %d: %d",
			MaxRetentionCohortsPerWorkflow,
			limit,
		)
	}
	if endHeight == 0 && startHeight != 0 {
		return nil, false, xerrors.New("retention selection end height is required when start height is set")
	}
	if endHeight != 0 && endHeight <= startHeight {
		return nil, false, xerrors.Errorf("invalid retention selection range [%d, %d)", startHeight, endHeight)
	}
	if eligibilityCutoff.IsZero() {
		return nil, false, xerrors.New("retention selection eligibility cutoff is required")
	}

	queryLimit := limit + 1
	pending, due, err := s.repo.ListRetentionCohorts(
		ctx,
		bucket,
		storageGeneration,
		tag,
		startHeight,
		endHeight,
		eligibilityCutoff,
		queryLimit,
	)
	if err != nil {
		return nil, false, xerrors.Errorf("failed to list retention cohorts: %w", err)
	}

	result := make([]RetentionCohort, 0, queryLimit)
	positions := make(map[string]int, queryLimit)
	appendOrMerge := func(cohort RetentionCohort) error {
		if cohort.ConsolidatedObjectKey == "" || cohort.EndHeight <= cohort.StartHeight || cohort.RowCount == 0 || cohort.EligibleAt.IsZero() {
			return xerrors.Errorf("invalid retention cohort: %+v", cohort)
		}
		if position, ok := positions[cohort.ConsolidatedObjectKey]; ok {
			existing := &result[position]
			if cohort.StartHeight < existing.StartHeight {
				existing.StartHeight = cohort.StartHeight
			}
			if cohort.EndHeight > existing.EndHeight {
				existing.EndHeight = cohort.EndHeight
			}
			if cohort.RowCount > math.MaxUint64-existing.RowCount {
				return xerrors.Errorf(
					"retention cohort row count overflow for object %q",
					cohort.ConsolidatedObjectKey,
				)
			}
			existing.RowCount += cohort.RowCount
			if cohort.EligibleAt.After(existing.EligibleAt) {
				existing.EligibleAt = cohort.EligibleAt
			}
			existing.Pending = existing.Pending || cohort.Pending
			return nil
		}
		if len(result) >= queryLimit {
			return nil
		}
		positions[cohort.ConsolidatedObjectKey] = len(result)
		result = append(result, cohort)
		return nil
	}

	for _, cohort := range pending {
		cohort.Pending = true
		if err := appendOrMerge(cohort); err != nil {
			return nil, false, err
		}
	}
	for _, cohort := range due {
		if err := appendOrMerge(cohort); err != nil {
			return nil, false, err
		}
	}
	hasMore := len(result) > limit
	if hasMore {
		result = result[:limit]
	}
	return result, hasMore, nil
}

// MaxRetentionPrimingLookahead bounds how many upcoming cohorts one Select pass
// primes safety observations for. Two workflow batches of lookahead lets the
// next continue-as-new run pass its quiescence gate on the first attempt.
const MaxRetentionPrimingLookahead = 2 * MaxRetentionCohortsPerWorkflow

// LookaheadKeys returns the distinct consolidated object keys of upcoming
// retention cohorts, beyond the per-workflow selection cap, so their safety
// quiescence clocks can be primed ahead of processing. It is advisory only:
// selection and execution never trust its output.
func (s *Selector) LookaheadKeys(
	ctx context.Context,
	bucket string,
	storageGeneration string,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
	eligibilityCutoff time.Time,
	limit int,
) ([]string, error) {
	if s == nil || s.repo == nil {
		return nil, xerrors.New("retention cohort repository is required")
	}
	if bucket == "" || eligibilityCutoff.IsZero() {
		return nil, xerrors.New("retention priming lookahead requires a bucket and eligibility cutoff")
	}
	if !isValidStorageGeneration(storageGeneration) {
		return nil, xerrors.Errorf("unsupported retention cohort storage generation %q", storageGeneration)
	}
	if limit <= 0 || limit > MaxRetentionPrimingLookahead {
		return nil, xerrors.Errorf(
			"retention priming lookahead limit must be between 1 and %d: %d",
			MaxRetentionPrimingLookahead,
			limit,
		)
	}
	pending, due, err := s.repo.ListRetentionCohorts(
		ctx,
		bucket,
		storageGeneration,
		tag,
		startHeight,
		endHeight,
		eligibilityCutoff,
		limit,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to list retention cohorts for priming: %w", err)
	}
	keys := make([]string, 0, len(pending)+len(due))
	seen := make(map[string]struct{}, len(pending)+len(due))
	for _, cohort := range append(pending, due...) {
		if cohort.ConsolidatedObjectKey == "" {
			continue
		}
		if _, duplicate := seen[cohort.ConsolidatedObjectKey]; duplicate {
			continue
		}
		seen[cohort.ConsolidatedObjectKey] = struct{}{}
		keys = append(keys, cohort.ConsolidatedObjectKey)
		if len(keys) >= limit {
			break
		}
	}
	return keys, nil
}

// FloorWatermark resolves the height the next probe should start from, given
// the operator's approved floor.
//
// The returned height is never below approvedStartHeight: that value is the
// authorization boundary and retention may not delete underneath it, so work
// found below it — a repair promoted into an old height, say — must not pull
// the probe down there. Everything between approvedStartHeight and the
// watermark has already been retired, so skipping it loses nothing.
//
// When the generation has no outstanding work at all, the approved floor is
// returned unchanged rather than some higher value. Advancing past the end of
// known work would put freshly consolidated rows below the floor before their
// retention delay expires, which is precisely how a floor strands data.
func (s *Selector) FloorWatermark(
	ctx context.Context,
	storageGeneration string,
	tag uint32,
	approvedStartHeight uint64,
) (uint64, error) {
	if s == nil || s.repo == nil {
		return 0, xerrors.New("retention cohort repository is required")
	}
	if !isValidStorageGeneration(storageGeneration) {
		return 0, xerrors.Errorf("unsupported retention cohort storage generation %q", storageGeneration)
	}
	watermark, found, err := s.repo.RetentionFloorWatermark(ctx, storageGeneration, tag, approvedStartHeight)
	if err != nil {
		return 0, xerrors.Errorf("failed to resolve retention floor watermark: %w", err)
	}
	if !found || watermark < approvedStartHeight {
		return approvedStartHeight, nil
	}
	return watermark, nil
}

// DueFloor resolves the lowest height in [floorHeight, endHeight) that is due
// at eligibilityCutoff. found is false when nothing is due there, which is the
// caller's signal to idle rather than to probe a window that cannot contain
// work. The result is clamped into [floorHeight, endHeight): a repository
// answer outside the requested range is a bug, and failing closed to
// "nothing due" beats anchoring a window outside the approved envelope.
func (s *Selector) DueFloor(
	ctx context.Context,
	storageGeneration string,
	tag uint32,
	floorHeight uint64,
	endHeight uint64,
	eligibilityCutoff time.Time,
) (uint64, bool, error) {
	if s == nil || s.repo == nil {
		return 0, false, xerrors.New("retention cohort repository is required")
	}
	if !isValidStorageGeneration(storageGeneration) {
		return 0, false, xerrors.Errorf("unsupported retention cohort storage generation %q", storageGeneration)
	}
	if endHeight <= floorHeight {
		return 0, false, nil
	}
	dueFloor, found, err := s.repo.RetentionDueFloor(ctx, storageGeneration, tag, floorHeight, endHeight, eligibilityCutoff)
	if err != nil {
		return 0, false, xerrors.Errorf("failed to resolve retention due floor: %w", err)
	}
	if !found || dueFloor >= endHeight {
		return 0, false, nil
	}
	if dueFloor < floorHeight {
		return floorHeight, true, nil
	}
	return dueFloor, true, nil
}
