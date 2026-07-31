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
			tag uint32,
			startHeight uint64,
			endHeight uint64,
			eligibilityCutoff time.Time,
			limit int,
		) ([]RetentionCohort, []RetentionCohort, error)
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
	tag uint32,
	startHeight uint64,
	endHeight uint64,
	eligibilityCutoff time.Time,
	limit int,
) ([]RetentionCohort, bool, error) {
	if s == nil || s.repo == nil {
		return nil, false, xerrors.New("retention cohort repository is required")
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
