package retirement

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeCohortRepository struct {
	pending    []RetentionCohort
	due        []RetentionCohort
	pendingErr error
	dueErr     error
}

func (r *fakeCohortRepository) ListPendingRetentionCohorts(
	context.Context,
	uint32,
	uint64,
	uint64,
	int,
) ([]RetentionCohort, error) {
	return r.pending, r.pendingErr
}

func (r *fakeCohortRepository) ListDueRetentionCohorts(
	context.Context,
	uint32,
	uint64,
	uint64,
	int,
) ([]RetentionCohort, error) {
	return r.due, r.dueErr
}

func TestSelectorPrioritizesPendingAndMergesDueRange(t *testing.T) {
	now := time.Date(2026, 7, 23, 0, 0, 0, 0, time.UTC)
	repo := &fakeCohortRepository{
		pending: []RetentionCohort{{
			ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
			StartHeight:           102,
			EndHeight:             104,
			RowCount:              2,
			EligibleAt:            now.Add(-time.Hour),
		}},
		due: []RetentionCohort{
			{
				ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
				StartHeight:           100,
				EndHeight:             110,
				RowCount:              9,
				EligibleAt:            now.Add(-2 * time.Hour),
			},
			{
				ConsolidatedObjectKey: "consolidated/b.cscb.zstd",
				StartHeight:           200,
				EndHeight:             210,
				RowCount:              10,
				EligibleAt:            now,
			},
		},
	}

	cohorts, hasMore, err := NewSelector(repo).Select(context.Background(), 2, 0, 0, 2)
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Equal(t, []RetentionCohort{
		{
			ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
			StartHeight:           100,
			EndHeight:             110,
			RowCount:              9,
			EligibleAt:            now.Add(-time.Hour),
			Pending:               true,
		},
		{
			ConsolidatedObjectKey: "consolidated/b.cscb.zstd",
			StartHeight:           200,
			EndHeight:             210,
			RowCount:              10,
			EligibleAt:            now,
		},
	}, cohorts)
}

func TestSelectorRejectsInvalidLimitAndCohort(t *testing.T) {
	_, _, err := NewSelector(&fakeCohortRepository{}).Select(context.Background(), 2, 0, 0, 0)
	require.ErrorContains(t, err, "between 1 and")

	repo := &fakeCohortRepository{
		due: []RetentionCohort{{
			ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
			StartHeight:           100,
			EndHeight:             100,
			RowCount:              1,
			EligibleAt:            time.Now(),
		}},
	}
	_, _, err = NewSelector(repo).Select(context.Background(), 2, 0, 0, 1)
	require.ErrorContains(t, err, "invalid retention cohort")
}

func TestSelectorPropagatesRepositoryErrors(t *testing.T) {
	repo := &fakeCohortRepository{pendingErr: errors.New("database unavailable")}
	_, _, err := NewSelector(repo).Select(context.Background(), 2, 0, 0, 1)
	require.ErrorContains(t, err, "database unavailable")
}

func TestSelectorValidatesOptionalHeightRange(t *testing.T) {
	selector := NewSelector(&fakeCohortRepository{})

	_, _, err := selector.Select(context.Background(), 2, 100, 0, 1)
	require.ErrorContains(t, err, "end height is required")

	_, _, err = selector.Select(context.Background(), 2, 100, 100, 1)
	require.ErrorContains(t, err, "invalid retention selection range")

	_, _, err = selector.Select(context.Background(), 2, 100, 200, 1)
	require.NoError(t, err)
}

func TestSelectorReportsRemainingBacklog(t *testing.T) {
	now := time.Now().UTC()
	repo := &fakeCohortRepository{
		due: []RetentionCohort{
			{
				ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
				StartHeight:           100,
				EndHeight:             110,
				RowCount:              10,
				EligibleAt:            now,
			},
			{
				ConsolidatedObjectKey: "consolidated/b.cscb.zstd",
				StartHeight:           200,
				EndHeight:             210,
				RowCount:              10,
				EligibleAt:            now,
			},
		},
	}

	cohorts, hasMore, err := NewSelector(repo).Select(context.Background(), 2, 0, 0, 1)
	require.NoError(t, err)
	require.True(t, hasMore)
	require.Len(t, cohorts, 1)
	require.Equal(t, "consolidated/a.cscb.zstd", cohorts[0].ConsolidatedObjectKey)
}
