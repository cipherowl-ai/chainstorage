package internal

import (
	"context"
	"fmt"
	"testing"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type fakeEventStorage struct {
	EventStorage
	events []*model.EventEntry
	err    error
}

func (f *fakeEventStorage) GetEventsByEventIdRange(ctx context.Context, eventTag uint32, minEventId int64, maxEventId int64) ([]*model.EventEntry, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.events[minEventId-model.EventIdStartValue : maxEventId-model.EventIdStartValue], nil
}

// add appends a BLOCK_ADDED event for the block at height, on the fork named by suffix.
func (f *fakeEventStorage) add(height uint64, suffix string, parentSuffix string) {
	f.append(api.BlockchainEvent_BLOCK_ADDED, height, suffix, parentSuffix)
}

// remove appends a BLOCK_REMOVED event for the block at height, on the fork named by suffix.
func (f *fakeEventStorage) remove(height uint64, suffix string, parentSuffix string) {
	f.append(api.BlockchainEvent_BLOCK_REMOVED, height, suffix, parentSuffix)
}

func (f *fakeEventStorage) append(eventType api.BlockchainEvent_Type, height uint64, suffix string, parentSuffix string) {
	f.events = append(f.events, &model.EventEntry{
		EventId:     int64(len(f.events)) + model.EventIdStartValue,
		EventType:   eventType,
		BlockHeight: height,
		BlockHash:   fakeHash(height, suffix),
		ParentHash:  fakeHash(height-1, parentSuffix),
	})
}

func (f *fakeEventStorage) maxEventId() int64 {
	return int64(len(f.events))
}

func fakeHash(height uint64, suffix string) string {
	return fmt.Sprintf("%d%s", height, suffix)
}

func TestGetTipEvent_LastAddedBlock(t *testing.T) {
	require := testutil.Require(t)

	storage := &fakeEventStorage{}
	storage.add(100, "a", "a")
	storage.add(101, "a", "a")

	tip, err := GetTipEvent(context.Background(), storage, 1, storage.maxEventId())

	require.NoError(err)
	require.Equal(int64(2), tip.EventId)
	require.Equal(fakeHash(101, "a"), tip.BlockHash)
}

func TestGetTipEvent_StreamEndsInRollback(t *testing.T) {
	require := testutil.Require(t)

	storage := &fakeEventStorage{}
	storage.add(100, "a", "a")
	storage.add(101, "a", "a")
	storage.add(102, "a", "a")
	storage.remove(102, "a", "a")
	storage.remove(101, "a", "a")

	tip, err := GetTipEvent(context.Background(), storage, 1, storage.maxEventId())

	require.NoError(err)
	require.Equal(uint64(100), tip.BlockHeight)
	require.Equal(fakeHash(100, "a"), tip.BlockHash)
}

func TestGetTipEvent_RollbackThenRegrowth(t *testing.T) {
	require := testutil.Require(t)

	storage := &fakeEventStorage{}
	storage.add(100, "a", "a")
	storage.add(101, "a", "a")
	storage.remove(101, "a", "a")
	storage.add(101, "b", "a")

	tip, err := GetTipEvent(context.Background(), storage, 1, storage.maxEventId())

	require.NoError(err)
	require.Equal(fakeHash(101, "b"), tip.BlockHash)
}

func TestGetTipEvent_RollbackLongerThanOneRead(t *testing.T) {
	require := testutil.Require(t)

	const rollback = uint64(tipEventBatchSize) + 50
	storage := &fakeEventStorage{}
	for height := uint64(100); height <= 100+rollback; height++ {
		storage.add(height, "a", "a")
	}
	for height := 100 + rollback; height > 100; height-- {
		storage.remove(height, "a", "a")
	}

	tip, err := GetTipEvent(context.Background(), storage, 1, storage.maxEventId())

	require.NoError(err)
	require.Equal(uint64(100), tip.BlockHeight)
}

func TestGetTipEvent_EverythingRemoved(t *testing.T) {
	require := testutil.Require(t)

	storage := &fakeEventStorage{}
	storage.add(100, "a", "a")
	storage.remove(100, "a", "a")

	_, err := GetTipEvent(context.Background(), storage, 1, storage.maxEventId())

	require.ErrorIs(err, errors.ErrItemNotFound)
}

func TestGetTipEvent_ReadFailure(t *testing.T) {
	require := testutil.Require(t)

	storage := &fakeEventStorage{err: xerrors.New("connection refused")}

	_, err := GetTipEvent(context.Background(), storage, 1, 5)

	require.Error(err)
	require.Contains(err.Error(), "connection refused")
}
