package internal

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

type infiniteReader struct{}

func (infiniteReader) Read(p []byte) (int, error) {
	return len(p), nil
}

func TestConsolidatedUploadProgressReadCloserReportsThreshold(t *testing.T) {
	var progressBytes []uint64
	ctx := WithConsolidatedUploadProgress(context.Background(), func(stage string, details ...any) {
		require.Equal(t, "s3_put_read", stage)
		require.Len(t, details, 1)
		progressBytes = append(progressBytes, details[0].(uint64))
	})

	reader := NewConsolidatedUploadProgressReadCloser(
		ctx,
		"s3_put_read",
		io.NopCloser(io.LimitReader(infiniteReader{}, int64(consolidatedUploadProgressReadInterval+1))),
	)
	copied, err := io.Copy(io.Discard, reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, int64(consolidatedUploadProgressReadInterval+1), copied)
	require.Len(t, progressBytes, 1)
	require.GreaterOrEqual(t, progressBytes[0], uint64(consolidatedUploadProgressReadInterval))
}
