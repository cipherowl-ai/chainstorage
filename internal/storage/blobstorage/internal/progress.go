package internal

import (
	"context"
	"io"
)

const consolidatedUploadProgressReadInterval = 64 * 1024 * 1024

type (
	ConsolidatedUploadProgress func(stage string, details ...any)

	consolidatedUploadProgressKey struct{}

	progressReadCloser struct {
		ctx       context.Context
		stage     string
		reader    io.ReadCloser
		totalRead uint64
		nextRead  uint64
	}
)

func WithConsolidatedUploadProgress(ctx context.Context, fn ConsolidatedUploadProgress) context.Context {
	if fn == nil {
		return ctx
	}
	return context.WithValue(ctx, consolidatedUploadProgressKey{}, fn)
}

func RecordConsolidatedUploadProgress(ctx context.Context, stage string, details ...any) {
	fn, ok := ctx.Value(consolidatedUploadProgressKey{}).(ConsolidatedUploadProgress)
	if !ok {
		return
	}
	fn(stage, details...)
}

func NewConsolidatedUploadProgressReadCloser(ctx context.Context, stage string, reader io.ReadCloser) io.ReadCloser {
	if _, ok := ctx.Value(consolidatedUploadProgressKey{}).(ConsolidatedUploadProgress); !ok {
		return reader
	}
	return &progressReadCloser{
		ctx:      ctx,
		stage:    stage,
		reader:   reader,
		nextRead: consolidatedUploadProgressReadInterval,
	}
}

func (r *progressReadCloser) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.totalRead += uint64(n)
		if r.totalRead >= r.nextRead {
			RecordConsolidatedUploadProgress(r.ctx, r.stage, r.totalRead)
			r.nextRead = r.totalRead + consolidatedUploadProgressReadInterval
		}
	}
	return n, err
}

func (r *progressReadCloser) Close() error {
	return r.reader.Close()
}
