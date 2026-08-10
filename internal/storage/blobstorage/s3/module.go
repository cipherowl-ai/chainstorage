package s3

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(fx.Annotated{
		Name:   "blobstorage/s3",
		Target: newFactory,
	}, fx.Private),
	fx.Provide(fx.Annotated{
		Name:   "blobstorage/s3/historical-single-block-downloader",
		Target: newHistoricalSingleBlockDownloaderFactory,
	}, fx.Private),
)
