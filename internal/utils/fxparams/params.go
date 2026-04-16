package fxparams

import (
	"math/rand"
	"time"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/config"
)

// Params provide the common dependencies.
// Usage:
//
//	MyParams struct {
//	  fx.In
//	  fxparams.Params
//	  ...
//	}
type Params struct {
	Config  *config.Config
	Logger  *zap.Logger
	Metrics tally.Scope
}

var Module = fx.Options(
	fx.Provide(func(config *config.Config, logger *zap.Logger, metrics tally.Scope) Params {
		rand.Seed(time.Now().UnixNano())
		return Params{
			Config:  config,
			Logger:  logger,
			Metrics: metrics,
		}
	}),
	fx.Invoke(applyGoMemLimit),
)

// applyGoMemLimit derives GOMEMLIMIT from the container's cgroup memory limit
// (90% by default). Go's GC is otherwise lazy about reclaiming memory and can
// OOM-kill a container long before it would voluntarily GC — a soft limit
// tells the runtime when to start reclaiming more aggressively. No-ops
// gracefully when not running in a cgroup (local dev on macOS, etc.).
func applyGoMemLimit(logger *zap.Logger) {
	limit, err := memlimit.SetGoMemLimitWithOpts()
	if err != nil {
		logger.Debug("GOMEMLIMIT auto-detect skipped", zap.Error(err))
		return
	}
	logger.Info("GOMEMLIMIT set from cgroup", zap.Int64("bytes", limit))
}
