package ethereum

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/ethereum/beacon"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
)

var Module = fx.Options(
	internal.NewParserBuilder("bsc", NewBscNativeParser).
		Build(),
	internal.NewParserBuilder("ethereum", NewEthereumNativeParser).
		SetRosettaParserFactory(NewEthereumRosettaParser).
		SetCheckerFactory(NewEthereumChecker).
		SetValidatorFactory(NewEthereumValidator).
		Build(),
	internal.NewParserBuilder("polygon", NewPolygonNativeParser).
		SetRosettaParserFactory(NewPolygonRosettaParser).
		SetCheckerFactory(NewPolygonChecker).
		SetValidatorFactory(NewPolygonValidator).
		Build(),
	internal.NewParserBuilder("avacchain", NewAvacchainNativeParser).
		SetCheckerFactory(NewAvacchainChecker).
		Build(),
	internal.NewParserBuilder("arbitrum", NewArbitrumNativeParser).
		SetCheckerFactory(NewArbitrumChecker).
		Build(),
	internal.NewParserBuilder("optimism", NewOptimismNativeParser).
		SetValidatorFactory(NewOptimismValidator).
		Build(),
	internal.NewParserBuilder("base", NewBaseNativeParser).
		SetRosettaParserFactory(NewBaseRosettaParser).
		SetCheckerFactory(NewBaseChecker).
		SetValidatorFactory(NewBaseValidator).
		Build(),
	internal.NewParserBuilder("fantom", NewFantomNativeParser).
		Build(),
	internal.NewParserBuilder("tron", NewTronNativeParser).
		SetValidatorFactory(NewBaseValidator).
		Build(),
	internal.NewParserBuilder("story", NewStoryNativeParser).
		SetValidatorFactory(NewStoryValidator).
		Build(),
	internal.NewParserBuilder("ethereumclassic", NewEthereumClassicNativeParser).
		SetValidatorFactory(NewEthereumClassicValidator).
		Build(),
	beacon.Module,
)
