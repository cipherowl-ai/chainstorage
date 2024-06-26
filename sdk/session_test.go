package sdk

import (
	"testing"

	"github.com/coinbase/chainstorage/internal/utils/pointer"

	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/gateway"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	"github.com/coinbase/chainstorage/sdk/services"
)

func TestNew(t *testing.T) {
	require := testutil.Require(t)
	manager := services.NewMockSystemManager()
	defer manager.Shutdown()

	for _, env := range []Env{EnvLocal, EnvDevelopment, EnvProduction} {
		session, err := New(manager, &Config{
			Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
			Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
			Env:        env,
		})
		require.NoError(err)
		require.NotNil(session)
		require.NotNil(session.Client())
		require.NotNil(session.Parser())
		require.Equal(uint32(0), session.Client().GetTag())
		require.Equal("", session.Client().GetClientID())
	}
}

func TestNew_Tag(t *testing.T) {
	require := testutil.Require(t)
	manager := services.NewMockSystemManager()
	defer manager.Shutdown()

	session, err := New(manager, &Config{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
		Env:        EnvDevelopment,
		Tag:        123,
	})
	require.NoError(err)
	require.NotNil(session)
	require.NotNil(session.Client())
	require.NotNil(session.Parser())
	require.Equal(uint32(123), session.Client().GetTag())
}

func TestNew_ClientID(t *testing.T) {
	require := testutil.Require(t)
	manager := services.NewMockSystemManager()
	defer manager.Shutdown()

	clientID := "fake-id"
	session, err := New(manager, &Config{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
		Env:        EnvDevelopment,
		ClientID:   clientID,
	})
	require.NoError(err)
	require.NotNil(session)
	require.NotNil(session.Client())
	require.NotNil(session.Parser())
	require.Equal(clientID, session.Client().GetClientID())
}

func TestNew_InvalidEnv(t *testing.T) {
	require := testutil.Require(t)
	manager := services.NewMockSystemManager()
	defer manager.Shutdown()

	_, err := New(manager, &Config{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
		Env:        "prod",
	})
	require.Error(err)
}

func TestNew_WithFx(t *testing.T) {
	require := testutil.Require(t)

	var session Session
	app := testapp.New(
		t,
		Module,
		parser.Module,
		downloader.Module,
		gateway.Module,
		fx.Populate(&session),
	)
	defer app.Close()

	require.NotNil(session)
	require.NotNil(session.Client())
	require.NotNil(session.Parser())
}

func TestNew_BlockValidation_Enabled(t *testing.T) {
	require := testutil.Require(t)
	manager := services.NewMockSystemManager()
	defer manager.Shutdown()

	session, err := New(manager, &Config{
		Blockchain:      common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:         common.Network_NETWORK_ETHEREUM_MAINNET,
		Env:             EnvDevelopment,
		BlockValidation: pointer.Ref(true),
	})
	require.NoError(err)
	require.NotNil(session)
	require.NotNil(session.Client())
	require.NotNil(session.Parser())
	require.True(session.Client().GetBlockValidation())
}

func TestNew_BlockValidation_Disabled(t *testing.T) {
	require := testutil.Require(t)
	manager := services.NewMockSystemManager()
	defer manager.Shutdown()

	session, err := New(manager, &Config{
		Blockchain:      common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:         common.Network_NETWORK_ETHEREUM_MAINNET,
		Env:             EnvDevelopment,
		BlockValidation: pointer.Ref(false),
	})
	require.NoError(err)
	require.NotNil(session)
	require.NotNil(session.Client())
	require.NotNil(session.Parser())
	require.False(session.Client().GetBlockValidation())
}
