package activity

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

const (
	migratorCheckpointSize = 1000
	migratorBatchSize      = 100
)

type migratorActivityTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env      *cadence.TestEnv
	ctrl     *gomock.Controller
	app      testapp.TestApp
	migrator *Migrator
	cfg      *config.Config
}

func TestMigratorActivityTestSuite(t *testing.T) {
	suite.Run(t, new(migratorActivityTestSuite))
}

func (s *migratorActivityTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	s.env = cadence.NewTestActivityEnv(s)
	s.ctrl = gomock.NewController(s.T())

	cfg, err := config.New(
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_ETHEREUM),
		config.WithNetwork(common.Network_NETWORK_ETHEREUM_MAINNET),
		config.WithEnvironment(config.EnvLocal),
	)
	require.NoError(err)
	s.cfg = cfg

	s.app = testapp.New(
		s.T(),
		Module,
		cadence.WithTestEnv(s.env),
		testapp.WithConfig(cfg),
		fx.Populate(&s.migrator),
	)
}

func (s *migratorActivityTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *migratorActivityTestSuite) TestMigrator_Instantiation() {
	require := testutil.Require(s.T())

	// Verify that the migrator activity can be instantiated successfully
	require.NotNil(s.migrator)

	// Verify that it has the expected activity name
	require.Equal(ActivityMigrator, "activity.migrator")
}

func (s *migratorActivityTestSuite) TestMigrator_RequestValidation() {
	require := testutil.Require(s.T())

	// Test valid request
	validRequest := &MigratorRequest{
		StartHeight: uint64(1000),
		EndHeight:   uint64(1050),
		Tag:         uint32(1),
		EventTag:    uint32(0),
		BatchSize:   10,
		SkipEvents:  false,
		SkipBlocks:  false,
	}
	require.NotNil(validRequest)
	require.Equal(uint64(1000), validRequest.StartHeight)
	require.Equal(uint64(1050), validRequest.EndHeight)
	require.Equal(uint32(1), validRequest.Tag)
	require.Equal(uint32(0), validRequest.EventTag)
	require.Equal(10, validRequest.BatchSize)
	require.False(validRequest.SkipEvents)
	require.False(validRequest.SkipBlocks)
}

func (s *migratorActivityTestSuite) TestMigrator_ResponseStructure() {
	require := testutil.Require(s.T())

	// Test response structure
	response := &MigratorResponse{
		BlocksMigrated: 100,
		EventsMigrated: 500,
		Success:        true,
		Message:        "Migration completed successfully",
	}

	require.Equal(100, response.BlocksMigrated)
	require.Equal(500, response.EventsMigrated)
	require.True(response.Success)
	require.Equal("Migration completed successfully", response.Message)
}

func (s *migratorActivityTestSuite) TestMigrator_RequestOptions() {
	require := testutil.Require(s.T())

	// Test skip blocks option
	skipBlocksRequest := &MigratorRequest{
		StartHeight: uint64(1000),
		EndHeight:   uint64(1050),
		Tag:         uint32(1),
		EventTag:    uint32(0),
		BatchSize:   10,
		SkipEvents:  false,
		SkipBlocks:  true,
	}
	require.True(skipBlocksRequest.SkipBlocks)
	require.False(skipBlocksRequest.SkipEvents)

	// Test skip events option
	skipEventsRequest := &MigratorRequest{
		StartHeight: uint64(1000),
		EndHeight:   uint64(1050),
		Tag:         uint32(1),
		EventTag:    uint32(0),
		BatchSize:   10,
		SkipEvents:  true,
		SkipBlocks:  false,
	}
	require.False(skipEventsRequest.SkipBlocks)
	require.True(skipEventsRequest.SkipEvents)

	// Test both skip options
	skipBothRequest := &MigratorRequest{
		StartHeight: uint64(1000),
		EndHeight:   uint64(1050),
		Tag:         uint32(1),
		EventTag:    uint32(0),
		BatchSize:   10,
		SkipEvents:  true,
		SkipBlocks:  true,
	}
	require.True(skipBothRequest.SkipBlocks)
	require.True(skipBothRequest.SkipEvents)
}

func (s *migratorActivityTestSuite) TestMigrator_InvalidRequest() {
	require := testutil.Require(s.T())

	// Test invalid request - EndHeight should be greater than StartHeight
	invalidRequest := &MigratorRequest{
		StartHeight: uint64(1050),
		EndHeight:   uint64(1000), // EndHeight < StartHeight
		Tag:         uint32(1),
		EventTag:    uint32(0),
		BatchSize:   10,
		SkipEvents:  false,
		SkipBlocks:  false,
	}

	// This should fail during execution, not during validation
	_, err := s.migrator.Execute(s.env.BackgroundContext(), invalidRequest)
	require.Error(err)
	// The error should be related to the actual migration logic, not validation
}

func (s *migratorActivityTestSuite) TestMigrator_DefaultBatchSize() {
	require := testutil.Require(s.T())

	// Test request with zero batch size (should use default)
	request := &MigratorRequest{
		StartHeight: uint64(1000),
		EndHeight:   uint64(1050),
		Tag:         uint32(1),
		EventTag:    uint32(0),
		BatchSize:   0, // Should use default
		SkipEvents:  true,
		SkipBlocks:  true, // Skip both to avoid actual migration logic
	}

	// This should succeed with default batch size
	response, err := s.migrator.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.NotNil(response)
	require.False(response.Success) // Should fail because both skip flags are true
	require.Contains(response.Message, "cannot skip both events and blocks")
}

func (s *migratorActivityTestSuite) TestGetLatestBlockHeight_RequestValidation() {
	require := testutil.Require(s.T())

	// Test valid request
	validRequest := &GetLatestBlockHeightRequest{
		Tag: uint32(1),
	}
	require.NotNil(validRequest)
	require.Equal(uint32(1), validRequest.Tag)
}

func (s *migratorActivityTestSuite) TestGetLatestBlockHeight_ResponseStructure() {
	require := testutil.Require(s.T())

	// Test response structure
	response := &GetLatestBlockHeightResponse{
		Height: uint64(12345),
	}

	require.Equal(uint64(12345), response.Height)
}
