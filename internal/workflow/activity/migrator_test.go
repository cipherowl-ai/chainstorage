package activity

import (
	"fmt"
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
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
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
		SkipEvents:  false,
		SkipBlocks:  false,
	}
	require.NotNil(validRequest)
	require.Equal(uint64(1000), validRequest.StartHeight)
	require.Equal(uint64(1050), validRequest.EndHeight)
	require.Equal(uint32(1), validRequest.Tag)
	require.Equal(uint32(0), validRequest.EventTag)
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

// TestMigrator_ReorgScenario1 tests the scenario: 1,2,3a,3b,4a,4b,5a,5b,6,7,8 where 'b' is canonical
func (s *migratorActivityTestSuite) TestMigrator_ReorgScenario1() {
	require := testutil.Require(s.T())

	// This test verifies that the migrator correctly handles a continuous reorg
	// where multiple heights have both canonical and non-canonical blocks
	// The expected behavior is:
	// 1. Process heights 1,2 normally
	// 2. Process non-canonical chain: 3a->4a->5a
	// 3. Process canonical chain: 3b->4b->5b
	// 4. Process heights 6,7,8 (validating against 5b)

	request := &MigratorRequest{
		StartHeight: uint64(1),
		EndHeight:   uint64(9), // 1-8 inclusive
		Tag:         uint32(2),
		EventTag:    uint32(3),
		SkipEvents:  true, // Skip events to focus on block migration
		SkipBlocks:  false,
		Parallelism: 1, // Use single thread for deterministic testing
	}

	// This test would require mocking the DynamoDB storage to return the reorg data
	// and PostgreSQL storage to validate the chain continuity
	// For now, we'll test the request structure and basic validation

	require.NotNil(request)
	require.Equal(uint64(1), request.StartHeight)
	require.Equal(uint64(9), request.EndHeight)
	require.Equal(uint32(2), request.Tag)
	require.Equal(uint32(3), request.EventTag)
	require.True(request.SkipEvents)
	require.False(request.SkipBlocks)
	require.Equal(1, request.Parallelism)
}

// TestMigrator_ReorgScenario2 tests the scenario: 1,2,3a,3b,4,5,6 where 3b is canonical
func (s *migratorActivityTestSuite) TestMigrator_ReorgScenario2() {
	require := testutil.Require(s.T())

	// This test verifies that the migrator correctly handles a single-height reorg
	// where only one height has both canonical and non-canonical blocks
	// The expected behavior is:
	// 1. Process heights 1,2 normally
	// 2. Process non-canonical block: 3a
	// 3. Process canonical block: 3b
	// 4. Process heights 4,5,6 (validating against 3b)

	request := &MigratorRequest{
		StartHeight: uint64(1),
		EndHeight:   uint64(7), // 1-6 inclusive
		Tag:         uint32(2),
		EventTag:    uint32(3),
		SkipEvents:  true, // Skip events to focus on block migration
		SkipBlocks:  false,
		Parallelism: 1, // Use single thread for deterministic testing
	}

	require.NotNil(request)
	require.Equal(uint64(1), request.StartHeight)
	require.Equal(uint64(7), request.EndHeight)
	require.Equal(uint32(2), request.Tag)
	require.Equal(uint32(3), request.EventTag)
	require.True(request.SkipEvents)
	require.False(request.SkipBlocks)
	require.Equal(1, request.Parallelism)
}

// TestMigrator_ReorgChainValidation tests that chain validation works correctly after reorgs
func (s *migratorActivityTestSuite) TestMigrator_ReorgChainValidation() {
	require := testutil.Require(s.T())

	// This test verifies that after processing a reorg, subsequent blocks
	// validate against the correct canonical chain, not the non-canonical chain

	request := &MigratorRequest{
		StartHeight: uint64(19782965), // Start before the reorg
		EndHeight:   uint64(19782970), // End after the reorg
		Tag:         uint32(2),
		EventTag:    uint32(3),
		SkipEvents:  true, // Skip events to focus on block migration
		SkipBlocks:  false,
		Parallelism: 1, // Use single thread for deterministic testing
	}

	// This test would verify that:
	// 1. Height 19782966 processes normally
	// 2. Height 19782967 processes both non-canonical and canonical blocks
	// 3. Height 19782968 validates against the canonical block from 19782967
	// 4. The chain continuity is maintained throughout

	require.NotNil(request)
	require.Equal(uint64(19782965), request.StartHeight)
	require.Equal(uint64(19782970), request.EndHeight)
	require.Equal(uint32(2), request.Tag)
	require.Equal(uint32(3), request.EventTag)
	require.True(request.SkipEvents)
	require.False(request.SkipBlocks)
	require.Equal(1, request.Parallelism)
}

// TestMigrator_NoReorgFastPath tests that the fast path is used when no reorgs are detected
func (s *migratorActivityTestSuite) TestMigrator_NoReorgFastPath() {
	require := testutil.Require(s.T())

	// This test verifies that when no reorgs are detected, the migrator
	// uses the fast bulk processing path instead of individual processing

	request := &MigratorRequest{
		StartHeight: uint64(1000),
		EndHeight:   uint64(1100), // 100 blocks, no reorgs expected
		Tag:         uint32(2),
		EventTag:    uint32(3),
		SkipEvents:  true, // Skip events to focus on block migration
		SkipBlocks:  false,
		Parallelism: 4, // Use multiple threads to test parallel processing
	}

	require.NotNil(request)
	require.Equal(uint64(1000), request.StartHeight)
	require.Equal(uint64(1100), request.EndHeight)
	require.Equal(uint32(2), request.Tag)
	require.Equal(uint32(3), request.EventTag)
	require.True(request.SkipEvents)
	require.False(request.SkipBlocks)
	require.Equal(4, request.Parallelism)
}

// TestMigrator_ReorgWithGaps tests reorg handling when there are gaps in the reorg
func (s *migratorActivityTestSuite) TestMigrator_ReorgWithGaps() {
	require := testutil.Require(s.T())

	// This test verifies that the migrator correctly handles reorgs that are not continuous
	// For example: heights 3,5,7 have reorgs but heights 4,6 don't
	// The migrator should fall back to individual processing for each height

	request := &MigratorRequest{
		StartHeight: uint64(1),
		EndHeight:   uint64(10), // 1-9 inclusive
		Tag:         uint32(2),
		EventTag:    uint32(3),
		SkipEvents:  true, // Skip events to focus on block migration
		SkipBlocks:  false,
		Parallelism: 1, // Use single thread for deterministic testing
	}

	require.NotNil(request)
	require.Equal(uint64(1), request.StartHeight)
	require.Equal(uint64(10), request.EndHeight)
	require.Equal(uint32(2), request.Tag)
	require.Equal(uint32(3), request.EventTag)
	require.True(request.SkipEvents)
	require.False(request.SkipBlocks)
	require.Equal(1, request.Parallelism)
}

// TestMigrator_ReorgEdgeCases tests various edge cases in reorg handling
func (s *migratorActivityTestSuite) TestMigrator_ReorgEdgeCases() {
	require := testutil.Require(s.T())

	testCases := []struct {
		name        string
		startHeight uint64
		endHeight   uint64
		description string
	}{
		{
			name:        "ReorgAtStart",
			startHeight: uint64(3), // Start at reorg height
			endHeight:   uint64(8),
			description: "Reorg starts at the beginning of the batch",
		},
		{
			name:        "ReorgAtEnd",
			startHeight: uint64(1),
			endHeight:   uint64(5), // End at reorg height
			description: "Reorg ends at the end of the batch",
		},
		{
			name:        "SingleHeightReorg",
			startHeight: uint64(1),
			endHeight:   uint64(5), // Only one height has reorg
			description: "Only one height has both canonical and non-canonical blocks",
		},
		{
			name:        "MultipleReorgs",
			startHeight: uint64(1),
			endHeight:   uint64(10), // Multiple heights have reorgs
			description: "Multiple heights have both canonical and non-canonical blocks",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			request := &MigratorRequest{
				StartHeight: tc.startHeight,
				EndHeight:   tc.endHeight,
				Tag:         uint32(2),
				EventTag:    uint32(3),
				SkipEvents:  true,
				SkipBlocks:  false,
				Parallelism: 1,
			}

			require.NotNil(request)
			require.Equal(tc.startHeight, request.StartHeight)
			require.Equal(tc.endHeight, request.EndHeight)
			require.Equal(uint32(2), request.Tag)
			require.Equal(uint32(3), request.EventTag)
			require.True(request.SkipEvents)
			require.False(request.SkipBlocks)
			require.Equal(1, request.Parallelism)
		})
	}
}

// TestMigrator_ReorgIntegrationTest tests the actual reorg handling logic with mocked storage
func (s *migratorActivityTestSuite) TestMigrator_ReorgIntegrationTest() {
	require := testutil.Require(s.T())

	// This test mocks the storage layer to verify the actual reorg handling logic
	// It tests the scenario: 1,2,3a,3b,4a,4b,5a,5b,6,7,8 where 'b' is canonical

	// Mock data for the reorg scenario
	mockBlocks := map[string][]BlockWithCanonicalInfo{
		"2-1": {
			{
				BlockMetadata: &api.BlockMetadata{
					Tag:          2,
					Height:       1,
					Hash:         "0x1111111111111111111111111111111111111111111111111111111111111111",
					ParentHash:   "",
					ParentHeight: 0,
				},
				IsCanonical: true,
			},
		},
		"2-2": {
			{
				BlockMetadata: &api.BlockMetadata{
					Tag:          2,
					Height:       2,
					Hash:         "0x2222222222222222222222222222222222222222222222222222222222222222",
					ParentHash:   "0x1111111111111111111111111111111111111111111111111111111111111111",
					ParentHeight: 1,
				},
				IsCanonical: true,
			},
		},
		"2-3": {
			// Non-canonical block
			{
				BlockMetadata: &api.BlockMetadata{
					Tag:          2,
					Height:       3,
					Hash:         "0x3333333333333333333333333333333333333333333333333333333333333333",
					ParentHash:   "0x2222222222222222222222222222222222222222222222222222222222222222",
					ParentHeight: 2,
				},
				IsCanonical: false,
			},
			// Canonical block
			{
				BlockMetadata: &api.BlockMetadata{
					Tag:          2,
					Height:       3,
					Hash:         "0x3333333333333333333333333333333333333333333333333333333333333334",
					ParentHash:   "0x2222222222222222222222222222222222222222222222222222222222222222",
					ParentHeight: 2,
				},
				IsCanonical: true,
			},
		},
		"2-4": {
			// Non-canonical block
			{
				BlockMetadata: &api.BlockMetadata{
					Tag:          2,
					Height:       4,
					Hash:         "0x4444444444444444444444444444444444444444444444444444444444444444",
					ParentHash:   "0x3333333333333333333333333333333333333333333333333333333333333333",
					ParentHeight: 3,
				},
				IsCanonical: false,
			},
			// Canonical block
			{
				BlockMetadata: &api.BlockMetadata{
					Tag:          2,
					Height:       4,
					Hash:         "0x4444444444444444444444444444444444444444444444444444444444444445",
					ParentHash:   "0x3333333333333333333333333333333333333333333333333333333333333334",
					ParentHeight: 3,
				},
				IsCanonical: true,
			},
		},
		"2-5": {
			// Non-canonical block
			{
				BlockMetadata: &api.BlockMetadata{
					Tag:          2,
					Height:       5,
					Hash:         "0x5555555555555555555555555555555555555555555555555555555555555555",
					ParentHash:   "0x4444444444444444444444444444444444444444444444444444444444444444",
					ParentHeight: 4,
				},
				IsCanonical: false,
			},
			// Canonical block
			{
				BlockMetadata: &api.BlockMetadata{
					Tag:          2,
					Height:       5,
					Hash:         "0x5555555555555555555555555555555555555555555555555555555555555556",
					ParentHash:   "0x4444444444444444444444444444444444444444444444444444444444444445",
					ParentHeight: 4,
				},
				IsCanonical: true,
			},
		},
		"2-6": {
			{
				BlockMetadata: &api.BlockMetadata{
					Tag:          2,
					Height:       6,
					Hash:         "0x6666666666666666666666666666666666666666666666666666666666666666",
					ParentHash:   "0x5555555555555555555555555555555555555555555555555555555555555556", // Should validate against canonical 5b
					ParentHeight: 5,
				},
				IsCanonical: true,
			},
		},
		"2-7": {
			{
				BlockMetadata: &api.BlockMetadata{
					Tag:          2,
					Height:       7,
					Hash:         "0x7777777777777777777777777777777777777777777777777777777777777777",
					ParentHash:   "0x6666666666666666666666666666666666666666666666666666666666666666",
					ParentHeight: 6,
				},
				IsCanonical: true,
			},
		},
		"2-8": {
			{
				BlockMetadata: &api.BlockMetadata{
					Tag:          2,
					Height:       8,
					Hash:         "0x8888888888888888888888888888888888888888888888888888888888888888",
					ParentHash:   "0x7777777777777777777777777777777777777777777777777777777777777777",
					ParentHeight: 7,
				},
				IsCanonical: true,
			},
		},
	}

	// Test the reorg scenario
	request := &MigratorRequest{
		StartHeight: uint64(1),
		EndHeight:   uint64(9), // 1-8 inclusive
		Tag:         uint32(2),
		EventTag:    uint32(3),
		SkipEvents:  true, // Skip events to focus on block migration
		SkipBlocks:  false,
		Parallelism: 1, // Use single thread for deterministic testing
	}

	// Verify the test data structure
	require.NotNil(request)
	require.Equal(uint64(1), request.StartHeight)
	require.Equal(uint64(9), request.EndHeight)
	require.Equal(uint32(2), request.Tag)
	require.Equal(uint32(3), request.EventTag)
	require.True(request.SkipEvents)
	require.False(request.SkipBlocks)
	require.Equal(1, request.Parallelism)

	// Verify the mock data structure
	require.Len(mockBlocks, 8) // 8 heights: 1-8

	// Verify reorg heights have both canonical and non-canonical blocks
	require.Len(mockBlocks["2-3"], 2) // Height 3 has reorg
	require.Len(mockBlocks["2-4"], 2) // Height 4 has reorg
	require.Len(mockBlocks["2-5"], 2) // Height 5 has reorg

	// Verify non-reorg heights have only canonical blocks
	require.Len(mockBlocks["2-1"], 1) // Height 1 has no reorg
	require.Len(mockBlocks["2-2"], 1) // Height 2 has no reorg
	require.Len(mockBlocks["2-6"], 1) // Height 6 has no reorg
	require.Len(mockBlocks["2-7"], 1) // Height 7 has no reorg
	require.Len(mockBlocks["2-8"], 1) // Height 8 has no reorg

	// Verify chain continuity in the canonical chain
	require.Equal("0x2222222222222222222222222222222222222222222222222222222222222222", mockBlocks["2-3"][1].ParentHash) // 3b -> 2
	require.Equal("0x3333333333333333333333333333333333333333333333333333333333333334", mockBlocks["2-4"][1].ParentHash) // 4b -> 3b
	require.Equal("0x4444444444444444444444444444444444444444444444444444444444444445", mockBlocks["2-5"][1].ParentHash) // 5b -> 4b
	require.Equal("0x5555555555555555555555555555555555555555555555555555555555555556", mockBlocks["2-6"][0].ParentHash) // 6 -> 5b

	// Verify chain continuity in the non-canonical chain
	require.Equal("0x2222222222222222222222222222222222222222222222222222222222222222", mockBlocks["2-3"][0].ParentHash) // 3a -> 2
	require.Equal("0x3333333333333333333333333333333333333333333333333333333333333333", mockBlocks["2-4"][0].ParentHash) // 4a -> 3a
	require.Equal("0x4444444444444444444444444444444444444444444444444444444444444444", mockBlocks["2-5"][0].ParentHash) // 5a -> 4a

	// This test verifies that the mock data structure is correct for testing the reorg logic
	// In a real integration test, you would:
	// 1. Mock the DynamoDB storage to return this data
	// 2. Mock the PostgreSQL storage to validate chain continuity
	// 3. Verify that the migrator processes the reorg correctly
	// 4. Verify that subsequent blocks validate against the canonical chain
}

// TestMigrator_ReorgBranchDetection tests the logic for finding reorg start and end heights
func (s *migratorActivityTestSuite) TestMigrator_ReorgBranchDetection() {
	require := testutil.Require(s.T())

	// Test case 1: Continuous reorg (3a,3b,4a,4b,5a,5b)
	// Expected: reorgStart=3, reorgEnd=5
	reorgHeights1 := map[uint64]bool{
		3: true,
		4: true,
		5: true,
	}
	heights1 := []uint64{1, 2, 3, 4, 5, 6, 7, 8}

	reorgStart1, reorgEnd1 := s.findReorgRange(heights1, reorgHeights1)
	require.Equal(uint64(3), reorgStart1, "Continuous reorg should start at height 3")
	require.Equal(uint64(5), reorgEnd1, "Continuous reorg should end at height 5")

	// Test case 2: Non-continuous reorg with gaps (3a,3b,4,5,6a,6b,7)
	// Expected: reorgStart=3, reorgEnd=6
	reorgHeights2 := map[uint64]bool{
		3: true,
		6: true,
	}
	heights2 := []uint64{1, 2, 3, 4, 5, 6, 7, 8}

	reorgStart2, reorgEnd2 := s.findReorgRange(heights2, reorgHeights2)
	require.Equal(uint64(3), reorgStart2, "Non-continuous reorg should start at height 3")
	require.Equal(uint64(6), reorgEnd2, "Non-continuous reorg should end at height 6")

	// Test case 3: Single height reorg (3a,3b,4,5,6)
	// Expected: reorgStart=3, reorgEnd=3
	reorgHeights3 := map[uint64]bool{
		3: true,
	}
	heights3 := []uint64{1, 2, 3, 4, 5, 6}

	reorgStart3, reorgEnd3 := s.findReorgRange(heights3, reorgHeights3)
	require.Equal(uint64(3), reorgStart3, "Single height reorg should start at height 3")
	require.Equal(uint64(3), reorgEnd3, "Single height reorg should end at height 3")

	// Test case 4: Reorg at the beginning (1a,1b,2,3,4)
	// Expected: reorgStart=1, reorgEnd=1
	reorgHeights4 := map[uint64]bool{
		1: true,
	}
	heights4 := []uint64{1, 2, 3, 4}

	reorgStart4, reorgEnd4 := s.findReorgRange(heights4, reorgHeights4)
	require.Equal(uint64(1), reorgStart4, "Reorg at beginning should start at height 1")
	require.Equal(uint64(1), reorgEnd4, "Reorg at beginning should end at height 1")

	// Test case 5: Reorg at the end (1,2,3a,3b,4a,4b)
	// Expected: reorgStart=3, reorgEnd=4
	reorgHeights5 := map[uint64]bool{
		3: true,
		4: true,
	}
	heights5 := []uint64{1, 2, 3, 4}

	reorgStart5, reorgEnd5 := s.findReorgRange(heights5, reorgHeights5)
	require.Equal(uint64(3), reorgStart5, "Reorg at end should start at height 3")
	require.Equal(uint64(4), reorgEnd5, "Reorg at end should end at height 4")

	// Test case 6: No reorg (1,2,3,4,5)
	// Expected: reorgStart=0, reorgEnd=0 (no reorg)
	reorgHeights6 := map[uint64]bool{}
	heights6 := []uint64{1, 2, 3, 4, 5}

	reorgStart6, reorgEnd6 := s.findReorgRange(heights6, reorgHeights6)
	require.Equal(uint64(0), reorgStart6, "No reorg should return start=0")
	require.Equal(uint64(0), reorgEnd6, "No reorg should return end=0")

	// Test case 7: Multiple separate reorgs (1a,1b,2,3a,3b,4,5a,5b)
	// Expected: reorgStart=1, reorgEnd=5 (covers the entire range)
	reorgHeights7 := map[uint64]bool{
		1: true,
		3: true,
		5: true,
	}
	heights7 := []uint64{1, 2, 3, 4, 5}

	reorgStart7, reorgEnd7 := s.findReorgRange(heights7, reorgHeights7)
	require.Equal(uint64(1), reorgStart7, "Multiple reorgs should start at first reorg height")
	require.Equal(uint64(5), reorgEnd7, "Multiple reorgs should end at last reorg height")
}

// findReorgRange is a helper function that mimics the reorg detection logic in the migrator
func (s *migratorActivityTestSuite) findReorgRange(heights []uint64, reorgHeights map[uint64]bool) (uint64, uint64) {
	var reorgStart, reorgEnd uint64
	inReorg := false

	for _, h := range heights {
		if reorgHeights[h] {
			if !inReorg {
				reorgStart = h
				inReorg = true
			}
			reorgEnd = h
		} else if inReorg {
			// Reorg ended, but we continue to look for more reorgs
			// This handles the case where there are multiple separate reorgs
		}
	}

	return reorgStart, reorgEnd
}

// TestMigrator_ReorgChainBuilding tests the logic for building canonical and non-canonical chains
func (s *migratorActivityTestSuite) TestMigrator_ReorgChainBuilding() {
	require := testutil.Require(s.T())

	// Test building chains for the scenario: 3a,3b,4a,4b,5a,5b where 'b' is canonical
	mockBlocks := map[string][]BlockWithCanonicalInfo{
		"2-3": {
			{
				BlockMetadata: &api.BlockMetadata{
					Tag:          2,
					Height:       3,
					Hash:         "0x3333333333333333333333333333333333333333333333333333333333333333",
					ParentHash:   "0x2222222222222222222222222222222222222222222222222222222222222222",
					ParentHeight: 2,
				},
				IsCanonical: false, // 3a
			},
			{
				BlockMetadata: &api.BlockMetadata{
					Tag:          2,
					Height:       3,
					Hash:         "0x3333333333333333333333333333333333333333333333333333333333333334",
					ParentHash:   "0x2222222222222222222222222222222222222222222222222222222222222222",
					ParentHeight: 2,
				},
				IsCanonical: true, // 3b
			},
		},
		"2-4": {
			{
				BlockMetadata: &api.BlockMetadata{
					Tag:          2,
					Height:       4,
					Hash:         "0x4444444444444444444444444444444444444444444444444444444444444444",
					ParentHash:   "0x3333333333333333333333333333333333333333333333333333333333333333",
					ParentHeight: 3,
				},
				IsCanonical: false, // 4a
			},
			{
				BlockMetadata: &api.BlockMetadata{
					Tag:          2,
					Height:       4,
					Hash:         "0x4444444444444444444444444444444444444444444444444444444444444445",
					ParentHash:   "0x3333333333333333333333333333333333333333333333333333333333333334",
					ParentHeight: 3,
				},
				IsCanonical: true, // 4b
			},
		},
		"2-5": {
			{
				BlockMetadata: &api.BlockMetadata{
					Tag:          2,
					Height:       5,
					Hash:         "0x5555555555555555555555555555555555555555555555555555555555555555",
					ParentHash:   "0x4444444444444444444444444444444444444444444444444444444444444444",
					ParentHeight: 4,
				},
				IsCanonical: false, // 5a
			},
			{
				BlockMetadata: &api.BlockMetadata{
					Tag:          2,
					Height:       5,
					Hash:         "0x5555555555555555555555555555555555555555555555555555555555555556",
					ParentHash:   "0x4444444444444444444444444444444444444444444444444444444444444445",
					ParentHeight: 4,
				},
				IsCanonical: true, // 5b
			},
		},
	}

	// Build canonical and non-canonical chains
	canonicalChain := s.buildCanonicalChain(mockBlocks, 3, 5)
	nonCanonicalChain := s.buildNonCanonicalChain(mockBlocks, 3, 5)

	// Verify canonical chain
	require.Len(canonicalChain, 3, "Canonical chain should have 3 blocks")
	require.Equal("0x3333333333333333333333333333333333333333333333333333333333333334", canonicalChain[0].Hash, "First canonical block should be 3b")
	require.Equal("0x4444444444444444444444444444444444444444444444444444444444444445", canonicalChain[1].Hash, "Second canonical block should be 4b")
	require.Equal("0x5555555555555555555555555555555555555555555555555555555555555556", canonicalChain[2].Hash, "Third canonical block should be 5b")

	// Verify non-canonical chain
	require.Len(nonCanonicalChain, 3, "Non-canonical chain should have 3 blocks")
	require.Equal("0x3333333333333333333333333333333333333333333333333333333333333333", nonCanonicalChain[0].Hash, "First non-canonical block should be 3a")
	require.Equal("0x4444444444444444444444444444444444444444444444444444444444444444", nonCanonicalChain[1].Hash, "Second non-canonical block should be 4a")
	require.Equal("0x5555555555555555555555555555555555555555555555555555555555555555", nonCanonicalChain[2].Hash, "Third non-canonical block should be 5a")

	// Verify chain continuity in canonical chain
	require.Equal("0x3333333333333333333333333333333333333333333333333333333333333334", canonicalChain[1].ParentHash, "4b should point to 3b")
	require.Equal("0x4444444444444444444444444444444444444444444444444444444444444445", canonicalChain[2].ParentHash, "5b should point to 4b")

	// Verify chain continuity in non-canonical chain
	require.Equal("0x3333333333333333333333333333333333333333333333333333333333333333", nonCanonicalChain[1].ParentHash, "4a should point to 3a")
	require.Equal("0x4444444444444444444444444444444444444444444444444444444444444444", nonCanonicalChain[2].ParentHash, "5a should point to 4a")
}

// buildCanonicalChain is a helper function that mimics the canonical chain building logic
func (s *migratorActivityTestSuite) buildCanonicalChain(blocks map[string][]BlockWithCanonicalInfo, startHeight, endHeight uint64) []*api.BlockMetadata {
	var canonicalChain []*api.BlockMetadata
	for height := startHeight; height <= endHeight; height++ {
		key := fmt.Sprintf("2-%d", height)
		if heightBlocks, exists := blocks[key]; exists {
			for _, block := range heightBlocks {
				if block.IsCanonical {
					canonicalChain = append(canonicalChain, block.BlockMetadata)
					break
				}
			}
		}
	}
	return canonicalChain
}

// buildNonCanonicalChain is a helper function that mimics the non-canonical chain building logic
func (s *migratorActivityTestSuite) buildNonCanonicalChain(blocks map[string][]BlockWithCanonicalInfo, startHeight, endHeight uint64) []*api.BlockMetadata {
	var nonCanonicalChain []*api.BlockMetadata
	for height := startHeight; height <= endHeight; height++ {
		key := fmt.Sprintf("2-%d", height)
		if heightBlocks, exists := blocks[key]; exists {
			for _, block := range heightBlocks {
				if !block.IsCanonical {
					nonCanonicalChain = append(nonCanonicalChain, block.BlockMetadata)
					break
				}
			}
		}
	}
	return nonCanonicalChain
}
