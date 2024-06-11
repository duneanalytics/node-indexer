package ingester_test

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duneanalytics/blockchain-ingester/ingester"
	duneapi_mock "github.com/duneanalytics/blockchain-ingester/mocks/duneapi"
	jsonrpc_mock "github.com/duneanalytics/blockchain-ingester/mocks/jsonrpc"
	"github.com/duneanalytics/blockchain-ingester/models"
	"github.com/go-errors/errors"
	"github.com/stretchr/testify/require"
)

func TestBlockConsumptionLoopErrors(t *testing.T) {
	testcases := []struct {
		name                  string
		LatestIsBroken        bool
		BlockByNumberIsBroken bool
	}{
		{
			name:                  "we're up to date, following the head",
			LatestIsBroken:        false,
			BlockByNumberIsBroken: false,
		},
		{
			name:                  "the RPC node is broken, all API calls are failing",
			LatestIsBroken:        true,
			BlockByNumberIsBroken: true,
		},
		{
			name:                  "BlockByNumber, a specific jsonRPC on the RPC node is broken",
			LatestIsBroken:        false,
			BlockByNumberIsBroken: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.LatestIsBroken {
				t.Skip("latest block number is broken, we don't behave correctly yet")
			}
			ctx, cancel := context.WithCancel(context.Background())
			maxBlockNumber := int64(100)
			producedBlockNumber := int64(0)
			rpcClient := &jsonrpc_mock.BlockchainClientMock{
				LatestBlockNumberFunc: func() (int64, error) {
					if tc.LatestIsBroken {
						return 0, errors.New("latest block number is broken")
					}
					return maxBlockNumber, nil
				},
				BlockByNumberFunc: func(_ context.Context, blockNumber int64) (models.RPCBlock, error) {
					if tc.BlockByNumberIsBroken {
						return models.RPCBlock{}, errors.New("block by number is broken")
					}
					if blockNumber > maxBlockNumber {
						// end tests
						cancel()
					}
					atomic.StoreInt64(&producedBlockNumber, blockNumber)
					return models.RPCBlock{
						BlockNumber: blockNumber,
						Payload:     []byte(`block`),
					}, nil
				},
				CloseFunc: func() error {
					return nil
				},
			}
			// Swap these to see logs
			// logOutput := os.Stderr
			logOutput := io.Discard
			ing := ingester.New(slog.New(slog.NewTextHandler(logOutput, nil)), rpcClient, nil, ingester.Config{
				MaxBatchSize: 1,
				PollInterval: 1000 * time.Millisecond,
			})

			outCh := make(chan models.RPCBlock, maxBlockNumber+1)
			err := ing.ConsumeBlocks(ctx, outCh, 0, maxBlockNumber)
			require.ErrorIs(t, err, ingester.ErrFinishedConsumeBlocks)
			if tc.BlockByNumberIsBroken {
				require.Equal(t, producedBlockNumber, int64(0))
			}
		})
	}
}

func TestBlockSendingLoop(t *testing.T) {
	testcases := []string{
		"we're up to date, following the head",
		"we're failing intermittently, the Dune API is broken",
		"we're erroring systematically, the Dune API is down",
	}
	for _, testcase := range testcases {
		t.Run(testcase, func(t *testing.T) {
			t.Skip("not implemented")
		})
	}
}

func TestRunLoopBaseCase(t *testing.T) {
	testCases := []struct {
		name             string
		maxCount         int64
		lastIngested     int64
		expectedEndBlock int64
	}{
		{name: "1 block", maxCount: 1, lastIngested: 0, expectedEndBlock: 1},
		{name: "2 blocks", maxCount: 2, lastIngested: 0, expectedEndBlock: 2},
		{name: "100 blocks", maxCount: 100, lastIngested: 0, expectedEndBlock: 100},
		{name: "100 blocks, starting from 50", maxCount: 100, lastIngested: 50, expectedEndBlock: 150},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			latestBlockNumber := int64(1000)
			ingestedBlockNumber := int64(0)

			rpcClient := &jsonrpc_mock.BlockchainClientMock{
				LatestBlockNumberFunc: func() (int64, error) {
					return latestBlockNumber, nil
				},
				BlockByNumberFunc: func(_ context.Context, blockNumber int64) (models.RPCBlock, error) {
					atomic.StoreInt64(&latestBlockNumber, blockNumber)
					return models.RPCBlock{
						BlockNumber: blockNumber,
						Payload:     []byte(`block`),
					}, nil
				},
				CloseFunc: func() error {
					return nil
				},
			}

			duneapi := &duneapi_mock.BlockchainIngesterMock{
				SendBlockFunc: func(_ context.Context, block models.RPCBlock) error {
					atomic.StoreInt64(&ingestedBlockNumber, block.BlockNumber)
					return nil
				},
				PostProgressReportFunc: func(_ context.Context, _ models.BlockchainIndexProgress) error {
					return nil
				},
			}

			// Swap these to see logs
			// logOutput := os.Stderr
			logOutput := io.Discard
			ing := ingester.New(
				slog.New(slog.NewTextHandler(logOutput, nil)),
				rpcClient,
				duneapi,
				ingester.Config{
					MaxBatchSize: 1,
					PollInterval: 1000 * time.Millisecond,
				},
			)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			startBlockNumber := tc.lastIngested + 1
			err := ing.Run(ctx, startBlockNumber, tc.maxCount)
			require.ErrorIs(t, err, ingester.ErrFinishedConsumeBlocks)
			// require.Equal(t, tc.lastIngested+tc.maxCount, producedBlockNumber)
			require.Equal(t, tc.expectedEndBlock, atomic.LoadInt64(&ingestedBlockNumber))
		})
	}
}

func TestRunLoopUntilCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	maxBlockNumber := int64(10)
	sentBlockNumber := int64(0)
	producedBlockNumber := int64(0)
	duneapi := &duneapi_mock.BlockchainIngesterMock{
		SendBlockFunc: func(_ context.Context, block models.RPCBlock) error {
			atomic.StoreInt64(&sentBlockNumber, block.BlockNumber)
			if block.BlockNumber == maxBlockNumber {
				// cancel execution when we send the last block
				cancel()
				return context.Canceled
			}
			return nil
		},
		PostProgressReportFunc: func(_ context.Context, _ models.BlockchainIndexProgress) error {
			return nil
		},
	}
	rpcClient := &jsonrpc_mock.BlockchainClientMock{
		LatestBlockNumberFunc: func() (int64, error) {
			return maxBlockNumber + 1, nil
		},
		BlockByNumberFunc: func(_ context.Context, blockNumber int64) (models.RPCBlock, error) {
			atomic.StoreInt64(&producedBlockNumber, blockNumber)
			return models.RPCBlock{
				BlockNumber: blockNumber,
				Payload:     []byte(`block`),
			}, nil
		},
		CloseFunc: func() error {
			return nil
		},
	}
	// Swap these to see logs
	// logOutput := os.Stderr
	logOutput := io.Discard
	ing := ingester.New(slog.New(slog.NewTextHandler(logOutput, nil)), rpcClient, duneapi, ingester.Config{
		MaxBatchSize: 1,
		PollInterval: 1000 * time.Millisecond,
	})

	err := ing.Run(ctx, 1, -1)                // run until canceled
	require.ErrorIs(t, err, context.Canceled) // this is expected
	require.Equal(t, sentBlockNumber, maxBlockNumber)
}
