package ingester_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
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
			}
			ing := ingester.New(slog.New(slog.NewTextHandler(os.Stderr, nil)), rpcClient, nil, ingester.Config{
				MaxBatchSize: 1,
				PollInterval: 1000 * time.Millisecond,
			})

			inCh := make(chan int64, maxBlockNumber+1)
			go ing.ProduceBlockNumbers(ctx, inCh, 1, maxBlockNumber)

			fmt.Println("hello")

			outCh := make(chan models.RPCBlock, maxBlockNumber+1)
			defer close(outCh)
			err := ing.ConsumeBlocks(ctx, inCh, outCh)
			require.Nil(t, err) // this is expected
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
		name string
		i    int64
	}{
		{name: "1 block", i: 1},
		{name: "100 blocks", i: 100},
	}
	sentBlockNumber := int64(0)
	producedBlockNumber := int64(0)
	duneapi := &duneapi_mock.BlockchainIngesterMock{
		SendBlockFunc: func(_ context.Context, block models.RPCBlock) error {
			atomic.StoreInt64(&sentBlockNumber, block.BlockNumber)
			return nil
		},
	}
	rpcClient := &jsonrpc_mock.BlockchainClientMock{
		LatestBlockNumberFunc: func() (int64, error) {
			return 1000, nil
		},
		BlockByNumberFunc: func(_ context.Context, blockNumber int64) (models.RPCBlock, error) {
			atomic.StoreInt64(&producedBlockNumber, blockNumber)
			return models.RPCBlock{
				BlockNumber: blockNumber,
				Payload:     []byte(`block`),
			}, nil
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ing := ingester.New(slog.New(slog.NewTextHandler(os.Stderr, nil)), rpcClient, duneapi, ingester.Config{
				MaxBatchSize: 1,
				PollInterval: 1000 * time.Millisecond,
			})

			// TODO: Cancel when done, this deadlocks in ReportProgress

			err := ing.Run(context.Background(), 0, tc.i)
			require.NoError(t, err)
			require.Equal(t, producedBlockNumber, tc.i)
			require.Equal(t, sentBlockNumber, tc.i)
		})
	}
}

func TestRunLoopUntilCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	maxBlockNumber := int64(1000)
	sentBlockNumber := int64(0)
	producedBlockNumber := int64(0)
	duneapi := &duneapi_mock.BlockchainIngesterMock{
		SendBlockFunc: func(_ context.Context, block models.RPCBlock) error {
			atomic.StoreInt64(&sentBlockNumber, block.BlockNumber)
			fmt.Println(block.BlockNumber, maxBlockNumber)
			if block.BlockNumber == maxBlockNumber {
				// cancel execution when we send the last block
				cancel()
			}
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
	}
	ing := ingester.New(slog.New(slog.NewTextHandler(os.Stderr, nil)), rpcClient, duneapi, ingester.Config{
		MaxBatchSize: 1,
		PollInterval: 1000 * time.Millisecond,
	})
	err := ing.Run(ctx, 0, maxBlockNumber)
	require.NoError(t, err)
	require.Equal(t, producedBlockNumber, maxBlockNumber)
	require.Equal(t, sentBlockNumber, maxBlockNumber)
}
