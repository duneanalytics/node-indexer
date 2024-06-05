package ingester_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duneanalytics/blockchain-ingester/ingester"
	duneapi_mock "github.com/duneanalytics/blockchain-ingester/mocks/duneapi"
	jsonrpc_mock "github.com/duneanalytics/blockchain-ingester/mocks/jsonrpc"
	"github.com/duneanalytics/blockchain-ingester/models"
	"github.com/stretchr/testify/require"
)

func TestBlockConsumptionLoop(t *testing.T) {
	testcases := []string{
		"we're up to date, following the head",
		"we're erroring systematically, the RPC node is broken, all API calls are failing",
		"we're erroring only on GetBlockByNumber, a specific jsonRPC on the RPC node is broken",
	}

	for _, testcase := range testcases {
		t.Run(testcase, func(t *testing.T) {
			t.Skip("not implemented")
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
		SendBlockFunc: func(block models.RPCBlock) error {
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
			ing := ingester.New(slog.New(slog.NewTextHandler(io.Discard, nil)), rpcClient, duneapi, ingester.Config{
				MaxBatchSize: 1,
				PollInterval: 1000 * time.Millisecond,
			})

			var wg sync.WaitGroup
			wg.Add(1)
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
		SendBlockFunc: func(block models.RPCBlock) error {
			atomic.StoreInt64(&sentBlockNumber, block.BlockNumber)
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
	ing := ingester.New(slog.New(slog.NewTextHandler(io.Discard, nil)), rpcClient, duneapi, ingester.Config{
		MaxBatchSize: 1,
		PollInterval: 1000 * time.Millisecond,
	})

	err := ing.Run(ctx, 0, maxBlockNumber)
	require.NoError(t, err)
	require.Equal(t, producedBlockNumber, maxBlockNumber)
	require.Equal(t, sentBlockNumber, maxBlockNumber)
}
