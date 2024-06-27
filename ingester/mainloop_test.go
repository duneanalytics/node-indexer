package ingester_test

import (
	"context"
	"io"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duneanalytics/blockchain-ingester/ingester"
	duneapi_mock "github.com/duneanalytics/blockchain-ingester/mocks/duneapi"
	jsonrpc_mock "github.com/duneanalytics/blockchain-ingester/mocks/jsonrpc"
	"github.com/duneanalytics/blockchain-ingester/models"
	"github.com/go-errors/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestRunUntilCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	maxBlockNumber := int64(10)
	sentBlockNumber := int64(0)
	producedBlockNumber := int64(0)
	duneapi := &duneapi_mock.BlockchainIngesterMock{
		SendBlocksFunc: func(_ context.Context, blocks []models.RPCBlock) error {
			if len(blocks) == 0 {
				return nil
			}

			next := sentBlockNumber + 1
			for _, block := range blocks {
				// We cannot send blocks out of order to DuneAPI
				require.Equalf(t, next, block.BlockNumber, "expected block %d, got %d", next, block.BlockNumber)
				next++
			}

			lastBlockNumber := blocks[len(blocks)-1].BlockNumber
			atomic.StoreInt64(&sentBlockNumber, lastBlockNumber)
			if lastBlockNumber >= maxBlockNumber {
				// cancel execution when we have sent the last block
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
	ing := ingester.New(
		slog.New(slog.NewTextHandler(logOutput, nil)),
		rpcClient,
		duneapi,
		ingester.Config{
			BlockSubmitInterval:   time.Nanosecond,
			MaxConcurrentRequests: 10,
			SkipFailedBlocks:      false,
		},
		nil, // progress
	)

	err := ing.Run(ctx, 1, -1)                // run until canceled
	require.ErrorIs(t, err, context.Canceled) // this is expected
	require.GreaterOrEqual(t, sentBlockNumber, maxBlockNumber)
}

func TestProduceBlockNumbers(t *testing.T) {
	duneapi := &duneapi_mock.BlockchainIngesterMock{
		PostProgressReportFunc: func(_ context.Context, _ models.BlockchainIndexProgress) error {
			return nil
		},
	}
	rpcClient := &jsonrpc_mock.BlockchainClientMock{
		LatestBlockNumberFunc: func() (int64, error) {
			return 100_000, nil
		},
		BlockByNumberFunc: func(_ context.Context, blockNumber int64) (models.RPCBlock, error) {
			return models.RPCBlock{BlockNumber: blockNumber}, nil
		},
		CloseFunc: func() error {
			return nil
		},
	}
	// Swap these to see logs
	logOutput := io.Discard
	// logOutput := os.Stderr
	ing := ingester.New(
		slog.New(slog.NewTextHandler(logOutput, nil)),
		rpcClient,
		duneapi,
		ingester.Config{
			BlockSubmitInterval: time.Nanosecond,
		},
		nil, // progress
	)
	blockNumbers := make(chan int64)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ing.ProduceBlockNumbers(context.Background(), blockNumbers, 1, 100_000)
	}()
	for i := 1; i <= 100_000; i++ {
		require.Equal(t, int64(i), <-blockNumbers)
	}
	wg.Wait()
}

func TestSendBlocks(t *testing.T) {
	sentBlockNumber := int64(0)
	duneapi := &duneapi_mock.BlockchainIngesterMock{
		SendBlocksFunc: func(_ context.Context, blocks []models.RPCBlock) error {
			if len(blocks) == 0 {
				return nil
			}

			next := sentBlockNumber + 1
			for _, block := range blocks {
				// We cannot send blocks out of order to DuneAPI
				require.Equalf(t, next, block.BlockNumber, "expected block %d, got %d", next, block.BlockNumber)
				next++
			}

			lastBlockNumber := blocks[len(blocks)-1].BlockNumber
			atomic.StoreInt64(&sentBlockNumber, lastBlockNumber)
			return nil
		},
	}
	// Swap these to see logs
	// logOutput := os.Stderr
	logOutput := io.Discard
	ing := ingester.New(
		slog.New(slog.NewTextHandler(logOutput, nil)),
		nil, // node client isn't used in this unit test
		duneapi,
		ingester.Config{
			BlockSubmitInterval: time.Nanosecond,
		},
		nil, // progress
	)

	blocks := make(chan models.RPCBlock)

	startFromBlock := 1

	group, ctx := errgroup.WithContext(context.Background())
	group.Go(func() error {
		return ing.SendBlocks(context.Background(), blocks, int64(startFromBlock))
	})

	// Send blocks except the next block, ensure none are sent to the API
	// NOTE: this size and maxBatchSize are related, because we optimize for not sending tiny batches
	for _, n := range []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 19} {
		select {
		case <-ctx.Done(): // if error group fails, its context is canceled
			require.Fail(t, "context was canceled")
		case blocks <- models.RPCBlock{BlockNumber: n, Payload: []byte("block")}:
			// Sent block
		}
		require.Equal(t, int64(0), sentBlockNumber)
	}
	// Now send the first block
	blocks <- models.RPCBlock{BlockNumber: 1, Payload: []byte("block")}
	time.Sleep(time.Millisecond) // Allow enough time for the tick before closing the channel
	close(blocks)
	require.NoError(t, group.Wait())

	// Ensure the last correct block was sent
	require.Equal(t, int64(10), sentBlockNumber)
}

func TestRunBlocksUseBatching(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	maxBlockNumber := int64(1000)
	sentBlockNumber := int64(0)
	producedBlockNumber := int64(0)
	duneapi := &duneapi_mock.BlockchainIngesterMock{
		SendBlocksFunc: func(_ context.Context, blocks []models.RPCBlock) error {
			if len(blocks) == 0 {
				return nil
			}

			// Fail if we're not sending a batch of blocks
			require.Greater(t, len(blocks), 1)

			next := sentBlockNumber + 1
			for _, block := range blocks {
				// We cannot send blocks out of order to DuneAPI
				require.Equalf(t, next, block.BlockNumber, "expected block %d, got %d", next, block.BlockNumber)
				next++
			}

			lastBlockNumber := blocks[len(blocks)-1].BlockNumber
			atomic.StoreInt64(&sentBlockNumber, lastBlockNumber)
			if lastBlockNumber >= maxBlockNumber {
				// cancel execution when we have sent the last block
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
			// Get blocks out of order by sleeping for a random amount of time
			atomic.StoreInt64(&producedBlockNumber, blockNumber)
			return models.RPCBlock{BlockNumber: blockNumber, Payload: []byte("block")}, nil
		},
		CloseFunc: func() error {
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
			MaxConcurrentRequests: 20, // fetch blocks in multiple goroutines
			// big enough compared to the time spent in block by number to ensure batching. We panic
			// in the mocked Dune client if we don't get a batch of blocks (more than one block).
			BlockSubmitInterval: 50 * time.Millisecond,
			SkipFailedBlocks:    false,
		},
		nil, // progress
	)

	err := ing.Run(ctx, 1, -1)                // run until canceled
	require.ErrorIs(t, err, context.Canceled) // this is expected
	require.GreaterOrEqual(t, sentBlockNumber, maxBlockNumber)
}

// TestRunBlocksOutOfOrder asserts that we can fetch blocks concurrently and that we ingest them in order
// even if they are produced out of order. We ensure they are produced out of order by sleeping a random amount of time.
func TestRunBlocksOutOfOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	maxBlockNumber := int64(1000)
	sentBlockNumber := int64(0)
	producedBlockNumber := int64(0)
	duneapi := &duneapi_mock.BlockchainIngesterMock{
		SendBlocksFunc: func(_ context.Context, blocks []models.RPCBlock) error {
			if len(blocks) == 0 {
				return nil
			}

			next := sentBlockNumber + 1
			for _, block := range blocks {
				// We cannot send blocks out of order to DuneAPI
				require.Equalf(t, next, block.BlockNumber, "expected block %d, got %d", next, block.BlockNumber)
				next++
			}

			lastBlockNumber := blocks[len(blocks)-1].BlockNumber
			atomic.StoreInt64(&sentBlockNumber, lastBlockNumber)
			if lastBlockNumber >= maxBlockNumber {
				// cancel execution when we have sent the last block
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
			// Get blocks out of order by sleeping for a random amount of time
			time.Sleep(time.Duration(rand.Intn(10)) * time.Nanosecond)
			atomic.StoreInt64(&producedBlockNumber, blockNumber)
			return models.RPCBlock{BlockNumber: blockNumber, Payload: []byte("block")}, nil
		},
		CloseFunc: func() error {
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
			MaxConcurrentRequests: 20, // fetch blocks in multiple goroutines
			// big enough compared to the time spent in block by number to ensure batching. We panic
			// in the mocked Dune client if we don't get a batch of blocks (more than one block).
			BlockSubmitInterval: 50 * time.Millisecond,
			SkipFailedBlocks:    false,
		},
		nil, // progress
	)

	err := ing.Run(ctx, 1, -1)                // run until canceled
	require.ErrorIs(t, err, context.Canceled) // this is expected
	require.GreaterOrEqual(t, sentBlockNumber, maxBlockNumber)
}

// TestRunRPCNodeFails shows that we crash if the RPC client fails to fetch a block
func TestRunRPCNodeFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	maxBlockNumber := int64(1000)
	someRPCError := errors.Errorf("some RPC error")
	duneapi := &duneapi_mock.BlockchainIngesterMock{
		SendBlocksFunc: func(_ context.Context, _ []models.RPCBlock) error {
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
		BlockByNumberFunc: func(_ context.Context, _ int64) (models.RPCBlock, error) {
			// Get blocks out of order by sleeping for a random amount of time
			time.Sleep(time.Duration(rand.Intn(10)) * time.Nanosecond)
			return models.RPCBlock{}, someRPCError
		},
		CloseFunc: func() error {
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
			MaxConcurrentRequests: 10,
			BlockSubmitInterval:   time.Millisecond,
			SkipFailedBlocks:      false,
		},
		nil, // progress
	)

	err := ing.Run(ctx, 1, -1) // run until canceled
	require.ErrorIs(t, err, someRPCError)
}

// TestRunRPCNodeFails shows that we crash if the RPC client fails to fetch a block
func TestRunFailsIfNoConcurrentRequests(t *testing.T) {
	logOutput := io.Discard
	ing := ingester.New(
		slog.New(slog.NewTextHandler(logOutput, nil)),
		nil,
		nil,
		ingester.Config{
			MaxConcurrentRequests: 0,
		},
		nil, // progress
	)

	err := ing.Run(context.Background(), 1, -1) // run until canceled
	require.ErrorContains(t, err, "MaxConcurrentRequests must be > 0")
}
