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

	"github.com/duneanalytics/blockchain-ingester/lib/dlq"

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
		duneapi,
		ingester.Config{
			BlockSubmitInterval:      time.Nanosecond,
			MaxConcurrentRequests:    10,
			MaxConcurrentRequestsDLQ: 2,
			SkipFailedBlocks:         false,
		},
		nil, // progress
		dlq.NewDLQ[int64](),
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
		duneapi,
		ingester.Config{
			BlockSubmitInterval: time.Nanosecond,
		},
		nil, // progress
		dlq.NewDLQ[int64](),
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
		duneapi,
		ingester.Config{
			BlockSubmitInterval: time.Nanosecond,
		},
		nil, // progress
		dlq.NewDLQ[int64](),
	)

	blocks := make(chan models.RPCBlock)

	startFromBlock := 1

	group, ctx := errgroup.WithContext(context.Background())
	group.Go(func() error {
		return ing.SendBlocks(context.Background(), blocks, int64(startFromBlock))
	})

	// Send blocks except the next block, ensure none are sent to the API
	for _, n := range []int64{2, 3, 4, 5, 6, 7, 8, 9, 10, 19} {
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
		duneapi,
		ingester.Config{
			MaxConcurrentRequests:    20,
			MaxConcurrentRequestsDLQ: 2, // fetch blocks in multiple goroutines
			// big enough compared to the time spent in block by number to ensure batching. We panic
			// in the mocked Dune client if we don't get a batch of blocks (more than one block).
			BlockSubmitInterval: 50 * time.Millisecond,
			SkipFailedBlocks:    false,
		},
		nil, // progress
		dlq.NewDLQ[int64](),
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
		duneapi,
		ingester.Config{
			MaxConcurrentRequests:    10,
			MaxConcurrentRequestsDLQ: 2,
			BlockSubmitInterval:      time.Millisecond,
			SkipFailedBlocks:         false,
		},
		nil, // progress
		dlq.NewDLQ[int64](),
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
		nil,
		ingester.Config{
			MaxConcurrentRequests: 0,
		},
		nil, // progress
		dlq.NewDLQ[int64](),
	)

	err := ing.Run(context.Background(), 1, -1) // run until canceled
	require.ErrorContains(t, err, "MaxConcurrentRequests must be > 0")
}

func TestRunFailsIfNoConcurrentRequestsDLQ(t *testing.T) {
	logOutput := io.Discard
	ing := ingester.New(
		slog.New(slog.NewTextHandler(logOutput, nil)),
		nil,
		nil,
		nil,
		ingester.Config{
			MaxConcurrentRequests:    10,
			MaxConcurrentRequestsDLQ: 0,
		},
		nil, // progress
		dlq.NewDLQ[int64](),
	)

	err := ing.Run(context.Background(), 1, -1) // run until canceled
	require.ErrorContains(t, err, "MaxConcurrentRequestsDLQ must be > 0")
}

func TestRunWithDLQ(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	maxBlockNumber := int64(1000)
	startBlockNumber := int64(10)

	// Initial DLQ
	dlqBlockNumbers := dlq.NewDLQWithDelay[int64](dlq.RetryDelayLinear(time.Duration(10) * time.Millisecond))
	gaps := []models.BlockGap{
		{
			FirstMissing: 9,
			LastMissing:  9,
		}, {
			FirstMissing: 3,
			LastMissing:  7,
		}, {
			FirstMissing: 0,
			LastMissing:  0,
		},
	}
	ingester.AddBlockGaps(dlqBlockNumbers, gaps)

	// blockNumber int64 -> timesSubmitted int
	var blocksIndexed sync.Map
	// Prepopulate expected blocks
	for i := int64(0); i < maxBlockNumber; i++ {
		blocksIndexed.Store(i, 0)
	}
	// Add those that aren't considered as previous gaps
	incrementAndGet(&blocksIndexed, int64(1))
	incrementAndGet(&blocksIndexed, int64(2))
	incrementAndGet(&blocksIndexed, int64(8))

	// Dune API Mocking
	var sendBlocksRequests sync.Map
	duneapi := &duneapi_mock.BlockchainIngesterMock{
		SendBlocksFunc: func(_ context.Context, blocks []models.RPCBlock) error {
			if len(blocks) == 0 {
				return nil
			}
			// Count Requests by block number
			for _, block := range blocks {
				incrementAndGet(&sendBlocksRequests, block.BlockNumber)
			}

			// Fail if this batch contains a block number that hasn't been requested at least twice before this call
			for _, block := range blocks {
				requests, _ := sendBlocksRequests.Load(block.BlockNumber)
				if requests.(int) <= 2 {
					return errors.Errorf("failing batch due to %v having only been requested %v times",
						block.BlockNumber, requests)
				}
			}

			// Count blocks as indexed by block number
			for _, block := range blocks {
				incrementAndGet(&blocksIndexed, block.BlockNumber)
			}

			// Look for gaps
			if !duneStoreContainsGaps(&blocksIndexed, maxBlockNumber) {
				// cancel execution when we have sent all blocks
				cancel()
				return context.Canceled
			}

			return nil
		},
		PostProgressReportFunc: func(_ context.Context, _ models.BlockchainIndexProgress) error {
			return nil
		},
	}

	// RPC Mocking
	var rpcBlocksRequests sync.Map
	rpcClient := &jsonrpc_mock.BlockchainClientMock{
		LatestBlockNumberFunc: func() (int64, error) {
			return maxBlockNumber + 1, nil
		},
		BlockByNumberFunc: func(_ context.Context, blockNumber int64) (models.RPCBlock, error) {
			incrementAndGet(&rpcBlocksRequests, blockNumber)

			// Fail every 10th block numbers the first 2 times
			if blockNumber%10 == 0 {
				requests, _ := rpcBlocksRequests.Load(blockNumber)
				if requests.(int) <= 2 {
					return models.RPCBlock{},
						errors.Errorf("failing rpc request due to %v having only been requested %v times",
							blockNumber, requests)
				}
			}

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
		duneapi,
		ingester.Config{
			BlockSubmitInterval:      time.Nanosecond,
			MaxConcurrentRequests:    10,
			MaxConcurrentRequestsDLQ: 1,
			DLQOnly:                  false,
			SkipFailedBlocks:         true,
		},
		nil, // progress
		dlqBlockNumbers,
	)

	err := ing.Run(ctx, startBlockNumber, -1) // run until canceled
	require.False(t, duneStoreContainsGaps(&blocksIndexed, maxBlockNumber))
	require.GreaterOrEqual(t, lenSyncMap(&blocksIndexed), int(maxBlockNumber))
	require.ErrorIs(t, err, context.Canceled) // this is expected
}

func duneStoreContainsGaps(blocksIndexed *sync.Map, maxBlockNumber int64) bool {
	containsGap := false
	blocksIndexed.Range(func(key, value any) bool {
		blockNumber := key.(int64)
		count := value.(int)
		if blockNumber <= maxBlockNumber && count < 1 {
			containsGap = true
			return false
		}
		return true
	})
	return containsGap
}

func incrementAndGet(m *sync.Map, key interface{}) int {
	for {
		// Load the current value associated with the key else initialise
		currentValue, _ := m.LoadOrStore(key, 0)

		// Increment the current value.
		newValue := currentValue.(int) + 1

		// Attempt to store the new value back into the sync.Map. Compare-and-swap (CAS) approach ensures atomicity.
		if m.CompareAndSwap(key, currentValue, newValue) {
			// If the swap succeeded, return the new value.
			return newValue
		}
		// If the swap failed, it means the value was updated by another goroutine. Retry the operation.
	}
}

func lenSyncMap(m *sync.Map) int {
	length := 0
	m.Range(func(_, _ interface{}) bool {
		length++
		return true
	})
	return length
}
