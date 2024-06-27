package ingester

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/duneanalytics/blockchain-ingester/models"
	"github.com/go-errors/errors"
	"golang.org/x/sync/errgroup"
)

const maxBatchSize = 100

// Run fetches blocks from a node RPC and sends them in order to the Dune API.
//
// ProduceBlockNumbers (blockNumbers channel) -> FetchBlockLoop (blocks channel) -> SendBlocks -> Dune
//
// We produce block numbers to fetch on an unbuffered channel (ProduceBlockNumbers),
// and each concurrent FetchBlockLoop goroutine gets a block number from that channel.
// The SendBlocks goroutine receives all blocks on an unbuffered channel,
// but buffers them in a map until they can be sent in order.
func (i *ingester) Run(ctx context.Context, startBlockNumber int64, maxCount int64) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errGroup, ctx := errgroup.WithContext(ctx)

	blockNumbers := make(chan int64)
	defer close(blockNumbers)

	// We buffer the block channel so that RPC requests can be made concurrently with sending blocks to Dune.
	// We limit the buffer size to the same number of concurrent requests, so we exert some backpressure.
	blocks := make(chan models.RPCBlock, maxBatchSize)
	defer close(blocks)

	// Start MaxBatchSize goroutines to consume blocks concurrently
	if i.cfg.MaxConcurrentRequests <= 0 {
		return errors.Errorf("MaxConcurrentRequests must be > 0")
	}
	for range i.cfg.MaxConcurrentRequests {
		errGroup.Go(func() error {
			return i.FetchBlockLoop(ctx, blockNumbers, blocks)
		})
	}
	errGroup.Go(func() error {
		return i.ReportProgress(ctx)
	})
	errGroup.Go(func() error {
		return i.SendBlocks(ctx, blocks, startBlockNumber)
	})

	// Ingest until endBlockNumber, inclusive. If maxCount is <= 0, we ingest forever
	endBlockNumber := startBlockNumber - 1 + maxCount
	i.log.Info("Starting ingester",
		"runForever", maxCount <= 0,
		"startBlockNumber", startBlockNumber,
		"endBlockNumber", endBlockNumber,
		"maxConcurrency", i.cfg.MaxConcurrentRequests,
	)

	// Produce block numbers in the main goroutine
	err := i.ProduceBlockNumbers(ctx, blockNumbers, startBlockNumber, endBlockNumber)
	i.log.Info("ProduceBlockNumbers is done", "error", err)
	i.log.Info("Cancelling context")
	cancel()

	return errGroup.Wait()
}

var ErrFinishedFetchBlockLoop = errors.New("finished FetchBlockLoop")

// ProduceBlockNumbers to be consumed by multiple goroutines running FetchBlockLoop
func (i *ingester) ProduceBlockNumbers(
	ctx context.Context, blockNumbers chan int64, startBlockNumber int64, endBlockNumber int64,
) error {
	latestBlockNumber := i.tryUpdateLatestBlockNumber()

	// Helper function
	waitForBlock := func(ctx context.Context, blockNumber int64, latestBlockNumber int64) int64 {
		for blockNumber > latestBlockNumber {
			select {
			case <-ctx.Done():
				return latestBlockNumber
			case <-time.After(i.cfg.PollInterval):
			}
			i.log.Debug(fmt.Sprintf("Waiting %v for block to be available..", i.cfg.PollInterval),
				"blockNumber", blockNumber,
				"latestBlockNumber", latestBlockNumber,
			)
			latestBlockNumber = i.tryUpdateLatestBlockNumber()
		}
		return latestBlockNumber
	}

	// Consume blocks forever if end is before start. This happens if Run is called with a maxCount of <= 0
	dontStop := endBlockNumber < startBlockNumber
	i.log.Debug("Produce block numbers from", "startBlockNumber", startBlockNumber, "endBlockNumber", endBlockNumber)
	for blockNumber := startBlockNumber; dontStop || blockNumber <= endBlockNumber; blockNumber++ {
		latestBlockNumber = waitForBlock(ctx, blockNumber, latestBlockNumber)

		select {
		case <-ctx.Done():
			i.log.Debug("ProduceBlockNumbers: Context canceled, stopping")
			return ctx.Err()
		case blockNumbers <- blockNumber:
		}
	}
	i.log.Debug("Finished producing block numbers")
	return ErrFinishedFetchBlockLoop
}

// FetchBlockLoop from the RPC node. This can be run in multiple goroutines to parallelize block fetching.
func (i *ingester) FetchBlockLoop(
	ctx context.Context, blockNumbers chan int64, blocks chan models.RPCBlock,
) error {
	for {
		select {
		case <-ctx.Done():
			i.log.Info("FetchBlockLoop: context is done")
			return ctx.Err()
		case blockNumber := <-blockNumbers:
			startTime := time.Now()

			block, err := i.node.BlockByNumber(ctx, blockNumber)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					i.log.Error("FetchBlockLoop: Context canceled, stopping")
					return ctx.Err()
				}

				i.log.Error("Failed to get block by number",
					"blockNumber", blockNumber,
					"continueing", i.cfg.SkipFailedBlocks,
					"elapsed", time.Since(startTime),
					"error", err,
				)
				if !i.cfg.SkipFailedBlocks {
					return err
				}
				blocks <- models.RPCBlock{BlockNumber: blockNumber, Error: err}
				continue
			}

			atomic.StoreInt64(&i.info.ConsumedBlockNumber, block.BlockNumber)
			getBlockElapsed := time.Since(startTime)
			select {
			case <-ctx.Done():
				i.log.Debug("FetchBlockLoop: Channel is closed, not sending block to channel", "blockNumber", block.BlockNumber)
				return ctx.Err()
			case blocks <- block:
				i.log.Debug(
					"FetchBlockLoop: Got and sent block",
					"blockNumber", blockNumber,
					"getBlockElapsed", getBlockElapsed,
				)
			}
		}
	}
}

// SendBlocks to Dune. We receive blocks from the FetchBlockLoop goroutines, potentially out of order.
// We buffer the blocks in a map until we have no gaps, so that we can send them in order to Dune.
func (i *ingester) SendBlocks(ctx context.Context, blocks <-chan models.RPCBlock, startBlockNumber int64) error {
	// Buffer for temporarily storing blocks that have arrived out of order
	collectedBlocks := make(map[int64]models.RPCBlock)
	nextNumberToSend := startBlockNumber
	batchTimer := time.NewTicker(i.cfg.BlockSubmitInterval)
	defer batchTimer.Stop()

	i.log.Debug("SendBlocks: Starting to receive blocks")
	for {
		// Either receive a block, send blocks, or shut down (if the context is done, or the channel is closed).
		select {
		case <-ctx.Done():
			i.log.Debug("SendBlocks: Context canceled, stopping")
			return ctx.Err()
		case block, ok := <-blocks:
			if !ok {
				i.log.Debug("SendBlocks: Channel is closed, returning")
				return nil
			}

			if block.Errored() {
				i.info.RPCErrors = append(i.info.RPCErrors, ErrorInfo{
					Timestamp:    time.Now(),
					BlockNumbers: fmt.Sprintf("%d", block.BlockNumber),
					Error:        block.Error,
				})

				i.log.Error("Received FAILED block", "number", block.BlockNumber)
			}

			collectedBlocks[block.BlockNumber] = block
			i.log.Debug(
				"SendBlocks: Received block",
				"blockNumber", block.BlockNumber,
				"bufferSize", len(collectedBlocks),
			)
		case <-batchTimer.C:
			var err error
			nextNumberToSend, err = i.trySendCompletedBlocks(ctx, collectedBlocks, nextNumberToSend)
			if err != nil {
				return errors.Errorf("send blocks: %w", err)
			}
		}
	}
}

// trySendCompletedBlocks sends all blocks that can be sent in order from the blockMap.
// Once we have sent all blocks, if any, we return with the nextNumberToSend.
// We return the next numberToSend such that the caller can continue from there.
func (i *ingester) trySendCompletedBlocks(
	ctx context.Context,
	collectedBlocks map[int64]models.RPCBlock,
	nextBlockToSend int64,
) (int64, error) {
	// Outer loop: We might need to send multiple batch requests if our buffer is too big
	for _, ok := collectedBlocks[nextBlockToSend]; ok; _, ok = collectedBlocks[nextBlockToSend] {
		// Collect a blocks of blocks to send, only send those which are in order
		// Collect a batch to send, only send those which are in order
		blockBatch := make([]models.RPCBlock, 0, maxBatchSize)
		for block, ok := collectedBlocks[nextBlockToSend]; ok; block, ok = collectedBlocks[nextBlockToSend] {
			// Skip Failed block if we're configured to skip Failed blocks
			if i.cfg.SkipFailedBlocks && block.Errored() {
				nextBlockToSend++
				continue
			}

			blockBatch = append(blockBatch, block)
			delete(collectedBlocks, nextBlockToSend)
			nextBlockToSend++

			if len(blockBatch) == maxBatchSize {
				break
			}
		}

		if len(blockBatch) == 0 {
			return nextBlockToSend, nil
		}

		// Send the batch
		lastBlockNumber := blockBatch[len(blockBatch)-1].BlockNumber
		if lastBlockNumber != nextBlockToSend-1 {
			panic("unexpected last block number")
		}
		if err := i.dune.SendBlocks(ctx, blockBatch); err != nil {
			if errors.Is(err, context.Canceled) {
				i.log.Info("SendBlocks: Context canceled, stopping")
				return nextBlockToSend, nil
			}
			// TODO: handle errors of duneAPI, save the blockRange impacted and report this back for later retries
			err := errors.Errorf("failed to send batch: %w", err)
			i.log.Error("SendBlocks: Failed to send batch, exiting", "error", err)
			return nextBlockToSend, err
		}
		atomic.StoreInt64(&i.info.IngestedBlockNumber, lastBlockNumber)
	}
	i.log.Info(
		"Sent completed blocks to DuneAPI",
		"bufferSize", len(collectedBlocks),
		"nextBlockToSend", nextBlockToSend,
	)
	return nextBlockToSend, nil
}

func (i *ingester) tryUpdateLatestBlockNumber() int64 {
	latest, err := i.node.LatestBlockNumber()
	if err != nil {
		i.log.Error("Failed to get latest block number, continuing..", "error", err)
		return atomic.LoadInt64(&i.info.LatestBlockNumber)
	}
	atomic.StoreInt64(&i.info.LatestBlockNumber, latest)
	return latest
}

func (i *ingester) ReportProgress(ctx context.Context) error {
	timer := time.NewTicker(i.cfg.ReportProgressInterval)
	defer timer.Stop()

	previousTime := time.Now()
	previousDistance := int64(0)
	previousIngested := atomic.LoadInt64(&i.info.IngestedBlockNumber)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case tNow := <-timer.C:
			latest := atomic.LoadInt64(&i.info.LatestBlockNumber)
			lastIngested := atomic.LoadInt64(&i.info.IngestedBlockNumber)

			blocksPerSec := float64(lastIngested-previousIngested) / tNow.Sub(previousTime).Seconds()
			newDistance := latest - lastIngested
			fallingBehind := newDistance > (previousDistance + 1) // TODO: make this more stable

			rpcErrors := len(i.info.RPCErrors)
			duneErrors := len(i.info.DuneErrors)
			fields := []interface{}{
				"blocksPerSec", fmt.Sprintf("%.2f", blocksPerSec),
				"latestBlockNumber", latest,
				"ingestedBlockNumber", lastIngested,
			}
			if fallingBehind {
				fields = append(fields, "fallingBehind", fallingBehind)
			}
			if newDistance > 1 {
				etaHours := time.Duration(float64(newDistance) / blocksPerSec * float64(time.Second)).Hours()
				fields = append(fields, "hoursToCatchUp", fmt.Sprintf("%.1f", etaHours))
			}
			if rpcErrors > 0 {
				fields = append(fields, "rpcErrors", rpcErrors)
			}
			if duneErrors > 0 {
				fields = append(fields, "duneErrors", duneErrors)
			}

			i.log.Info("PROGRESS REPORT", fields...)
			previousIngested = lastIngested
			previousDistance = newDistance
			previousTime = tNow

			// TODO: include errors in the report, reset the error list
			err := i.dune.PostProgressReport(ctx, models.BlockchainIndexProgress{
				BlockchainName:          i.cfg.BlockchainName,
				EVMStack:                i.cfg.Stack.String(),
				LastIngestedBlockNumber: lastIngested,
				LatestBlockNumber:       latest,
			})
			if err != nil {
				i.log.Error("Failed to post progress report", "error", err)
			}
		}
	}
}

func (i *ingester) Close() error {
	// Send a final progress report to flush progress
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	i.log.Info("Sending final progress report")
	err := i.dune.PostProgressReport(
		ctx,
		models.BlockchainIndexProgress{
			BlockchainName:          i.cfg.BlockchainName,
			EVMStack:                i.cfg.Stack.String(),
			LastIngestedBlockNumber: i.info.IngestedBlockNumber,
			LatestBlockNumber:       i.info.LatestBlockNumber,
		})
	i.log.Info("Closing node")
	if err != nil {
		_ = i.node.Close()
		return err
	}

	return i.node.Close()
}
