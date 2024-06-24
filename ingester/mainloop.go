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
	errGroup, ctx := errgroup.WithContext(ctx)

	blockNumbers := make(chan int64)
	defer close(blockNumbers)
	blocks := make(chan models.RPCBlock)
	defer close(blocks)

	// Start MaxBatchSize goroutines to consume blocks concurrently
	for range i.cfg.MaxBatchSize {
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
		"max_batch_size", i.cfg.MaxBatchSize,
		"run_forever", maxCount <= 0,
		"start_block_number", startBlockNumber,
		"end_block_number", endBlockNumber,
		"batch_size", i.cfg.MaxBatchSize,
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
	i.log.Info("Produce block numbers from", "startBlockNumber", startBlockNumber, "endBlockNumber", endBlockNumber)
	for blockNumber := startBlockNumber; dontStop || blockNumber <= endBlockNumber; blockNumber++ {
		latestBlockNumber = waitForBlock(ctx, blockNumber, latestBlockNumber)

		select {
		case <-ctx.Done():
			i.log.Info("ProduceBlockNumbers: Context canceled, stopping")
			return ctx.Err()
		case blockNumbers <- blockNumber:
		}

		distanceFromLatest := latestBlockNumber - blockNumber
		if distanceFromLatest > 0 {
			// TODO: improve logs of processing speed and catchup estimated ETA
			i.log.Info("We're behind, trying to catch up..",
				"blockNumber", blockNumber,
				"latestBlockNumber", latestBlockNumber,
				"distanceFromLatest", distanceFromLatest,
			)
		}
	}
	i.log.Info("Finished producing block numbers")
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
					i.log.Info("FetchBlockLoop: Context canceled, stopping")
					return ctx.Err()
				}

				i.log.Error("Failed to get block by number, continuing..",
					"blockNumber", blockNumber,
					"error", err,
				)
				i.info.RPCErrors = append(i.info.RPCErrors, ErrorInfo{
					Timestamp:   time.Now(),
					BlockNumber: blockNumber,
					Error:       err,
				})

				// TODO: should we sleep (backoff) here?
				continue
			}

			atomic.StoreInt64(&i.info.ConsumedBlockNumber, block.BlockNumber)
			getBlockElapsed := time.Since(startTime)
			i.log.Info("FetchBlockLoop: Got block by number", "blockNumber", blockNumber, "elapsed", getBlockElapsed)
			startTime = time.Now()
			select {
			case <-ctx.Done():
				i.log.Info("FetchBlockLoop: Channel is closed, not sending block to channel", "blockNumber", block.BlockNumber)
				return ctx.Err()
			case blocks <- block:
				i.log.Info("FetchBlockLoop: Sent block", "blockNumber", blockNumber, "elapsed", time.Since(startTime))
			}
		}
	}
}

// SendBlocks to Dune. We receive blocks from the FetchBlockLoop goroutines, potentially out of order.
// We buffer the blocks in a map until we have no gaps, so that we can send them in order to Dune.
func (i *ingester) SendBlocks(ctx context.Context, blocksCh <-chan models.RPCBlock, startBlockNumber int64) error {
	i.log.Info("SendBlocks: Starting to receive blocks")
	blocks := make(map[int64]models.RPCBlock) // Buffer for temporarily storing blocks that have arrived out of order
	nextNumberToSend := startBlockNumber
	batchTimer := time.NewTicker(i.cfg.BatchRequestInterval)
	defer batchTimer.Stop()
	for {
		// Check if we can send any blocks before checking if we should shut down
		select {
		case <-batchTimer.C:
			nextNumberToSend = i.trySendCompletedBlocks(blocks, nextNumberToSend)
			i.log.Info("SendBlocks: Sent completed blocks to DuneAPI", "nextNumberToSend", nextNumberToSend)
		default:
		}

		// Receive a block or exit if the context is done or the channel is closed
		// To avoid blocking on the select, we also select on the batchTimer tick
		select {
		case <-ctx.Done():
			i.log.Info("SendBlocks: Context canceled, stopping")
			return ctx.Err()
		case block, ok := <-blocksCh:
			if !ok {
				i.log.Info("SendBlocks: Channel is closed, returning")
				return nil
			}
			blocks[block.BlockNumber] = block
			i.log.Info("SendBlocks: Received block", "blockNumber", block.BlockNumber, "bufferSize", len(blocks))
		case <-batchTimer.C:
			nextNumberToSend = i.trySendCompletedBlocks(blocks, nextNumberToSend)
			i.log.Info("SendBlocks: Sent completed blocks to DuneAPI", "nextNumberToSend", nextNumberToSend)
		}
	}
}

// trySendCompletedBlocks sends all blocks that can be sent in order from the blockMap.
// Once we have sent all blocks, if any, we return with the nextNumberToSend.
// We return the next numberToSend such that the caller can continue from there.
// We use a new context so we're able to flush the buffer even if the main context is canceled.
func (i *ingester) trySendCompletedBlocks(
	blocks map[int64]models.RPCBlock,
	nextNumberToSend int64,
) int64 {
	// Collect a batch of blocks to send, only send those which are in order
	batch := make([]models.RPCBlock, 0, len(blocks))
	for block, ok := blocks[nextNumberToSend]; ok; block, ok = blocks[nextNumberToSend] {
		batch = append(batch, block)
		delete(blocks, nextNumberToSend)
		nextNumberToSend++
	}

	if len(batch) == 0 {
		return nextNumberToSend
	}

	blockNumbers := make([]string, len(batch))
	for i, block := range batch {
		blockNumbers[i] = fmt.Sprintf("%d", block.BlockNumber)
	}

	i.log.Info(
		"SendBlocks: Sending batch",
		"blockNumberFirst", batch[0].BlockNumber,
		"blockNumberLast", batch[len(batch)-1].BlockNumber,
		"batchSize", len(batch),
	)

	// Send the batch, with a new context so we're able to flush the buffer even if the context is canceled
	ctx, cancel := context.WithTimeout(context.Background(), time.Second) // Some timeout
	defer cancel()
	if err := i.dune.SendBlocks(ctx, batch); err != nil {
		if errors.Is(err, context.Canceled) {
			i.log.Info("SendBlocks: Context canceled, stopping")
			return nextNumberToSend
		}
		// this will leave a "block gap" in DuneAPI, TODO: implement a way to fill this gap
		i.log.Error(
			"SendBlocks: Failed to send batch, continuing",
			"blockNumberFirst", batch[0].BlockNumber,
			"blockNumberLast", batch[len(batch)-1].BlockNumber,
			"error", err,
		)
		i.info.DuneErrors = append(i.info.DuneErrors, ErrorInfo{
			Timestamp:   time.Now(),
			BlockNumber: batch[0].BlockNumber, // TODO?
			Error:       err,
		})
	} else {
		i.log.Info("Updating latest ingested block number", "blockNumber", batch[len(batch)-1].BlockNumber)
		atomic.StoreInt64(&i.info.IngestedBlockNumber, batch[len(batch)-1].BlockNumber)
	}

	return nextNumberToSend
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
