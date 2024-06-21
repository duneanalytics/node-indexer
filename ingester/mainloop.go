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
// ProduceBlockNumbers (blockNumbers channel) -> ConsumeBlocks (blocks channel) -> SendBlocks -> Dune
//
// We produce block numbers to fetch on an unbuffered channel (ProduceBlockNumbers),
// and each concurrent ConsumeBlock goroutine gets a block number from that channel.
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
			return i.ConsumeBlocks(ctx, blockNumbers, blocks)
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

var ErrFinishedConsumeBlocks = errors.New("finished ConsumeBlocks")

// ProduceBlockNumbers to be consumed by multiple goroutines running ConsumeBlocks
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
	return ErrFinishedConsumeBlocks
}

// ConsumeBlocks from the RPC node. This can be run in multiple goroutines to parallelize block fetching.
func (i *ingester) ConsumeBlocks(
	ctx context.Context, blockNumbers chan int64, blocks chan models.RPCBlock,
) error {
	for {
		select {
		case <-ctx.Done():
			i.log.Info("ConsumeBlocks: context is done")
			return ctx.Err()
		case blockNumber := <-blockNumbers:
			startTime := time.Now()

			i.log.Info("Getting block by number", "blockNumber", blockNumber)
			block, err := i.node.BlockByNumber(ctx, blockNumber)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					i.log.Info("ConsumeBlocks: Context canceled, stopping")
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
			i.log.Info("Got block by number", "blockNumber", blockNumber, "elapsed", getBlockElapsed)
			select {
			case <-ctx.Done():
				i.log.Info("ConsumeBlocks: Channel is closed, not sending block to channel", "blockNumber", block.BlockNumber)
				return ctx.Err()
			case blocks <- block:
				i.log.Info("Sent block")
			}
		}
	}
}

// SendBlocks to Dune. We receive blocks from the ConsumeBlocks goroutines, potentially out of order.
// We buffer the blocks in a map until we have no gaps, so that we can send them in order to Dune.
func (i *ingester) SendBlocks(ctx context.Context, blocksCh <-chan models.RPCBlock, startBlockNumber int64) error {
	i.log.Info("SendBlocks: Starting to receive blocks")
	blockMap := make(map[int64]models.RPCBlock) // Buffer for temporarily storing blocks that have arrived out of order
	next := startBlockNumber
	for {
		select {
		case <-ctx.Done():
			i.log.Info("SendBlocks: Context canceled, stopping")
			return ctx.Err()
		case block, ok := <-blocksCh:
			if !ok {
				i.log.Info("SendBlocks: Channel is closed, returning")
				return nil
			}

			blockMap[block.BlockNumber] = block
			i.log.Info("Received block", "blockNumber", block.BlockNumber)

			// Send this block only if we have sent all previous blocks
			for block, ok := blockMap[next]; ok; block, ok = blockMap[next] {
				i.log.Info("SendBlocks: Sending block to DuneAPI", "blockNumber", block.BlockNumber)
				if err := i.dune.SendBlock(ctx, block); err != nil {
					if errors.Is(err, context.Canceled) {
						i.log.Info("SendBlocks: Context canceled, stopping")
						return ctx.Err()
					}
					// TODO: implement DeadLetterQueue
					// this will leave a "block gap" in DuneAPI, TODO: implement a way to fill this gap
					i.log.Error("SendBlocks: Failed, continuing", "blockNumber", block.BlockNumber, "error", err)
					i.info.DuneErrors = append(i.info.DuneErrors, ErrorInfo{
						Timestamp:   time.Now(),
						BlockNumber: block.BlockNumber,
						Error:       err,
					})
				} else {
					i.log.Info("Updating latest ingested block number", "blockNumber", block.BlockNumber)
					atomic.StoreInt64(&i.info.IngestedBlockNumber, block.BlockNumber)
				}

				// We've sent block N, so increment the pointer
				delete(blockMap, next)
				next++
			}
		}
	}
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
				fields = append(fields, "distanceFromLatest", newDistance)
			}
			if rpcErrors > 0 {
				fields = append(fields, "rpcErrors", rpcErrors)
			}
			if duneErrors > 0 {
				fields = append(fields, "duneErrors", duneErrors)
			}

			i.log.Info("ProgressReport", fields...)
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
