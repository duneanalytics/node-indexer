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
	previousHoursToCatchUp := float64(0)
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

			rpcErrors := len(i.info.RPCErrors)
			duneErrors := len(i.info.DuneErrors)
			fields := []interface{}{
				"blocksPerSec", fmt.Sprintf("%.2f", blocksPerSec),
				"latestBlockNumber", latest,
				"ingestedBlockNumber", lastIngested,
			}
			if newDistance > 1 {
				etaHours := time.Duration(float64(newDistance) / blocksPerSec * float64(time.Second)).Hours()
				fields = append(fields, "hoursToCatchUp", fmt.Sprintf("%.1f", etaHours))
				if previousHoursToCatchUp < (0.8 * etaHours) {
					fields = append(fields, "fallingBehind", true)
				}
				previousHoursToCatchUp = etaHours
			}
			if rpcErrors > 0 {
				fields = append(fields, "rpcErrors", rpcErrors)
			}
			if duneErrors > 0 {
				fields = append(fields, "duneErrors", duneErrors)
			}

			i.log.Info("PROGRESS REPORT", fields...)
			previousIngested = lastIngested
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
