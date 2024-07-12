package ingester

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync/atomic"
	"time"

	"github.com/duneanalytics/blockchain-ingester/lib/dlq"
	"github.com/emirpasic/gods/utils"

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
	//
	if i.cfg.DLQOnly {
		i.cfg.MaxConcurrentRequests = 0 // if running DLQ Only mode, ignore the MaxConcurrentRequests and set this to 0
	} else {
		if i.cfg.MaxConcurrentRequests <= 0 {
			return errors.Errorf("MaxConcurrentRequests must be > 0")
		}
	}
	if i.cfg.MaxConcurrentRequestsDLQ <= 0 {
		return errors.Errorf("MaxConcurrentRequestsDLQ must be > 0")
	}

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

	// Start DLQ processing

	blockNumbersDLQ := make(chan dlq.Item[int64])
	defer close(blockNumbersDLQ)

	blocksDLQ := make(chan dlq.Item[models.RPCBlock], i.cfg.MaxConcurrentRequestsDLQ+1)
	defer close(blocksDLQ)

	errGroup.Go(func() error {
		return i.SendBlocksDLQ(ctx, blocksDLQ)
	})
	for range i.cfg.MaxConcurrentRequestsDLQ {
		errGroup.Go(func() error {
			return i.FetchBlockLoopDLQ(ctx, blockNumbersDLQ, blocksDLQ)
		})
	}
	errGroup.Go(func() error {
		return i.ProduceBlockNumbersDLQ(ctx, blockNumbersDLQ)
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
			i.log.Debug(
				"Waiting for block to be available",
				"waitTime", i.cfg.PollInterval.String(),
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
					"continuing", i.cfg.SkipFailedBlocks,
					"elapsed", time.Since(startTime),
					"error", err,
				)
				if !i.cfg.SkipFailedBlocks {
					return err
				}
				select {
				case <-ctx.Done():
					i.log.Debug("FetchBlockLoop: Channel is closed, not sending block to channel", "blockNumber", block.BlockNumber)
					return ctx.Err()
				case blocks <- models.RPCBlock{BlockNumber: blockNumber, Error: err}:
				}
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
			if i.info.Errors.RPCErrorCount > 0 {
				fields = append(fields, "rpcErrors", i.info.Errors.RPCErrorCount)
			}
			if i.info.Errors.DuneErrorCount > 0 {
				fields = append(fields, "duneErrors", i.info.Errors.DuneErrorCount)
			}

			i.log.Info("PROGRESS REPORT", fields...)
			previousIngested = lastIngested
			previousTime = tNow

			err := i.dune.PostProgressReport(ctx, i.info.ToProgressReport())
			if err != nil {
				i.log.Error("Failed to post progress report", "error", err)
			} else {
				i.log.Debug("Posted progress report")
				i.info.ResetErrors()
			}
		}
	}
}

func (i *ingester) ProduceBlockNumbersDLQ(ctx context.Context, outChan chan dlq.Item[int64]) error {
	for {
		select {
		case <-ctx.Done():
			i.log.Debug("ProduceBlockNumbersDLQ: Context canceled, stopping")
			return ctx.Err()
		default:
			block, ok := i.dlq.GetNextItem()

			if ok {
				if i.log.Enabled(ctx, slog.LevelDebug) {
					i.log.Debug("ProduceBlockNumbersDLQ: Reprocessing block", "block", block,
						"dlqSize", i.dlq.Size())
				}
				select {
				case outChan <- *block:
					// Successfully sent the block to the out channel
				case <-ctx.Done():
					i.log.Debug("ProduceBlockNumbersDLQ: Context canceled while sending block, stopping")
					return ctx.Err()
				}
			} else {
				if i.log.Enabled(ctx, slog.LevelDebug) {
					i.log.Debug("ProduceBlockNumbersDLQ: No eligible blocks in the DLQ so sleeping",
						"dlqSize", i.dlq.Size())
				}
				select {
				case <-time.After(i.cfg.PollDLQInterval): // Polling interval when DLQ is empty
				case <-ctx.Done():
					i.log.Debug("ProduceBlockNumbersDLQ: Context canceled while sleeping, stopping")
					return ctx.Err()
				}
			}
		}
	}
}

func (i *ingester) FetchBlockLoopDLQ(ctx context.Context, blockNumbers <-chan dlq.Item[int64],
	blocks chan<- dlq.Item[models.RPCBlock],
) error {
	for {
		select {
		case <-ctx.Done():
			i.log.Info("FetchBlockLoopDLQ: context is done")
			return ctx.Err()
		case blockNumber := <-blockNumbers:
			startTime := time.Now()
			block, err := i.node.BlockByNumber(ctx, blockNumber.Value)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					i.log.Error("FetchBlockLoopDLQ: Context canceled, stopping")
					return ctx.Err()
				}
				i.log.Error("FetchBlockLoopDLQ: Failed to get block by number",
					"blockNumber", blockNumber,
					"elapsed", time.Since(startTime),
					"error", err,
				)
				select {
				case <-ctx.Done():
					i.log.Debug("FetchBlockLoop: Channel is closed, not sending block to channel", "blockNumber", block.BlockNumber)
					return ctx.Err()
				case blocks <- dlq.MapItem(blockNumber, func(blockNumber int64) models.RPCBlock {
					return models.RPCBlock{BlockNumber: blockNumber, Error: err}
				}):
				}
				continue
			}
			getBlockElapsed := time.Since(startTime)
			select {
			case <-ctx.Done():
				i.log.Debug("FetchBlockLoopDLQ: Channel is closed, not sending block to channel", "blockNumber", block.BlockNumber)
				return ctx.Err()
			case blocks <- dlq.MapItem(blockNumber, func(_ int64) models.RPCBlock {
				return block
			}):
				i.log.Debug(
					"FetchBlockLoopDLQ: Got and sent block",
					"blockNumber", blockNumber,
					"getBlockElapsed", getBlockElapsed,
				)
			}
		}
	}
}

func (i *ingester) SendBlocksDLQ(ctx context.Context, blocks <-chan dlq.Item[models.RPCBlock]) error {
	i.log.Debug("SendBlocksDLQ: Starting to receive blocks")
	for {
		select {
		case <-ctx.Done():
			i.log.Debug("SendBlocksDLQ: Context canceled, stopping")
			return ctx.Err()
		case block, ok := <-blocks:
			if !ok {
				i.log.Debug("SendBlocksDLQ: Channel is closed, returning")
				return nil
			}
			if block.Value.Errored() {
				i.dlq.AddItem(block.Value.BlockNumber, block.Retries)
				i.log.Error("Received FAILED block", "number", block.Value.BlockNumber)
				// TODO: report error once ErrorState struct is made thread-safe
			} else {
				i.log.Debug(
					"SendBlocksDLQ: Received block",
					"blockNumber", block.Value.BlockNumber,
				)
				if err := i.duneDLQ.SendBlocks(ctx, []models.RPCBlock{block.Value}); err != nil {
					if errors.Is(err, context.Canceled) {
						i.log.Info("SendBlocksDLQ: Context canceled, stopping")
						return ctx.Err()
					}
					i.log.Error("SendBlocksDLQ: Failed to send block, requeueing...", "block", block.Value.BlockNumber, "error", err)
					i.dlq.AddItem(block.Value.BlockNumber, block.Retries)
					// TODO: report error once ErrorState struct is made thread-safe
				}
			}
		}
	}
}

func (i *ingester) Close() error {
	// Send a final progress report to flush progress
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	i.log.Info("Sending final progress report")
	err := i.dune.PostProgressReport(ctx, i.info.ToProgressReport())
	i.log.Info("Closing node")
	if err != nil {
		_ = i.node.Close()
		return err
	}

	return i.node.Close()
}

func AddBlockGaps(dlq *dlq.DLQ[int64], gaps []models.BlockGap) {
	// queue these in reverse so that recent blocks are retried first
	slices.SortFunc(gaps, func(a, b models.BlockGap) int {
		return -utils.Int64Comparator(a.FirstMissing, b.FirstMissing)
	})

	for _, gap := range gaps {
		for i := gap.FirstMissing; i <= gap.LastMissing; i++ {
			dlq.AddItem(i, 0)
		}
	}
}
