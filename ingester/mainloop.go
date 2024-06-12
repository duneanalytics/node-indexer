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

func (i *ingester) Run(ctx context.Context, startBlockNumber int64, maxCount int64) error {
	ctx, cancel := context.WithCancel(ctx)

	inFlightChan := make(chan models.RPCBlock, i.cfg.MaxBatchSize) // we close this after ConsumeBlocks has returned

	// Ingest until endBlockNumber, inclusive. If maxCount is <= 0, we ingest forever
	endBlockNumber := startBlockNumber - 1 + maxCount

	i.log.Info("Starting ingester",
		"maxBatchSize", i.cfg.MaxBatchSize,
		"startBlockNumber", startBlockNumber,
		"endBlockNumber", endBlockNumber,
		"maxCount", maxCount,
	)

	errGroup, ctx := errgroup.WithContext(ctx)
	errGroup.Go(func() error {
		return i.SendBlocks(ctx, inFlightChan)
	})
	errGroup.Go(func() error {
		return i.ReportProgress(ctx)
	})

	err := i.ConsumeBlocks(ctx, inFlightChan, startBlockNumber, endBlockNumber)
	close(inFlightChan)
	cancel()
	if err != nil {
		if err := errGroup.Wait(); err != nil {
			i.log.Error("errgroup wait", "error", err)
		}
		return errors.Errorf("consume blocks: %w", err)
	}

	if err := errGroup.Wait(); err != nil && err != ErrFinishedConsumeBlocks {
		return err
	}

	return nil
}

var ErrFinishedConsumeBlocks = errors.New("finished ConsumeBlocks")

// ConsumeBlocks from the NPC Node
func (i *ingester) ConsumeBlocks(
	ctx context.Context, outChan chan models.RPCBlock, startBlockNumber, endBlockNumber int64,
) error {
	latestBlockNumber := i.tryUpdateLatestBlockNumber()

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

	for blockNumber := startBlockNumber; dontStop || blockNumber <= endBlockNumber; blockNumber++ {
		latestBlockNumber = waitForBlock(ctx, blockNumber, latestBlockNumber)
		startTime := time.Now()

		i.log.Info("Getting block by number", "blockNumber", blockNumber, "latestBlockNumber", latestBlockNumber)
		block, err := i.node.BlockByNumber(ctx, blockNumber)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				i.log.Info("Context canceled, stopping..")
				return ctx.Err()
			}

			i.log.Error("Failed to get block by number, continuing..",
				"blockNumber", blockNumber,
				"latestBlockNumber", latestBlockNumber,
				"error", err,
			)
			i.info.RPCErrors = append(i.info.RPCErrors, ErrorInfo{
				Timestamp:   time.Now(),
				BlockNumber: blockNumber,
				Error:       err,
			})

			// TODO: should I sleep (backoff) here?
			continue
		}

		atomic.StoreInt64(&i.info.ConsumedBlockNumber, block.BlockNumber)
		getBlockElapsed := time.Since(startTime)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case outChan <- block:
		}

		distanceFromLatest := latestBlockNumber - block.BlockNumber
		if distanceFromLatest > 0 {
			// TODO: improve logs of processing speed and catchup estimated ETA
			i.log.Info("We're behind, trying to catch up..",
				"blockNumber", block.BlockNumber,
				"latestBlockNumber", latestBlockNumber,
				"distanceFromLatest", distanceFromLatest,
				"getBlockElapsedMillis", getBlockElapsed.Milliseconds(),
				"elapsedMillis", time.Since(startTime).Milliseconds(),
			)
		}
	}
	// Done consuming blocks, either because we reached the endBlockNumber or the context was canceled
	i.log.Info("Finished consuming blocks", "latestBlockNumber", latestBlockNumber, "endBlockNumber", endBlockNumber)
	return ErrFinishedConsumeBlocks
}

func (i *ingester) SendBlocks(ctx context.Context, blocksCh <-chan models.RPCBlock) error {
	for payload := range blocksCh {
		// TODO: we should batch RCP blocks here before sending to Dune.
		if err := i.dune.SendBlock(ctx, payload); err != nil {
			if errors.Is(err, context.Canceled) {
				i.log.Info("Context canceled, stopping..")
				return ctx.Err()
			}
			// TODO: implement DeadLetterQueue
			// this will leave a "block gap" in DuneAPI, TODO: implement a way to fill this gap
			i.log.Error("SendBlock failed, continuing..", "blockNumber", payload.BlockNumber, "error", err)
			i.info.DuneErrors = append(i.info.DuneErrors, ErrorInfo{
				Timestamp:   time.Now(),
				BlockNumber: payload.BlockNumber,
				Error:       err,
			})
		} else {
			i.log.Info("Updating latest ingested block number", "blockNumber", payload.BlockNumber)
			atomic.StoreInt64(&i.info.IngestedBlockNumber, payload.BlockNumber)
		}
	}
	return ctx.Err() // channel closed
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
			fallingBehind := newDistance > (previousDistance + 1) // TODO: make is more stable

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
