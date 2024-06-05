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

func (i *ingester) Run(ctx context.Context, startBlockNumber, maxCount int64) error {
	inFlightChan := make(chan models.RPCBlock, i.cfg.MaxBatchSize)
	defer close(inFlightChan)

	var err error

	if startBlockNumber < 0 {
		startBlockNumber, err = i.node.LatestBlockNumber()
		if err != nil {
			return errors.Errorf("failed to get latest block number: %w", err)
		}
	}

	i.log.Info("Starting ingester",
		"maxBatchSize", i.cfg.MaxBatchSize,
		"startBlockNumber", startBlockNumber,
		"maxCount", maxCount,
	)

	errGroup, ctx := errgroup.WithContext(ctx)
	errGroup.Go(func() error {
		return i.ConsumeBlocks(ctx, inFlightChan, startBlockNumber, startBlockNumber+maxCount)
	})
	errGroup.Go(func() error {
		return i.SendBlocks(ctx, inFlightChan)
	})
	errGroup.Go(func() error {
		return i.ReportProgress(ctx)
	})

	if err := errGroup.Wait(); err != nil && err != errFinishedConsumeBlocks {
		return err
	}
	return nil
}

var errFinishedConsumeBlocks = errors.New("finished ConsumeBlocks")

// ConsumeBlocks from the NPC Node
func (i *ingester) ConsumeBlocks(
	ctx context.Context, outChan chan models.RPCBlock, startBlockNumber, endBlockNumber int64,
) error {
	dontStop := endBlockNumber <= startBlockNumber
	latestBlockNumber := i.tryUpdateLatestBlockNumber()

	waitForBlock := func(blockNumber, latestBlockNumber int64) int64 {
		for blockNumber > latestBlockNumber {
			i.log.Info(fmt.Sprintf("Waiting %v for block to be available..", i.cfg.PollInterval),
				"blockNumber", blockNumber,
				"latestBlockNumber", latestBlockNumber,
			)
			time.Sleep(i.cfg.PollInterval)
			latestBlockNumber = i.tryUpdateLatestBlockNumber()
		}
		return latestBlockNumber
	}

	for blockNumber := startBlockNumber; dontStop || blockNumber <= endBlockNumber; blockNumber++ {

		latestBlockNumber = waitForBlock(blockNumber, latestBlockNumber)
		startTime := time.Now()

		block, err := i.node.BlockByNumber(ctx, blockNumber)
		if err != nil {
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
			return nil
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
	return errFinishedConsumeBlocks
}

func (i *ingester) SendBlocks(ctx context.Context, blocksCh <-chan models.RPCBlock) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case payload, ok := <-blocksCh:
			// TODO: we should batch RCP blocks here before sending to Dune.
			if !ok {
				return nil // channel closed
			}
			if err := i.dune.SendBlock(payload); err != nil {
				// TODO: implement DeadLetterQueue
				// this will leave a "block gap" in DuneAPI, TODO: implement a way to fill this gap
				i.log.Error("SendBlock failed, continuing..", "blockNumber", payload.BlockNumber, "error", err)
				i.info.DuneErrors = append(i.info.DuneErrors, ErrorInfo{
					Timestamp:   time.Now(),
					BlockNumber: payload.BlockNumber,
					Error:       err,
				})
			} else {
				atomic.StoreInt64(&i.info.IngestedBlockNumber, payload.BlockNumber)
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
	timer := time.NewTicker(20 * time.Second)
	defer timer.Stop()

	previousTime := time.Now()
	previousDistance := int64(0)
	previousIngested := atomic.LoadInt64(&i.info.IngestedBlockNumber)

	for {
		select {
		case <-ctx.Done():
			return nil
		case tNow := <-timer.C:
			latest := atomic.LoadInt64(&i.info.LatestBlockNumber)
			lastIngested := atomic.LoadInt64(&i.info.IngestedBlockNumber)
			lastConsumed := atomic.LoadInt64(&i.info.ConsumedBlockNumber)

			blocksPerSec := float64(lastIngested-previousIngested) / tNow.Sub(previousTime).Seconds()
			newDistance := latest - lastIngested
			fallingBehind := newDistance > (previousDistance + 1) // TODO: make is more stable

			i.log.Info("Info",
				"latestBlockNumber", latest,
				"ingestedBlockNumber", lastIngested,
				"consumedBlockNumber", lastConsumed,
				"distanceFromLatest", latest-lastIngested,
				"FallingBehind", fallingBehind,
				"blocksPerSec", fmt.Sprintf("%.2f", blocksPerSec),
			)
			previousIngested = lastIngested
			previousDistance = newDistance
			previousTime = tNow
		}
	}
}

func (i *ingester) Close() error {
	return i.node.Close()
}
