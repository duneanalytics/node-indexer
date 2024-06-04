package ingester

import (
	"context"
	"sync"
	"time"

	"github.com/duneanalytics/blockchain-ingester/models"
	"github.com/go-errors/errors"
	"golang.org/x/sync/errgroup"
)

func (i *ingester) Run(ctx context.Context, wg *sync.WaitGroup) error {
	defer wg.Done()

	inFlightChan := make(chan models.RPCBlock, i.cfg.MaxBatchSize)
	defer close(inFlightChan)

	var err error

	startBlockNumber := i.cfg.StartBlockHeight
	if startBlockNumber <= 0 {
		startBlockNumber, err = i.node.LatestBlockNumber()
		if err != nil {
			return errors.Errorf("failed to get latest block number: %w", err)
		}
	}
	i.log.Info("Starting ingester",
		"maxBatchSize", i.cfg.MaxBatchSize,
		"startBlockHeight", i.cfg.StartBlockHeight,
		"startBlockNumber", startBlockNumber,
	)

	errGroup, ctx := errgroup.WithContext(ctx)
	errGroup.Go(func() error {
		return i.ConsumeBlocks(ctx, inFlightChan, startBlockNumber, -1)
	})
	errGroup.Go(func() error {
		return i.SendBlocks(ctx, inFlightChan)
	})
	errGroup.Go(func() error {
		timer := time.NewTicker(20 * time.Second)
		defer timer.Stop()

		previousTime := time.Now()
		previousDistance := int64(0)
		previoussIngested := sync.LoadInt64(&i.info.IngestedBlockNumber)

		for {
			select {
			case tNow := <-timer.C:
				latest, err := i.node.LatestBlockNumber()
				if err != nil {
					i.log.Error("Failed to get latest block number, continuing..", "error", err)
					continue
				}
				sync.StoreInt64(&i.info.LatestBlockNumber, latest)
				lastIngested := sync.LoadInt64(&i.info.IngestedBlockNumber)
				lastConsumed := sync.LoadInt64(&i.info.ConsumedBlockNumber)

				blocksPerSec := float64(lastIngested-previoussIngested) / tNow.Sub(previousTime).Seconds()
				newDistance := latest - lastIngested
				fallingBehind := newDistance > (previousDistance + 1) // TODO: make is more stable

				i.log.Info("Info",
					"latestBlockNumber", latest,
					"ingestedBlockNumber", lastIngested,
					"consumedBlockNumber", lastConsumed,
					"distanceFromLatest", latest-lastIngested,
					"FallingBehind", falingBehind,
					"blocksPerSec", fmt.Sprintf("%.2f", blocksPerSec),
				)
				previoussIngested = lastIngested
				previousDistance = newDistance
				previousTime = tNow
			}
		}
	})

	return errGroup.Wait()
}

// ConsumeBlocks from the NPC Node
func (i *ingester) ConsumeBlocks(
	ctx context.Context, outChan chan models.RPCBlock, startBlockNumber, endBlockNumber int64,
) error {
	dontStop := endBlockNumber <= startBlockNumber
	for blockNumber := startBlockNumber; dontStop || startBlockNumber <= endBlockNumber; blockNumber++ {
		startTime := time.Now()
		block, err := i.node.BlockByNumber(ctx, blockNumber)
		if err != nil {
			i.log.Error("Failed to get block by number, continuing..",
				"blockNumber", blockNumber,
				"error", err,
			)
			i.info.RPCErrors = append(i.info.RPCErrors, ErrorInfo{
				Timestamp:   time.Now(),
				BlockNumber: blockNumber,
				Error:       err,
			})
			continue
		} else {
			sync.StoreInt64(&i.info.ConsumedBlockNumber, block.BlockNumber)
		}
		// TODO:
		//  - track if we're getting blocked on sending to outChan
		//  - track blocks per second and our distance from LatestBlockNumber
		select {
		case <-ctx.Done():
			return nil
		case outChan <- block:
		}
		sleepTime := time.Until(startTime.Add(i.cfg.PollInterval))
		time.Sleep(sleepTime)
	}
	return nil
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
				sync.StoreInt64(&i.info.IngestedBlockNumber, payload.BlockNumber)
			}
		}
	}
}

func (i *ingester) Close() error {
	return i.node.Close()
}
