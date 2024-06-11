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
	inFlightChan := make(chan models.RPCBlock, i.cfg.MaxBatchSize)

	blockChan := make(chan int64, i.cfg.MaxBatchSize)

	// var err error

	// TODO: Use progress endpoint
	// TODO: Fetch blocks in parallel

	i.tryUpdateLatestBlockNumber()
	i.log.Info("Got latest block number", "latestBlockNumber", i.info.LatestBlockNumber)

	// endBlockNumber := startBlockNumber+maxCount

	i.log.Info("Starting ingester",
		"maxBatchSize", i.cfg.MaxBatchSize,
		"startBlockNumber", startBlockNumber,
		"maxCount", maxCount,
	)

	errGroup, ctx := errgroup.WithContext(ctx)
	errGroup.Go(func() error {
		defer close(blockChan)
		return i.ProduceBlockNumbers(ctx, blockChan, startBlockNumber, startBlockNumber+maxCount)
	})
	errGroup.Go(func() error {
		defer close(inFlightChan)
		return i.ConsumeBlocks(ctx, blockChan, inFlightChan)
	})
	errGroup.Go(func() error {
		return i.SendBlocks(ctx, inFlightChan)
	})
	errGroup.Go(func() error {
		return i.ReportProgress(ctx)
	})

	fmt.Println("waiting")
	if err := errGroup.Wait(); err != nil && err != ErrFinishedConsumeBlocks {
		fmt.Println("errro in wait")
		return err
	}
	fmt.Println("exiting run")
	return nil
}

// ProduceBlockNumbers on a channel
func (i *ingester) ProduceBlockNumbers(ctx context.Context, outChan chan int64, startBlockNumber, endBlockNumber int64) error {
	blockNumber := startBlockNumber

	fmt.Println("starting produce", startBlockNumber, endBlockNumber)

	// TODO: Update latest block number if we're caught up

	for blockNumber < i.info.LatestBlockNumber && blockNumber <= endBlockNumber {
		// i.log.Info("Attempting to produce block number", "blockNumber", blockNumber)
		fmt.Println("lol")
		select {
		case <-ctx.Done():
			return nil
		case outChan <- blockNumber:
			i.log.Info("Produced block number", "blockNumber", blockNumber)
			blockNumber++
		}
	}
	fmt.Println("exiting produce")
	return nil
}

var ErrFinishedConsumeBlocks = errors.New("finished ConsumeBlocks")

// ConsumeBlocks from the NPC Node
func (i *ingester) ConsumeBlocks(
	ctx context.Context, blockNumbers chan int64, blocks chan models.RPCBlock,
) error {
	for {
		select {
		case <-ctx.Done():
			return nil // context canceled
		case blockNumber, ok := <-blockNumbers:
			fmt.Println("got block number", blockNumber)
			// TODO: we should batch RPC blocks here before sending to Dune.
			if !ok {
				fmt.Println("channel was closed")
				fmt.Println("exiting consume")
				return nil // channel closed
			}
			i.log.Info("Got block number", "blockNumber", blockNumber)
			startTime := time.Now()
			block, err := i.node.BlockByNumber(ctx, blockNumber)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					i.log.Info("Context canceled, stopping..")
					fmt.Println("exiting consume")
					return err
				}

				i.log.Error("Failed to get block by number, continuing..",
					"blockNumber", blockNumber,
					"latestBlockNumber", i.info.LatestBlockNumber,
					"error", err,
				)
				i.info.RPCErrors = append(i.info.RPCErrors, ErrorInfo{
					Timestamp:   time.Now(),
					BlockNumber: blockNumber,
					Error:       err,
				})

				i.log.Info("Waiting for block number", "blockNumber", blockNumber)
				continue
			}

			atomic.StoreInt64(&i.info.ConsumedBlockNumber, block.BlockNumber)
			getBlockElapsed := time.Since(startTime)

			// Send block
			select {
			case <-ctx.Done():
				fmt.Println("exiting consume ctx err")
				return ctx.Err()
			case blocks <- block:
			}

			distanceFromLatest := i.info.LatestBlockNumber - block.BlockNumber
			if distanceFromLatest > 0 {
				// TODO: improve logs of processing speed and catchup estimated ETA
				i.log.Info("We're behind, trying to catch up..",
					"blockNumber", block.BlockNumber,
					"latestBlockNumber", i.info.LatestBlockNumber,
					"distanceFromLatest", distanceFromLatest,
					"getBlockElapsedMillis", getBlockElapsed.Milliseconds(),
					"elapsedMillis", time.Since(startTime).Milliseconds(),
				)
			}
			i.log.Info("Waiting for block number", "blockNumber", blockNumber)
		}
	}
}

func (i *ingester) SendBlocks(ctx context.Context, blocksCh <-chan models.RPCBlock) error {
	for {
		fmt.Println("Hello")
		select {
		case _, ok := <-ctx.Done():
			fmt.Println("ctx done, exiting send", ok)
			return nil // context canceled
		case payload, ok := <-blocksCh:
			// TODO: we should batch RPC blocks here before sending to Dune.
			if !ok {
				fmt.Println("sendblocks: channel closedd")
				fmt.Println("exiting send")
				return nil // channel closed
			}
			fmt.Println("sending block", payload.BlockNumber)
			err := i.dune.SendBlock(ctx, payload)
			if err != nil {
				fmt.Println("got block err")
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
			fmt.Printf("got result %v\n", err)
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
		if ctx.Err() != nil {
			fmt.Println("exiting report progress ctx err")
			return ctx.Err()
		}
		select {
		case _, ok := <-ctx.Done():
			fmt.Println("exiting report progress", ok)
			return nil
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
		}
	}
}

func (i *ingester) Close() error {
	return i.node.Close()
}
