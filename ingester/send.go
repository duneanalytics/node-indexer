package ingester

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/duneanalytics/blockchain-ingester/models"
	"github.com/go-errors/errors"
)

const maxBatchSize = 100

// SendBlocks to Dune. We receive blocks from the FetchBlockLoop goroutines, potentially out of order.
// We buffer the blocks in a map until we have no gaps, so that we can send them in order to Dune.
func (i *ingester) SendBlocks(ctx context.Context, blocks <-chan models.RPCBlock, startBlockNumber int64) error {
	// Buffer for temporarily storing blocks that have arrived out of order
	collectedBlocks := make(map[int64]models.RPCBlock, maxBatchSize)
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
	for {
		if len(collectedBlocks) < maxBatchSize/10 {
			// if we have very little to send, wait for next tick to avoid tiny batches impacting throughput
			return nextBlockToSend, nil
		}
		nextBlock, err := i.trySendBlockBatch(ctx, collectedBlocks, nextBlockToSend, maxBatchSize)
		if err != nil || nextBlock == nextBlockToSend {
			return nextBlock, err
		}
		nextBlockToSend = nextBlock
	}
}

func (i *ingester) trySendBlockBatch(
	ctx context.Context,
	collectedBlocks map[int64]models.RPCBlock,
	nextBlockToSend int64,
	maxBatchSize int,
) (int64, error) {
	startTime := time.Now()

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

		// Store error for reporting
		blocknumbers := make([]string, len(blockBatch))
		for i, block := range blockBatch {
			blocknumbers[i] = fmt.Sprintf("%d", block.BlockNumber)
		}
		i.info.DuneErrors = append(i.info.DuneErrors, ErrorInfo{
			Timestamp:    time.Now(),
			Error:        err,
			BlockNumbers: strings.Join(blocknumbers, ","),
		})

		err := errors.Errorf("failed to send batch: %w", err)
		i.log.Error("SendBlocks: Failed to send batch, exiting", "error", err)
		return nextBlockToSend, err
	}
	atomic.StoreInt64(&i.info.IngestedBlockNumber, lastBlockNumber)
	i.log.Info(
		"Sent blocks to DuneAPI",
		"batchSize", len(blockBatch),
		"nextBlockToSend", nextBlockToSend,
		"elapsed", time.Since(startTime),
	)
	return nextBlockToSend, nil
}
