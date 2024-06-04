package jsonrpc

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"

	"github.com/duneanalytics/blockchain-ingester/models"
	"golang.org/x/sync/errgroup"
)

type OpStackClient struct {
	rpcClient
}

var _ BlockchainClient = &OpStackClient{}

func NewOpStackClient(cfg Config, log *slog.Logger) *OpStackClient {
	rpcClient := NewRPCClient(cfg, log)
	return &OpStackClient{*rpcClient}
}

// BlockByNumber returns the block with the given blockNumber.
// it uses 3 different methods to get the block:
// 1. eth_getBlockByNumber
// 2. eth_getBlockReceipts
// 3. debug_traceBlockByNumber with tracer "callTracer"
// We encode the payload in NDJSON, in this order.
// TODO: debug_traceBlockByNumber should be optional
//
//	we should handle the case where it is not available
func (c *OpStackClient) BlockByNumber(ctx context.Context, blockNumber int64) (models.RPCBlock, error) {
	blockNumberHex := fmt.Sprintf("0x%x", blockNumber)

	// TODO: split this into mandatory and optional methods
	methods := []string{
		"eth_getBlockByNumber",
		"eth_getBlockReceipts",
		"debug_traceBlockByNumber",
	}
	methodArgs := map[string][]any{
		"eth_getBlockByNumber":     {blockNumberHex, true},
		"eth_getBlockReceipts":     {blockNumberHex},
		"debug_traceBlockByNumber": {blockNumberHex, `{"tracer":"callTracer"}`},
	}
	group, ctx := errgroup.WithContext(ctx)
	results := make([]*bytes.Buffer, len(methods))
	for i, method := range methods {
		results[i] = c.bufPool.Get().(*bytes.Buffer)
		defer c.bufPool.Put(results[i])

		group.Go(func() error {
			results[i].Reset()
			err := c.getResponseBody(ctx, method, methodArgs[method], results[i])
			if err != nil {
				c.log.Error("Failed to get response for jsonRPC",
					"method", method,
					"error", err,
				)
			}
			return err
		})
	}

	if err := group.Wait(); err != nil {
		return models.RPCBlock{}, err
	}

	// copy the responses in order
	var buffer bytes.Buffer
	for _, res := range results {
		buffer.Grow(res.Len() + 1)
		buffer.ReadFrom(res)
		buffer.WriteString("\n")
	}
	return models.RPCBlock{
		BlockNumber: blockNumber,
		Payload:     buffer.Bytes(),
	}, nil
}

func (c *OpStackClient) SendBlocks(
	ctx context.Context, outChan chan models.RPCBlock, startBlockNumber, endBlockNumber int64,
) error {
	dontStop := endBlockNumber <= startBlockNumber
	for blockNumber := startBlockNumber; dontStop || startBlockNumber <= endBlockNumber; blockNumber++ {
		block, err := c.BlockByNumber(ctx, blockNumber)
		if err != nil {
			c.log.Error("Failed to get block by number",
				"blockNumber", blockNumber,
				"error", err,
			)
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case outChan <- block:
		}
	}
	return nil
}
