package jsonrpc

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/duneanalytics/blockchain-ingester/models"
	"golang.org/x/sync/errgroup"
)

type OpStackClient struct {
	rpcClient
}

var _ BlockchainClient = &OpStackClient{}

func NewOpStackClient(log *slog.Logger, cfg Config) (*OpStackClient, error) {
	rpcClient, err := newClient(log.With("module", "jsonrpc"), cfg)
	if err != nil {
		return nil, err
	}
	return &OpStackClient{*rpcClient}, nil
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
	tStart := time.Now()
	defer func() {
		c.log.Debug("BlockByNumber", "blockNumber", blockNumber, "duration", time.Since(tStart))
	}()
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
		"debug_traceBlockByNumber": {blockNumberHex, map[string]string{"tracer": "callTracer"}},
	}
	group, ctx := errgroup.WithContext(ctx)
	results := make([]*bytes.Buffer, len(methods))
	for i, method := range methods {
		results[i] = c.bufPool.Get().(*bytes.Buffer)
		defer c.putBuffer(results[i])

		group.Go(func() error {
			errCh := make(chan error, 1)
			c.wrkPool.Submit(func() {
				defer close(errCh)
				err := c.getResponseBody(ctx, method, methodArgs[method], results[i])
				if err != nil {
					c.log.Error("Failed to get response for jsonRPC",
						"blockNumber", blockNumber,
						"method", method,
						"error", err,
					)
					errCh <- err
				} else {
					errCh <- nil
				}
			})
			return <-errCh
		})
	}

	if err := group.Wait(); err != nil {
		return models.RPCBlock{}, err
	}

	return c.buildRPCBlockResponse(blockNumber, results)
}
