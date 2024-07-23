package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/duneanalytics/blockchain-ingester/models"
	"golang.org/x/sync/errgroup"
)

type ArbitrumNitroClient struct {
	rpcClient
}

var _ BlockchainClient = &ArbitrumNitroClient{}

func NewArbitrumNitroClient(log *slog.Logger, cfg Config) (*ArbitrumNitroClient, error) {
	rpcClient, err := newClient(log.With("module", "jsonrpc"), cfg)
	if err != nil {
		return nil, err
	}
	return &ArbitrumNitroClient{*rpcClient}, nil
}

// BlockByNumber returns the block with the given blockNumber.
// it uses 3 different methods to get the block:
//  1. eth_getBlockByNumber
//  2. debug_traceBlockByNumber with tracer "callTracer"
//     TODO: this method should be optional
//  2. call to eth_getTransactionReceipt for each Tx present in the Block
//
// We encode the payload in NDJSON
func (c *ArbitrumNitroClient) BlockByNumber(ctx context.Context, blockNumber int64) (models.RPCBlock, error) {
	tStart := time.Now()
	defer func() {
		c.log.Debug("BlockByNumber", "blockNumber", blockNumber, "duration", time.Since(tStart))
	}()

	blockNumberHex := fmt.Sprintf("0x%x", blockNumber)

	methods := []string{
		"eth_getBlockByNumber",
		"debug_traceBlockByNumber",
	}
	methodArgs := map[string][]any{
		"eth_getBlockByNumber":     {blockNumberHex, true},
		"debug_traceBlockByNumber": {blockNumberHex, map[string]string{"tracer": "callTracer"}},
	}
	group, groupCtx := errgroup.WithContext(ctx)
	results := make([]*bytes.Buffer, len(methods))
	for i, method := range methods {
		results[i] = c.bufPool.Get().(*bytes.Buffer)
		defer c.putBuffer(results[i])

		group.Go(func() error {
			errCh := make(chan error, 1)
			c.wrkPool.Submit(func() {
				defer close(errCh)
				err := c.getResponseBody(groupCtx, method, methodArgs[method], results[i])
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

	txHashes, err := getTransactionHashes(results[0].Bytes())
	if err != nil {
		return models.RPCBlock{}, err
	}

	c.log.Debug("BlockByNumber", "blockNumber", blockNumber, "txCount", len(txHashes))
	group, groupCtx = errgroup.WithContext(ctx)
	for _, tx := range txHashes {
		result := c.bufPool.Get().(*bytes.Buffer)
		defer c.putBuffer(result)

		results = append(results, result)
		group.Go(func() error {
			errCh := make(chan error, 1)
			c.wrkPool.Submit(func() {
				defer close(errCh)
				err := c.getResponseBody(groupCtx, "eth_getTransactionReceipt", []any{tx.Hash}, result)
				if err != nil {
					c.log.Error("Failed to get response for jsonRPC",
						"blockNumber", blockNumber,
						"method", "eth_getTransactionReceipt",
						"txHash", tx.Hash,
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

type transactionHash struct {
	Hash string `json:"hash"`
}

func getTransactionHashes(blockResp []byte) ([]transactionHash, error) {
	// minimal parse the block response to extract the transaction hashes
	type blockResponse struct {
		Result struct {
			Transactions []transactionHash `json:"transactions"`
		} `json:"result"`
	}
	var resp blockResponse
	err := json.Unmarshal(blockResp, &resp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse eth_getBlockByNumber response: %w", err)
	}
	return resp.Result.Transactions, nil
}
