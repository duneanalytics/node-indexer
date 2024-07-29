package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/duneanalytics/blockchain-ingester/models"
	"golang.org/x/sync/errgroup"
)

type ArbitrumNitroClient struct {
	rpcClient
}

var _ BlockchainClient = &ArbitrumNitroClient{}

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

	results := make([]*bytes.Buffer, 0, 8)

	// eth_getBlockByNumber and extract the transaction hashes
	getBlockNumberResponse := c.bufPool.Get().(*bytes.Buffer)
	defer c.putBuffer(getBlockNumberResponse)
	results = append(results, getBlockNumberResponse)
	err := c.getResponseBody(ctx, "eth_getBlockByNumber", []any{blockNumberHex, true}, getBlockNumberResponse)
	if err != nil {
		c.log.Error("Failed to get response for jsonRPC",
			"blockNumber", blockNumber,
			"method", "eth_getBlockByNumber",
			"error", err,
		)
		return models.RPCBlock{}, err
	}
	txHashes, err := getTransactionHashes(getBlockNumberResponse.Bytes())
	if err != nil {
		return models.RPCBlock{}, err
	}

	c.log.Debug("BlockByNumber", "blockNumber", blockNumber, "txCount", len(txHashes))
	group, groupCtx := errgroup.WithContext(ctx)

	// debug_traceBlockByNumber
	result := c.bufPool.Get().(*bytes.Buffer)
	defer c.putBuffer(result)
	results = append(results, result)

	c.GroupedJSONrpc(
		groupCtx,
		group,
		"debug_traceBlockByNumber",
		[]any{blockNumberHex, map[string]string{"tracer": "callTracer"}},
		result,
		blockNumber,
	)
	for _, tx := range txHashes {
		result := c.bufPool.Get().(*bytes.Buffer)
		defer c.putBuffer(result)
		results = append(results, result)

		c.GroupedJSONrpc(groupCtx, group, "eth_getTransactionReceipt", []any{tx.Hash}, result, blockNumber)
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
