package jsonrpc

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/duneanalytics/blockchain-ingester/models"
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
// We encode the payload in NDJSON, and use a header line to indicate how many Tx are present in the block
func (c *ArbitrumNitroClient) BlockByNumber(_ context.Context, blockNumber int64) (models.RPCBlock, error) {
	tStart := time.Now()
	defer func() {
		c.log.Debug("BlockByNumber", "blockNumber", blockNumber, "duration", time.Since(tStart))
	}()
	// TODO: lets not implement this yet
	return models.RPCBlock{
		BlockNumber: blockNumber,
		Error:       errors.New("not implemented"),
	}, errors.New("not implemented")
}
