package ingester

import (
	"context"
	"log/slog"
	"time"

	"github.com/duneanalytics/blockchain-ingester/client/duneapi"
	"github.com/duneanalytics/blockchain-ingester/client/jsonrpc"
	"github.com/duneanalytics/blockchain-ingester/models"
)

type Ingester interface {
	// Run starts the ingester and blocks until the context is cancelled or maxCount blocks are ingested
	Run(ctx context.Context, startBlockNumber, maxCount int64) error

	// ConsumeBlocks sends blocks from startBlockNumber to endBlockNumber to outChan, inclusive.
	// If endBlockNumber is -1, it sends blocks from startBlockNumber to the tip of the chain
	// it will run continuously until the context is cancelled
	ConsumeBlocks(ctx context.Context, outChan chan models.RPCBlock, startBlockNumber, endBlockNumber int64) error

	// SendBlocks pushes to DuneAPI the RPCBlock Payloads as they are received in an endless loop
	// it will block until:
	//	- the context is cancelled
	//  - channel is closed
	//  - a fatal error occurs
	SendBlocks(ctx context.Context, blocksCh <-chan models.RPCBlock) error

	// This is just a placeholder for now
	Info() Info
}

const defaultMaxBatchSize = 1

type Config struct {
	MaxBatchSize int
	PollInterval time.Duration
}

type Info struct {
	LatestBlockNumber   int64
	IngestedBlockNumber int64
	ConsumedBlockNumber int64
	RPCErrors           []ErrorInfo
	DuneErrors          []ErrorInfo
}

type ErrorInfo struct {
	Timestamp   time.Time
	BlockNumber int64
	Error       error
}

type ingester struct {
	log  *slog.Logger
	node jsonrpc.BlockchainClient
	dune duneapi.BlockchainIngester
	cfg  Config
	info Info
}

func New(log *slog.Logger, node jsonrpc.BlockchainClient, dune duneapi.BlockchainIngester, cfg Config) Ingester {
	ing := &ingester{
		log:  log,
		node: node,
		dune: dune,
		cfg:  cfg,
		info: Info{
			RPCErrors:  []ErrorInfo{},
			DuneErrors: []ErrorInfo{},
		},
	}
	if ing.cfg.MaxBatchSize == 0 {
		ing.cfg.MaxBatchSize = defaultMaxBatchSize
	}
	return ing
}

func (i *ingester) Info() Info {
	return Info{}
}
