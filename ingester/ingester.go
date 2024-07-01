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
	Run(ctx context.Context, startBlockNumber int64, maxCount int64) error

	// ProduceBlockNumbers sends block numbers from startBlockNumber to endBlockNumber to outChan, inclusive.
	// If endBlockNumber is -1, it sends blocks from startBlockNumber to the tip of the chain
	// it will run continuously until the context is cancelled
	ProduceBlockNumbers(ctx context.Context, outChan chan int64, startBlockNumber int64, endBlockNumber int64) error

	// FetchBlockLoop fetches blocks sent on the channel and sends them on the other channel.
	// It will run continuously until the context is cancelled, or the channel is closed.
	// It can safely be run concurrently.
	FetchBlockLoop(context.Context, chan int64, chan models.RPCBlock) error

	// SendBlocks pushes to DuneAPI the RPCBlock Payloads as they are received in an endless loop
	// it will block until:
	//	- the context is cancelled
	//  - channel is closed
	//  - a fatal error occurs
	SendBlocks(ctx context.Context, blocksCh <-chan models.RPCBlock, startFrom int64) error

	// This is just a placeholder for now
	Info() Info

	Close() error
}

const (
	defaultMaxBatchSize           = 5
	defaultReportProgressInterval = 30 * time.Second
)

type Config struct {
	MaxConcurrentRequests  int
	PollInterval           time.Duration
	ReportProgressInterval time.Duration
	Stack                  models.EVMStack
	BlockchainName         string
	BlockSubmitInterval    time.Duration
	SkipFailedBlocks       bool
}

type Info struct {
	LatestBlockNumber   int64
	IngestedBlockNumber int64
	ConsumedBlockNumber int64
	RPCErrors           []ErrorInfo
	DuneErrors          []ErrorInfo
}

// Errors returns a combined list of errors from RPC requests and Dune requests, for use in progress reporting
func (info Info) Errors() []models.BlockchainIndexError {
	errors := make([]models.BlockchainIndexError, 0, len(info.RPCErrors)+len(info.DuneErrors))
	for _, e := range info.RPCErrors {
		errors = append(errors, models.BlockchainIndexError{
			Timestamp:    e.Timestamp,
			BlockNumbers: e.BlockNumbers,
			Error:        e.Error.Error(),
			Source:       "rpc",
		})
	}
	for _, e := range info.DuneErrors {
		errors = append(errors, models.BlockchainIndexError{
			Timestamp:    e.Timestamp,
			BlockNumbers: e.BlockNumbers,
			Error:        e.Error.Error(),
			Source:       "dune",
		})
	}
	return errors
}

type ErrorInfo struct {
	Timestamp    time.Time
	BlockNumbers string
	Error        error
}

type ingester struct {
	log  *slog.Logger
	node jsonrpc.BlockchainClient
	dune duneapi.BlockchainIngester
	cfg  Config
	info Info
}

func New(
	log *slog.Logger,
	node jsonrpc.BlockchainClient,
	dune duneapi.BlockchainIngester,
	cfg Config,
	progress *models.BlockchainIndexProgress,
) Ingester {
	info := Info{
		RPCErrors:  []ErrorInfo{},
		DuneErrors: []ErrorInfo{},
	}
	if progress != nil {
		info.LatestBlockNumber = progress.LatestBlockNumber
		info.IngestedBlockNumber = progress.LastIngestedBlockNumber
	}
	ing := &ingester{
		log:  log.With("module", "ingester"),
		node: node,
		dune: dune,
		cfg:  cfg,
		info: info,
	}
	if ing.cfg.ReportProgressInterval == 0 {
		ing.cfg.ReportProgressInterval = defaultReportProgressInterval
	}
	return ing
}

func (i *ingester) Info() Info {
	return i.info
}
