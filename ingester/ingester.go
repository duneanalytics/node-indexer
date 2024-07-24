package ingester

import (
	"context"
	"log/slog"
	"time"

	"github.com/duneanalytics/blockchain-ingester/lib/dlq"

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

	// SendBlocks consumes RPCBlocks from the channel, reorders them, and sends batches to DuneAPI in an endless loop
	// it will block until:
	//	- the context is cancelled
	//  - channel is closed
	//  - a fatal error occurs
	SendBlocks(ctx context.Context, blocksCh <-chan models.RPCBlock, startFrom int64) error

	// ProduceBlockNumbersDLQ sends block numbers from the DLQ to outChan.
	// It will run continuously until the context is cancelled.
	// When the DLQ does not return an eligible next block, it waits for PollDLQInterval before trying again
	ProduceBlockNumbersDLQ(ctx context.Context, outChan chan dlq.Item[int64]) error

	// FetchBlockLoopDLQ fetches blocks sent on the channel and sends them on the other channel.
	// It will run continuously until the context is cancelled, or the channel is closed.
	// It can safely be run concurrently.
	FetchBlockLoopDLQ(ctx context.Context,
		blockNumbers <-chan dlq.Item[int64],
		blocks chan<- dlq.Item[models.RPCBlock],
	) error

	// SendBlocksDLQ pushes one RPCBlock at a time to DuneAPI in the order they are received in
	SendBlocksDLQ(ctx context.Context, blocks <-chan dlq.Item[models.RPCBlock]) error

	Close() error
}

const (
	defaultReportProgressInterval = 30 * time.Second
)

type Config struct {
	MaxConcurrentRequests    int
	MaxConcurrentRequestsDLQ int
	PollInterval             time.Duration
	PollDLQInterval          time.Duration
	ReportProgressInterval   time.Duration
	Stack                    models.EVMStack
	BlockchainName           string
	BlockSubmitInterval      time.Duration
	SkipFailedBlocks         bool
	DLQOnly                  bool
	MaxBatchSize             int
}

type ingester struct {
	log     *slog.Logger
	node    jsonrpc.BlockchainClient
	dune    duneapi.BlockchainIngester
	duneDLQ duneapi.BlockchainIngester
	cfg     Config
	info    Info
	dlq     *dlq.DLQ[int64]
}

func New(
	log *slog.Logger,
	node jsonrpc.BlockchainClient,
	dune duneapi.BlockchainIngester,
	duneDLQ duneapi.BlockchainIngester,
	cfg Config,
	progress *models.BlockchainIndexProgress,
	dlq *dlq.DLQ[int64],
) Ingester {
	info := NewInfo(cfg.BlockchainName, cfg.Stack.String())
	if progress != nil {
		info.LatestBlockNumber = progress.LatestBlockNumber
		info.IngestedBlockNumber = progress.LastIngestedBlockNumber
	}
	ing := &ingester{
		log:     log.With("module", "ingester"),
		node:    node,
		dune:    dune,
		duneDLQ: duneDLQ,
		cfg:     cfg,
		info:    info,
		dlq:     dlq,
	}
	if ing.cfg.ReportProgressInterval == 0 {
		ing.cfg.ReportProgressInterval = defaultReportProgressInterval
	}
	if ing.cfg.MaxBatchSize == 0 {
		ing.cfg.MaxBatchSize = maxBatchSize
	} else if ing.cfg.MaxBatchSize > maxBatchSize {
		ing.cfg.MaxBatchSize = maxBatchSize
	}
	return ing
}
