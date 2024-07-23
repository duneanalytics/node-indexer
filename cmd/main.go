package main

// ingester is a "synchronizer" that ingests into DuneAPI the blocks from the blockchain.
// it has the ability to resume and catch up with the the head of the blockchain.

import (
	"context"
	stdlog "log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	stdsync "sync"
	"syscall"
	"time"

	"github.com/duneanalytics/blockchain-ingester/client/duneapi"
	"github.com/duneanalytics/blockchain-ingester/client/jsonrpc"
	"github.com/duneanalytics/blockchain-ingester/config"
	"github.com/duneanalytics/blockchain-ingester/ingester"
	"github.com/duneanalytics/blockchain-ingester/lib/dlq"
	"github.com/duneanalytics/blockchain-ingester/models"
)

func init() {
	// always use UTC
	time.Local = time.UTC
}

func main() {
	cfg, err := config.Parse()
	if err != nil {
		stdlog.Fatal(err)
	}
	logOptions := &slog.HandlerOptions{}
	switch cfg.LogLevel {
	case "debug":
		logOptions.Level = slog.LevelDebug
	case "info":
		logOptions.Level = slog.LevelInfo
	case "warn":
		logOptions.Level = slog.LevelWarn
	case "error":
		logOptions.Level = slog.LevelError
	default:
		stdlog.Fatalf("unsupported log level: '%s'", cfg.LogLevel)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, logOptions))
	slog.SetDefault(logger)

	duneClient, err := duneapi.New(logger, duneapi.Config{
		APIKey:             cfg.Dune.APIKey,
		URL:                cfg.Dune.URL,
		BlockchainName:     cfg.BlockchainName,
		Stack:              cfg.RPCStack,
		DisableCompression: cfg.DisableCompression,
		DisableBatchHeader: cfg.Dune.DisableBatchHeader,
	})
	if err != nil {
		stdlog.Fatal(err)
	}
	defer duneClient.Close()

	// Create an extra Dune API client for DLQ processing since it is not thread-safe yet
	duneClientDLQ, err := duneapi.New(logger, duneapi.Config{
		APIKey:             cfg.Dune.APIKey,
		URL:                cfg.Dune.URL,
		BlockchainName:     cfg.BlockchainName,
		Stack:              cfg.RPCStack,
		DisableCompression: cfg.DisableCompression,
	})
	if err != nil {
		stdlog.Fatal(err)
	}
	defer duneClientDLQ.Close()

	var wg stdsync.WaitGroup
	var rpcClient jsonrpc.BlockchainClient

	rpcHTTPHeaders := make(map[string]string)
	for _, header := range cfg.RPCNode.ExtraHTTPHeaders {
		pair := strings.Split(header, ":")
		// We've validated this list has two elements in `config.HasError()`
		key := strings.Trim(pair[0], " ")
		value := strings.Trim(pair[1], " ")
		logger.Info("Adding extra HTTP header to RPC requests", "key", key, "value", value)
		rpcHTTPHeaders[key] = value
	}
	switch cfg.RPCStack {
	case models.OpStack:
		rpcClient, err = jsonrpc.NewOpStackClient(logger, jsonrpc.Config{
			URL:         cfg.RPCNode.NodeURL,
			HTTPHeaders: rpcHTTPHeaders,
		})
	default:
		stdlog.Fatalf("unsupported RPC stack: %s", cfg.RPCStack)
	}
	if err != nil {
		stdlog.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Get stored progress unless config indicates we should start from 0
	var startBlockNumber int64
	// Default to -1 to start where the ingester left off
	var progress *models.BlockchainIndexProgress
	if cfg.BlockHeight == -1 {
		progress, err = duneClient.GetProgressReport(ctx)
		if err != nil {
			stdlog.Fatal(err)
		} else {
			startBlockNumber = progress.LastIngestedBlockNumber + 1
		}
	} else {
		startBlockNumber = cfg.BlockHeight
	}

	dlqBlockNumbers := dlq.NewDLQWithDelay[int64](dlq.RetryDelayLinear(cfg.DLQRetryInterval))

	if !cfg.DisableGapsQuery {
		blockGaps, err := duneClient.GetBlockGaps(ctx)
		if err != nil {
			stdlog.Fatal(err)
		} else {
			ingester.AddBlockGaps(dlqBlockNumbers, blockGaps.Gaps)
		}
	}

	maxCount := int64(0) // 0 means ingest until cancelled
	ingester := ingester.New(
		logger,
		rpcClient,
		duneClient,
		duneClientDLQ,
		ingester.Config{
			MaxConcurrentRequests:    cfg.RPCConcurrency,
			MaxConcurrentRequestsDLQ: cfg.DLQConcurrency,
			ReportProgressInterval:   cfg.ReportProgressInterval,
			PollInterval:             cfg.PollInterval,
			PollDLQInterval:          cfg.PollDLQInterval,
			Stack:                    cfg.RPCStack,
			BlockchainName:           cfg.BlockchainName,
			BlockSubmitInterval:      cfg.BlockSubmitInterval,
			SkipFailedBlocks:         cfg.RPCNode.SkipFailedBlocks,
			DLQOnly:                  cfg.DLQOnly,
		},
		progress,
		dlqBlockNumbers,
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := ingester.Run(ctx, startBlockNumber, maxCount)
		logger.Info("Ingester finished", "err", err)
		cancel()
	}()

	defer ingester.Close()

	// TODO: add a metrics exporter or healthcheck http endpoint ?

	quit := make(chan os.Signal, 1)
	// handle Interrupt (ctrl-c) Term, used by `kill` et al, HUP which is commonly used to reload configs
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	select {
	case <-ctx.Done():
		logger.Warn("Context done")
	case s := <-quit:
		logger.Warn("Caught UNIX signal", "signal", s)
		cancel()
	}

	// wait for Run to finish
	wg.Wait()
}
