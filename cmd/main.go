package main

// ingester is a "synchronizer" that ingests into DuneAPI the blocks from the blockchain.
// it has the ability to resume and catch up with the the head of the blockchain.

import (
	"context"
	stdlog "log"
	"log/slog"
	"os"
	"os/signal"
	stdsync "sync"
	"syscall"
	"time"

	"github.com/duneanalytics/blockchain-ingester/client/duneapi"
	"github.com/duneanalytics/blockchain-ingester/client/jsonrpc"
	"github.com/duneanalytics/blockchain-ingester/config"
	"github.com/duneanalytics/blockchain-ingester/ingester"
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
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	duneClient, err := duneapi.New(logger, duneapi.Config{
		APIKey:             cfg.Dune.APIKey,
		URL:                cfg.Dune.URL,
		BlockchainName:     cfg.BlockchainName,
		Stack:              cfg.RPCStack,
		DisableCompression: cfg.DisableCompression,
	})
	if err != nil {
		stdlog.Fatal(err)
	}
	defer duneClient.Close()

	var wg stdsync.WaitGroup
	var rpcClient jsonrpc.BlockchainClient

	switch cfg.RPCStack {
	case models.OpStack:
		rpcClient, err = jsonrpc.NewOpStackClient(logger, jsonrpc.Config{
			URL: cfg.RPCNode.NodeURL,
		})
	default:
		stdlog.Fatalf("unsupported RPC stack: %s", cfg.RPCStack)
	}
	if err != nil {
		stdlog.Fatal(err)
	}

	ctx, cancelFn := context.WithCancel(context.Background())

	// Get stored progress unless config indicates we should start from 0
	var startBlockNumber int64
	// Default to -1 to start where the ingester left off
	if cfg.BlockHeight == -1 {
		progress, err := duneClient.GetProgressReport(ctx)
		if err != nil {
			stdlog.Fatal(err)
		} else {
			startBlockNumber = progress.LastIngestedBlockNumber + 1
		}
	} else {
		startBlockNumber = cfg.BlockHeight
	}

	maxCount := int64(0) // 0 means ingest until cancelled
	ingester := ingester.New(
		logger,
		rpcClient,
		duneClient,
		ingester.Config{
			MaxBatchSize:           cfg.Concurrency,
			ReportProgressInterval: cfg.ReportProgressInterval,
			PollInterval:           cfg.PollInterval,
			Stack:                  cfg.RPCStack,
			BlockchainName:         cfg.BlockchainName,
			BatchRequestInterval:   cfg.BatchRequestInterval,
		},
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := ingester.Run(ctx, startBlockNumber, maxCount)
		logger.Info("Ingester finished", "err", err)
	}()

	defer ingester.Close()

	// TODO: add a metrics exporter or healthcheck http endpoint ?

	quit := make(chan os.Signal, 1)
	// handle Interrupt (ctrl-c) Term, used by `kill` et al, HUP which is commonly used to reload configs
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	s := <-quit
	logger.Warn("Caught UNIX signal", "signal", s)
	cancelFn()

	// wait for all goroutines to finish
	wg.Wait()
}
