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
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(logger)
	cfg, err := config.Parse()
	if err != nil {
		stdlog.Fatal(err)
	}

	duneClient, err := duneapi.New(logger, duneapi.Config{
		APIKey:         cfg.Dune.APIKey,
		URL:            cfg.Dune.URL,
		BlockchainName: cfg.BlockchainName,
		Stack:          cfg.RPCStack,
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

	ingester := ingester.New(
		logger,
		rpcClient,
		duneClient,
		ingester.Config{
			PollInterval: cfg.PollInterval,
			MaxBatchSize: 1,
		},
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := ingester.Run(context.Background(), cfg.BlockHeight, 0 /* maxCount */)
		logger.Info("Ingester finished", "err", err)
	}()

	// TODO: add a metrics exporter or healthcheck http endpoint ?

	_, cancelFn := context.WithCancel(context.Background())
	quit := make(chan os.Signal, 1)
	// handle Interrupt (ctrl-c) Term, used by `kill` et al, HUP which is commonly used to reload configs
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	s := <-quit
	logger.Warn("Caught UNIX signal", "signal", s)
	cancelFn()

	// wait for all goroutines to finish
	wg.Wait()
}
