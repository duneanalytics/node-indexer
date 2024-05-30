package main

// topsql is a "synchronized" that inserts into a MeteringDB all the records present on S3.
// it has the ability to resume and continue from the last most recent record present in the DB.

import (
	"context"
	stdlog "log"
	"log/slog"
	"os"
	"os/signal"
	stdsync "sync"
	"syscall"
	"time"

	"github.com/duneanalytics/blockchain-ingester/config"
)

func init() {
	// always use UTC
	time.Local = time.UTC
}

func main() {
	logger := slog.New(os.Stdout, "harvester", slog.LstdFlags)
	cfg, err := config.Parse()
	if err != nil {
		logger.Fatal(err)
	}

	duneClient, err := dune.NewClient(&dune.Config{
		APIKey:         cfg.DuneAPIKey,
		URL:            cfg.DuneAPIURL,
		BlockchainName: cfg.BlockchainName,
		Stack:          cfg.BlockchainStack,
	})
	if err != nil {
		stdlog.Fatal(err)
	}

	rpcClient, err := jsonrpc.NewClient(&rpc.Config{
		NodeURL:      cfg.BlockchainNodeURL,
		PoolInterval: cfg.PoolInterval,
		Stack:        cfg.BlockchainStack,
	})

	ctx, stopSync := context.WithCancel(context.Background())

	var wg stdsync.WaitGroup

	harvester, err := harvester.New(&harvester.Config{
		Logger:     logger,
		DuneClient: duneClient,
		RPCClient:  rpcClient,
	})

	wg.Add(1)
	harvester.Run(ctx, wg)

	quit := make(chan os.Signal, 1)
	// handle Interrupt (ctrl-c) Term, used by `kill` et al, HUP which is commonly used to reload configs
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	s := <-quit
	logger.Warn("Caught UNIX signal", "signal", s)
	stopSync()

	// wait for all goroutines to finish
	wg.Wait()
}
