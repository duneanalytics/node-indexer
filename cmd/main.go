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

	"github.com/duneanalytics/blockchain-ingester/client/duneapi"
	"github.com/duneanalytics/blockchain-ingester/config"
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

	// rpcClient, err := jsonrpc.NewClient(&rpc.Config{
	// 	NodeURL:      cfg.BlockchainNodeURL,
	// 	PoolInterval: cfg.PoolInterval,
	// 	Stack:        cfg.BlockchainStack,
	// })

	// harvester, err := harvester.New(&harvester.Config{
	// 	Logger:     logger,
	// 	DuneClient: duneClient,
	// 	RPCClient:  rpcClient,
	// })

	// wg.Add(1)
	// harvester.Run(ctx, &wg)

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
