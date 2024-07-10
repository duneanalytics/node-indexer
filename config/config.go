package config

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/duneanalytics/blockchain-ingester/models"
	flags "github.com/jessevdk/go-flags"
)

type DuneClient struct {
	APIKey string `long:"dune-api-key" env:"DUNE_API_KEY" description:"API key for DuneAPI"`
	URL    string `long:"dune-api-url" env:"DUNE_API_URL" description:"URL for DuneAPI" default:"https://api.dune.com"`
}

func (d DuneClient) HasError() error {
	if d.APIKey == "" {
		return errors.New("DuneAPI key is required")
	}
	return nil
}

type RPCClient struct {
	NodeURL          string   `long:"rpc-node-url" env:"RPC_NODE_URL" description:"URL for the blockchain node"`
	ExtraHTTPHeaders []string `long:"rpc-http-header" env:"RPC_HTTP_HEADERS" env-delim:"|" description:"Extra HTTP headers to send with RPC requests. Each header pair must be on the form 'key:value'"` // nolint:lll
	SkipFailedBlocks bool     `long:"rpc-skip-failed-blocks" env:"RPC_SKIP_FAILED_BLOCKS" description:"Skip blocks that we fail to get from RPC. If false (default), we crash on RPC request failure"`   // nolint:lll
}

func (r RPCClient) HasError() error {
	if r.NodeURL == "" {
		return errors.New("RPC node URL is required")
	}
	for _, header := range r.ExtraHTTPHeaders {
		pair := strings.Split(header, ":")
		if len(pair) != 2 {
			return fmt.Errorf("invalid rpc http headers: expected 'key:value', got '%s'", pair)
		}
	}
	return nil
}

type Config struct {
	BlockHeight            int64  `long:"block-height" env:"BLOCK_HEIGHT" description:"block height to start from" default:"-1"`                         // nolint:lll
	BlockchainName         string `long:"blockchain-name" env:"BLOCKCHAIN_NAME" description:"name of the blockchain" required:"true"`                    // nolint:lll
	DisableCompression     bool   `long:"disable-compression" env:"DISABLE_COMPRESSION" description:"disable compression when sending data to Dune"`     // nolint:lll
	DisableGapsQuery       bool   `long:"disable-gaps-query" env:"DISABLE_GAPS_QUERY" description:"disable gaps query used to populate the initial DLQ"` // nolint:lll
	DLQOnly                bool   `long:"dlq-only" env:"DLQ_ONLY" description:"Runs just the DLQ processing on its own"`                                 // nolint:lll
	Dune                   DuneClient
	PollInterval           time.Duration `long:"rpc-poll-interval" env:"RPC_POLL_INTERVAL" description:"Interval to poll the blockchain node" default:"300ms"`    // nolint:lll
	PollDLQInterval        time.Duration `long:"dlq-poll-interval" env:"DLQ_POLL_INTERVAL" description:"Interval to poll the dlq" default:"300ms"`                // nolint:lll
	DLQRetryInterval       time.Duration `long:"dlq-retry-interval" env:"DLQ_RETRY_INTERVAL" description:"Interval for linear backoff in DLQ " default:"1m"`      // nolint:lll
	ReportProgressInterval time.Duration `long:"report-progress-interval" env:"REPORT_PROGRESS_INTERVAL" description:"Interval to report progress" default:"30s"` // nolint:lll
	RPCNode                RPCClient
	RPCStack               models.EVMStack `long:"rpc-stack" env:"RPC_STACK" description:"Stack for the RPC client" default:"opstack"`                                                 // nolint:lll
	RPCConcurrency         int             `long:"rpc-concurrency" env:"RPC_CONCURRENCY" description:"Number of concurrent requests to the RPC node" default:"25"`                     // nolint:lll
	DLQConcurrency         int             `long:"dlq-concurrency" env:"DLQ_CONCURRENCY" description:"Number of concurrent requests to the RPC node for DLQ processing" default:"2"`   // nolint:lll
	BlockSubmitInterval    time.Duration   `long:"block-submit-interval" env:"BLOCK_SUBMIT_INTERVAL" description:"Interval at which to submit batched blocks to Dune" default:"500ms"` // nolint:lll
	LogLevel               string          `long:"log" env:"LOG" description:"Log level" choice:"info" choice:"debug" choice:"warn" choice:"error" default:"info"`                     // nolint:lll
}

func (c Config) HasError() error {
	if err := c.Dune.HasError(); err != nil {
		return err
	}
	if err := c.RPCNode.HasError(); err != nil {
		return err
	}
	if c.BlockchainName == "" {
		return errors.New("blockchain name is required")
	}

	return nil
}

func Parse() (*Config, error) {
	var config Config
	parser := flags.NewParser(&config, flags.Default)
	_, err := parser.Parse()
	if err != nil {
		return nil, err
	}
	if err := config.HasError(); err != nil {
		return nil, err
	}
	return &config, nil
}
