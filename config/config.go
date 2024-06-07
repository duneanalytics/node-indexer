package config

import (
	"errors"
	"time"

	"github.com/duneanalytics/blockchain-ingester/models"
	flags "github.com/jessevdk/go-flags"
)

type DuneClient struct {
	APIKey string `long:"dune-api-key" env:"DUNE_API_KEY" description:"API key for DuneAPI"`
	URL    string `long:"dune-api-url" env:"DUNE_API_URL" description:"URL for DuneAPI" default:"https://api.dune.com/"`
}

func (d DuneClient) HasError() error {
	if d.APIKey == "" {
		return errors.New("DuneAPI key is required")
	}
	return nil
}

type RPCClient struct {
	NodeURL string `long:"rpc-node-url" env:"RPC_NODE_URL" description:"URL for the blockchain node"`
}

func (r RPCClient) HasError() error {
	if r.NodeURL == "" {
		return errors.New("RPC node URL is required")
	}
	return nil
}

type Config struct {
	BlockHeight            int64  `long:"block-height" env:"BLOCK_HEIGHT" description:"block height to start from" default:"-1"`      // nolint:lll
	BlockchainName         string `long:"blockchain-name" env:"BLOCKCHAIN_NAME" description:"name of the blockchain" required:"true"` // nolint:lll
	Dune                   DuneClient
	PollInterval           time.Duration `long:"rpc-poll-interval" env:"RPC_POLL_INTERVAL" description:"Interval to poll the blockchain node" default:"200ms"` // nolint:lll
	RPCNode                RPCClient
	RPCStack               models.EVMStack `long:"rpc-stack" env:"RPC_STACK" description:"Stack for the RPC client" default:"opstack"`                              // nolint:lll
	ReportProgressInterval time.Duration   `long:"report-progress-interval" env:"REPORT_PROGRESS_INTERVAL" description:"Interval to report progress" default:"20s"` // nolint:lll
	DisableCompression     bool            `long:"disable-compression" env:"DISABLE_COMPRESSION" description:"Disable compression on requests to DuneAPI"`          // nolint: lll
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
