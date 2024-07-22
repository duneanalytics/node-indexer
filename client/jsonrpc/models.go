package jsonrpc

import (
	"time"

	"github.com/duneanalytics/blockchain-ingester/models"
)

type Config struct {
	URL          string
	PollInterval time.Duration
	HTTPHeaders  map[string]string
	EVMStack     models.EVMStack
}
