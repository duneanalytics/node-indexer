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
	// rpcClient is used in parallel by the ingester to fetch blocks
	// but it also has internal request concurrency on handling each block
	// to avoid spawning too many http requests to the RPC node we set here an upper limit
	TotalRPCConcurrency int
}
