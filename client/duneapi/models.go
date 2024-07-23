package duneapi

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/duneanalytics/blockchain-ingester/models"
)

type Config struct {
	APIKey string
	URL    string

	// this is used by DuneAPI to determine the logic used to decode the EVM transactions
	Stack models.EVMStack
	// the name of this blockchain as it will be stored in DuneAPI
	BlockchainName string

	// RPCBlock json payloads can be very large, we default to compressing for better throughput
	// - lowers latency
	// - reduces bandwidth
	DisableCompression bool

	DisableBatchHeader bool // for testing/backwards compatibility
}

// The response from the DuneAPI ingest endpoint.
type BlockchainIngestResponse struct {
	Tables []IngestedTableInfo `json:"tables"`
}

type IngestedTableInfo struct {
	Name string `json:"name"`
	Rows int    `json:"rows"`
}

func (b *BlockchainIngestResponse) String() string {
	sort.Slice(b.Tables, func(i, j int) bool {
		return b.Tables[i].Name < b.Tables[j].Name
	})
	s := strings.Builder{}
	s.WriteString("[")
	for i, t := range b.Tables {
		if i > 0 {
			s.WriteString(", ")
		}
		s.WriteString(fmt.Sprintf("%s: %d", t.Name, t.Rows))
	}
	s.WriteString("]")
	return s.String()
}

type BlockchainIngestRequest struct {
	BlockNumbers    string
	BatchSize       int // number of blocks in the batch
	ContentEncoding string
	EVMStack        string
	IdempotencyKey  string
	Payload         []byte
}

type GetBlockchainProgressResponse struct {
	LastIngestedBlockNumber int64 `json:"last_ingested_block_number,omitempty"`
	LatestBlockNumber       int64 `json:"latest_block_number,omitempty"`
}

func (p *GetBlockchainProgressResponse) String() string {
	return fmt.Sprintf("%+v", *p)
}

type PostBlockchainProgressRequest struct {
	LastIngestedBlockNumber int64             `json:"last_ingested_block_number,omitempty"`
	LatestBlockNumber       int64             `json:"latest_block_number,omitempty"`
	Errors                  []BlockchainError `json:"errors"`
	DuneErrorCounts         int               `json:"dune_error_counts"`
	RPCErrorCounts          int               `json:"rpc_error_counts"`
	Since                   time.Time         `json:"since"`
}

type BlockchainError struct {
	Timestamp    time.Time `json:"timestamp"`
	BlockNumbers string    `json:"block_numbers"`
	Error        string    `json:"error"`
	Source       string    `json:"source"`
}

type BlockchainGapsResponse struct {
	Gaps []BlockGap `json:"gaps"`
}

// BlockGap declares an inclusive range of missing block numbers
type BlockGap struct {
	FirstMissing int64 `json:"first_missing"`
	LastMissing  int64 `json:"last_missing"`
}

func (b *BlockchainGapsResponse) String() string {
	return fmt.Sprintf("%+v", *b)
}
