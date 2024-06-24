package duneapi

import (
	"fmt"
	"sort"
	"strings"

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
}

type BlockchainIngestResponse struct {
	Error  string              `json:"error,omitempty"`
	Tables []IngestedTableInfo `json:"tables,omitempty"`
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
	FirstBlockNumber int64
	LastBlockNumber  int64
	BlockNumbers     []string
	ContentEncoding  string
	EVMStack         string
	IdempotencyKey   string
	Payload          []byte
}

type BlockchainProgress struct {
	LastIngestedBlockNumber int64 `json:"last_ingested_block_number"`
	LatestBlockNumber       int64 `json:"latest_block_number"`
}

func (p *BlockchainProgress) String() string {
	return fmt.Sprintf("%+v", *p)
}

type errorResponse struct {
	Error string `json:"error"`
}
