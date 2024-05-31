package duneapi

import (
	"fmt"

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
	Tables []IngestedTableInfo `json:"tables"`
}

type IngestedTableInfo struct {
	Name  string `json:"name"`
	Rows  int    `json:"rows"`
	Bytes int    `json:"bytes"`
}

func (b *BlockchainIngestResponse) String() string {
	return fmt.Sprintf("Ingested: %+v", b.Tables)
}

type BlockchainIngestRequest struct {
	BlockNumber    string
	ContentType    string
	EVMStack       string
	IdempotencyKey string
	Payload        []byte
}
